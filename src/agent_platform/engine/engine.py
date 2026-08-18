
# Core agent execution engine with run loop and lifecycle management

import asyncio
import logging

from src.agent_platform.core.agent import AgentRuntimeState, BaseAgent
from src.agent_platform.core.task import Task, TaskStatus
from src.agent_platform.engine.context import AgentContext
from src.agent_platform.registry.base import BaseAgentRegistry
from src.agent_platform.scheduler.scheduler import TaskScheduler
from src.agent_platform.scheduler.worker import TaskWorker  # <-- NEW IMPORT

logger = logging.getLogger(__name__)


class AgentEngine:
    """
    Main engine that manages multiple agents, their lifecycles, and task execution.
    Polls the task scheduler and dispatches tasks to the appropriate agents.
    """

    def __init__(
        self,
        registry: BaseAgentRegistry,
        scheduler: TaskScheduler,
        poll_interval: float = 0.5,
        max_concurrent_tasks_per_agent: int = 1,
    ):
        self.registry = registry
        self.scheduler = scheduler
        self.poll_interval = poll_interval
        self.max_concurrent_tasks_per_agent = max_concurrent_tasks_per_agent

        # agent_id -> BaseAgent instance
        self._agents: dict[str, BaseAgent] = {}
        # agent_id -> asyncio.Task for the worker
        self._workers: dict[str, asyncio.Task] = {}
        # agent_id -> semaphore for concurrency limit
        self._semaphores: dict[str, asyncio.Semaphore] = {}

        self._running = False
        self._main_task: asyncio.Task | None = None

    async def register_agent(self, agent: BaseAgent, context: AgentContext | None = None) -> None:
        """
        Register an agent instance with the engine.
        Initializes the agent and registers it with the registry.
        """
        agent_id = agent.agent_id

        # Create context if not provided
        if context is None:
            context = AgentContext(
                agent_id=agent_id,
                tenant_id=agent.tenant_id,
                config={}
            )
        agent.context = context

        # Initialize the agent
        try:
            await agent.initialize()
            agent._initialized = True
            # Set state to RUNNING after successful initialization
            agent.state = AgentRuntimeState.RUNNING
        except Exception as e:
            logger.error(f"Failed to initialize agent {agent_id}: {e}")
            agent.state = AgentRuntimeState.ERROR
            raise

        self._agents[agent_id] = agent
        self._semaphores[agent_id] = asyncio.Semaphore(self.max_concurrent_tasks_per_agent)

        agent._task_queue = asyncio.Queue()

        # Register with the central registry
        from src.agent_platform.core.agent import AgentRecord, AgentStatus
        record = AgentRecord(
            agent_id=agent_id,
            name=agent.name,
            status=AgentStatus.ACTIVE,
            tenant_id=agent.tenant_id,
        )
        await self.registry.register(record)

        logger.info(f"Agent {agent_id} registered and initialized")

    async def unregister_agent(self, agent_id: str) -> bool:
        """Unregister an agent, shutdown it, and remove from engine."""
        agent = self._agents.get(agent_id)
        if not agent:
            return False

        # Stop the worker if running
        if agent_id in self._workers:
            self._workers[agent_id].cancel()
            try:
                await self._workers[agent_id]
            except asyncio.CancelledError:
                pass
            del self._workers[agent_id]

        # Shutdown the agent
        await agent.shutdown()
        agent.state = AgentRuntimeState.STOPPED
        agent._initialized = False

        # Remove from registry
        await self.registry.unregister(agent_id)

        del self._agents[agent_id]
        del self._semaphores[agent_id]

        logger.info(f"Agent {agent_id} unregistered")
        return True

    async def start(self) -> None:
        """Start the engine's main run loop."""
        if self._running:
            return

        self._running = True
        logger.info("AgentEngine starting...")

        # Start main task dispatcher loop
        self._main_task = asyncio.create_task(self._dispatcher_loop())

        # Start workers for all currently registered agents
        for agent_id in self._agents:
            if agent_id not in self._workers:
                self._start_worker(agent_id)

        logger.info("AgentEngine started successfully")

    async def stop(self) -> None:
        """Gracefully stop the engine and all agents."""
        self._running = False

        # Cancel main task
        if self._main_task:
            self._main_task.cancel()
            try:
                await self._main_task
            except asyncio.CancelledError:
                pass

        # Cancel all workers
        for agent_id, worker in list(self._workers.items()):
            worker.cancel()
            try:
                await worker
            except asyncio.CancelledError:
                pass
            del self._workers[agent_id]

        # Shutdown all agents
        for agent in self._agents.values():
            await agent.shutdown()

        logger.info("AgentEngine stopped")

    def _start_worker(self, agent_id: str) -> None:
        """Start a worker task for a specific agent."""
        if agent_id in self._workers:
            return
        self._workers[agent_id] = asyncio.create_task(
            self._worker_loop(agent_id)
        )

    async def _dispatcher_loop(self) -> None:
        """
        Main dispatcher loop: polls tasks and assigns them to agents.
        """
        while self._running:
            try:
                # Dequeue the next task
                task = await self.scheduler.dequeue_next()

                if task is None:
                    await asyncio.sleep(self.poll_interval)
                    continue

                agent_id = task.agent_id
                agent = self._agents.get(agent_id)

                if not agent:
                    # Agent not found, mark task as failed
                    task.status = TaskStatus.FAILED
                    task.error = f"Agent {agent_id} not found in engine"
                    logger.warning(f"Task {task.task_id} failed: agent not found")
                    # Update scheduler
                    await self.scheduler.on_task_completed(task)
                    continue

                if not agent.is_ready():
                    # Agent not ready, mark failed
                    task.status = TaskStatus.FAILED
                    task.error = f"Agent {agent_id} is not ready (state: {agent.state})"
                    logger.warning(f"Task {task.task_id} failed: agent not ready")
                    await self.scheduler.on_task_completed(task)
                    continue

                # Acquire semaphore to respect concurrency limit
                # Create a queue for this agent if not exists
                if agent._task_queue is None:
                    logger.error(
                        "Agent %s has no task queue",
                        agent_id,
                    )
                    task.status = TaskStatus.FAILED
                    task.error = f"Agent {agent_id} task queue is not initialized"
                    await self.scheduler.on_task_completed(task)
                    continue

                await agent._task_queue.put(task)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in dispatcher loop: {e}")
                await asyncio.sleep(1)

    async def _worker_loop(self, agent_id: str) -> None:
        """
        Worker loop for a specific agent.
        Pulls tasks from the agent's queue and executes them.
        """
        agent = self._agents.get(agent_id)
        if not agent:
            return

        semaphore = self._semaphores[agent_id]

        while self._running and agent_id in self._agents:
            try:
                # Check if agent has a task queue
                if agent._task_queue is None:
                    await asyncio.sleep(0.5)
                    continue

                queue: asyncio.Queue = agent._task_queue

                # Wait for a task with timeout to allow for cancellation checks
                try:
                    task = await asyncio.wait_for(queue.get(), timeout=1.0)
                except TimeoutError:
                    continue

                # Execute task with concurrency limit
                async with semaphore:
                    await self._execute_task(agent, task)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Worker error for agent {agent_id}: {e}")

        logger.info(f"Worker stopped for agent {agent_id}")

    async def _execute_task(self, agent: BaseAgent, task: Task) -> None:
        """
        Execute a single task on the given agent using TaskWorker.
        """
        # Create a worker and execute
        worker = TaskWorker(task, agent)
        updated_task = await worker.execute()

        # Notify scheduler about the completion/update
        await self.scheduler.on_task_completed(updated_task)

    # --- Agent lifecycle control methods ---
    async def pause_agent(self, agent_id: str) -> bool:
        """Pause a specific agent."""
        agent = self._agents.get(agent_id)
        if not agent:
            return False
        await agent.pause()
        return True

    async def resume_agent(self, agent_id: str) -> bool:
        """Resume a specific agent."""
        agent = self._agents.get(agent_id)
        if not agent:
            return False
        await agent.resume()
        # Make sure worker is running if it isn't
        if agent_id not in self._workers:
            self._start_worker(agent_id)
        return True

    async def get_agent_state(self, agent_id: str) -> AgentRuntimeState | None:
        """Get the runtime state of an agent."""
        agent = self._agents.get(agent_id)
        if not agent:
            return None
        return agent.state

    def list_agents(self) -> list[str]:
        """List all agent IDs registered with the engine."""
        return list(self._agents.keys())

    @property
    def is_running(self) -> bool:
        return self._running
