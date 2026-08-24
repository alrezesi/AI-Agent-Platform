import asyncio
import logging
import os
from pathlib import Path

from redis.asyncio import Redis

from src.agent_platform.core.agent import AgentCapability, AgentRecord, AgentStatus, BaseAgent
from src.agent_platform.db import get_session_factory
from src.agent_platform.distributed.node import NodeInfo
from src.agent_platform.distributed.registry import DistributedRegistry
from src.agent_platform.distributed.worker import WorkerConfig, WorkerNode
from src.agent_platform.scheduler.redis_queue import RedisTaskQueue
from src.agents.bge_m3_agent import BGEM3Agent


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger = logging.getLogger(__name__)


class RuntimeAgentRegistry:
    """Runtime registry for executable agent instances."""

    def __init__(self, distributed_registry: DistributedRegistry):
        self._agents: dict[str, BaseAgent] = {}
        self.distributed_registry = distributed_registry

    async def register(self, agent: BaseAgent) -> None:
        self._agents[agent.agent_id] = agent
        record = AgentRecord(
            agent_id=agent.agent_id,
            name=agent.name,
            description=f"Runtime agent: {agent.name}",
            capabilities=self._build_capabilities(agent),
            status=AgentStatus.ACTIVE,
            metadata={"worker": os.getenv("WORKER_ID", "worker-1")},
            tenant_id=agent.tenant_id,
        )
        await self.distributed_registry.register(record)
        logger.info("Registered agent: %s (%s)", agent.agent_id, agent.name)

    async def get_agent(self, agent_id: str, tenant_id: str | None = None):
        agent = self._agents.get(agent_id)
        if agent is None:
            return None
        if tenant_id is not None and agent.tenant_id != tenant_id:
            return None
        return agent

    async def unregister(self, agent_id: str, tenant_id: str | None = None) -> bool:
        agent = self._agents.get(agent_id)
        if agent is None:
            return False
        if tenant_id is not None and agent.tenant_id != tenant_id:
            return False
        self._agents.pop(agent_id, None)
        await self.distributed_registry.unregister(agent_id, tenant_id)
        return True

    async def update_node_status(self, node_info: NodeInfo) -> None:
        await self.distributed_registry.update_node_status(node_info)

    async def register_node(self, node_info: NodeInfo) -> None:
        await self.distributed_registry.register_node(node_info)

    @staticmethod
    def _build_capabilities(agent: BaseAgent):
        if agent.agent_id == "bge-m3":
            return [AgentCapability(name="embedding", description="Generate BGE-M3 text embeddings")]
        return []


def validate_local_model_path(label: str, env_var: str) -> str:
    path = os.getenv(env_var)
    if not path:
        raise RuntimeError(f"Required local model not found: environment variable {env_var} is not set")
    model_path = Path(path)
    if not model_path.exists():
        raise RuntimeError(f"Required local model not found: {model_path}")
    if not model_path.is_dir():
        raise RuntimeError(f"Required local model path is not a directory: {model_path}")
    logger.info("Using local %s model: %s", label, model_path)
    return str(model_path)


async def initialize_agents() -> list[BaseAgent]:
    tenant_id = os.getenv("AGENT_TENANT_ID", "dummy")
    bge_model_path = validate_local_model_path("BGE-M3", "BGE_MODEL_PATH")

    agents: list[BaseAgent] = [
        BGEM3Agent(
            agent_id="bge-m3",
            name="BGE-M3",
            tenant_id=tenant_id,
            model_path=bge_model_path,
            device=os.getenv("BGE_DEVICE", "cpu"),
        ),
    ]

    for agent in agents:
        logger.info("Initializing agent %s...", agent.agent_id)
        await agent.initialize()
        if not agent.is_ready():
            raise RuntimeError(f"Agent {agent.agent_id} failed to initialize")
        logger.info("Agent %s initialized successfully", agent.agent_id)

    return agents


async def main() -> None:
    worker_id = os.getenv("WORKER_ID", "worker-1")
    redis_url = os.getenv("REDIS_URL", "redis://redis:6379/1")
    worker_port = int(os.getenv("WORKER_PORT", "8001"))

    logger.info("Starting worker %s", worker_id)
    logger.info("Redis URL: %s", redis_url)

    redis = Redis.from_url(redis_url, decode_responses=False)

    try:
        await redis.ping()
        logger.info("Redis connection established")

        queue = RedisTaskQueue(redis_client=redis, session_factory=get_session_factory())
        distributed_registry = DistributedRegistry(redis_client=redis)
        runtime_registry = RuntimeAgentRegistry(distributed_registry=distributed_registry)

        agents = await initialize_agents()

        node_info = NodeInfo.create(
            port=worker_port,
            capabilities={"agents": [agent.agent_id for agent in agents], "worker_id": worker_id},
        )
        node_info.node_id = worker_id
        await runtime_registry.register_node(node_info)

        for agent in agents:
            await runtime_registry.register(agent)

        worker = WorkerNode(
            info=node_info,
            queue=queue,
            agent_registry=runtime_registry,
            config=WorkerConfig(
                max_concurrent_tasks=int(os.getenv("WORKER_MAX_CONCURRENT_TASKS", "1")),
                poll_interval=float(os.getenv("WORKER_POLL_INTERVAL", "0.5")),
                node_heartbeat_interval=float(os.getenv("WORKER_HEARTBEAT_INTERVAL", "2.0")),
                task_timeout_seconds=int(os.getenv("WORKER_TASK_TIMEOUT_SECONDS", "5")),
            ),
        )

        await worker.start()
        if hasattr(queue, "recover_orphaned_tasks"):
            await queue.recover_orphaned_tasks()

        logger.info("Worker %s is running and waiting for tasks...", worker_id)

        try:
            while True:
                await asyncio.sleep(3600)
        except asyncio.CancelledError:
            logger.info("Worker %s received cancellation", worker_id)
        finally:
            await worker.stop()
            for agent in agents:
                try:
                    await agent.shutdown()
                except Exception:
                    logger.exception("Failed to shutdown agent %s", agent.agent_id)
    finally:
        await redis.aclose()
        logger.info("Redis connection closed")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Worker stopped by user")
