# Worker node runner for Chaos Engineering tests

import asyncio
import argparse
import logging
import sys
import os
from pathlib import Path

# Add project root to Python path
sys.path.insert(0, str(Path(__file__).parent.parent))

from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from src.agent_platform.core.agent import AgentRecord, AgentStatus, AgentCapability, BaseAgent
from src.agent_platform.distributed.registry import DistributedRegistry
from src.agent_platform.distributed.queue import DistributedTaskQueue
from src.agent_platform.distributed.worker import WorkerNode, WorkerConfig
from src.agent_platform.distributed.node import NodeInfo
from src.agent_platform.core.task import Task

# Import real agents
from src.agents import BGEM3Agent, GemmaAgent
from src.agents.bge_m3_agent import BGE_MODEL_PATH
from src.agents.gemma_agent import GEMMA_MODEL_PATH

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class EchoAgent(BaseAgent):
    """Simple Echo agent for testing."""
    async def initialize(self) -> None:
        self._initialized = True
        self.state = AgentRuntimeState.RUNNING  # noqa: F821
        logger.info(f"EchoAgent {self.agent_id} initialized")

    async def run(self, task: Task) -> str:
        message = task.payload.get("message", "")
        logger.info(f"EchoAgent {self.agent_id} processing task {task.task_id}: {message}")
        return f"Echo: {message}"

    async def shutdown(self) -> None:
        self._initialized = False
        logger.info(f"EchoAgent {self.agent_id} shut down")


async def register_agent(registry, agent_instance):
    """Register an agent instance in the registry."""
    record = AgentRecord(
        agent_id=agent_instance.agent_id,
        name=agent_instance.name,
        capabilities=[],
        status=AgentStatus.ACTIVE,
        tenant_id=agent_instance.tenant_id,
    )
    await registry.register(record)
    logger.info(f"Agent {agent_instance.agent_id} registered")


async def main():
    """Main worker entry point."""
    parser = argparse.ArgumentParser(description="Run a Chaos worker node.")
    parser.add_argument("--node-id", default="worker1", help="Unique node identifier")
    parser.add_argument("--port", type=int, default=8001, help="Port for node registration")
    args = parser.parse_args()

    database_url = os.getenv("DATABASE_URL", "postgresql+asyncpg://user:pass@postgres:5432/agent_platform")
    redis_url = os.getenv("REDIS_URL", "redis://redis:6379/0")

    logger.info(f"Starting Chaos worker: {args.node_id} on port {args.port}")

    redis_client = Redis.from_url(redis_url)
    engine = create_async_engine(database_url, echo=False)
    session_factory = async_sessionmaker(engine)

    registry = DistributedRegistry(redis_client)
    queue = DistributedTaskQueue(redis_client)

    config = WorkerConfig(
        max_concurrent_tasks=5,
        poll_interval=0.5,
        node_heartbeat_interval=10.0,
        task_timeout_seconds=30,
    )

    node_info = NodeInfo.create(port=args.port)
    node_info.node_id = args.node_id

    worker = WorkerNode(node_info, queue, registry, config)
    await worker.start()

    # --- Register agents ---

    # 1. EchoAgent (for testing)
    echo_agent = EchoAgent(agent_id="echo-agent", name="Echo Test Agent", tenant_id=None)
    await register_agent(registry, echo_agent)

    # 2. BGE-M3 embedding agent
    try:
        bge_agent = BGEM3Agent(
            agent_id="bge-m3",
            name="BGE-M3 Embedding",
            model_path=BGE_MODEL_PATH,  # uses default path from agent file
            device="cpu",               # change to "cuda" if GPU is available
            tenant_id=None,
        )
        await bge_agent.initialize()
        await register_agent(registry, bge_agent)
        logger.info("BGE-M3 agent registered and initialized")
    except Exception as e:
        logger.error(f"Failed to initialize BGE-M3 agent: {e}")

    # 3. Gemma 2 2B text generation agent
    try:
        gemma_agent = GemmaAgent(
            agent_id="gemma-2b",
            name="Gemma 2 2B",
            model_path=GEMMA_MODEL_PATH,
            device="cpu",
            tenant_id=None,
        )
        await gemma_agent.initialize()
        await register_agent(registry, gemma_agent)
        logger.info("Gemma agent registered and initialized")
    except Exception as e:
        logger.error(f"Failed to initialize Gemma agent: {e}")

    logger.info("All agents registered. Worker is ready.")

    # Keep the worker running until interrupted
    try:
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        logger.info("Shutting down worker...")
        await worker.stop()
        await redis_client.close()
        await engine.dispose()
        logger.info("Worker stopped.")


if __name__ == "__main__":
    asyncio.run(main())