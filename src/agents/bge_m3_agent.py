
# BGE-M3 embedding agent

import logging
import os
from typing import Any

from src.agent_platform.core.agent import AgentRuntimeState, BaseAgent
from src.agent_platform.core.task import Task

logger = logging.getLogger(__name__)


BGE_MODEL_PATH = os.getenv(
    "BGE_MODEL_PATH",
    "BAAI/bge-m3",
)

class BGEM3Agent(BaseAgent):
    def __init__(self, model_path: str = None, device: str = "cpu", **kwargs):
        super().__init__(**kwargs)
        self.model_path = model_path or BGE_MODEL_PATH
        self.device = device
        self._model = None

    async def initialize(self) -> None:
        logger.info(f"Loading BGE-M3 from: {self.model_path}")
        from sentence_transformers import SentenceTransformer
        self._model = SentenceTransformer(self.model_path, device=self.device)
        self._initialized = True
        self.state = AgentRuntimeState.RUNNING
        logger.info("BGE-M3 agent initialized")

    async def run(self, task: Task) -> Any:
        if self._model is None:
            raise RuntimeError("Model not initialized")
        text = task.payload.get("text", "")
        if not text:
            raise ValueError("Missing 'text' in task payload")
        embedding = self._model.encode(text)
        return embedding.tolist()

    async def shutdown(self) -> None:
        self._model = None
        self._initialized = False
        logger.info("BGE-M3 agent shut down")
