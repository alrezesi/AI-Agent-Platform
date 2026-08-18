import logging
import os
from pathlib import Path
from typing import Any

from src.agent_platform.core.agent import AgentRuntimeState, BaseAgent
from src.agent_platform.core.task import Task

logger = logging.getLogger(__name__)


class BGEM3Agent(BaseAgent):
    def __init__(self, model_path: str | None = None, device: str = "cpu", **kwargs):
        super().__init__(**kwargs)
        self.model_path = model_path or os.getenv("BGE_MODEL_PATH", "/app/models/bge-m3")
        self.device = device
        self._model = None

    async def initialize(self) -> None:
        model_path = Path(self.model_path or os.getenv("BGE_MODEL_PATH", "/app/models/bge-m3"))
        if not model_path.exists():
            raise RuntimeError(f"Required local model not found: {model_path}")
        if not model_path.is_dir():
            raise RuntimeError(f"Required local model path is not a directory: {model_path}")
        logger.info("Using local BGE-M3 model: %s", model_path)
        from sentence_transformers import SentenceTransformer

        self._model = SentenceTransformer(str(model_path), device=self.device)
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
        return {"embedding": embedding.tolist()}

    async def shutdown(self) -> None:
        self._model = None
        self._initialized = False
        logger.info("BGE-M3 agent shut down")
