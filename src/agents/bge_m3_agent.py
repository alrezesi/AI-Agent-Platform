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
        os.environ.setdefault("TOKENIZERS_PARALLELISM", "false")
        os.environ.setdefault("OMP_NUM_THREADS", os.getenv("OMP_NUM_THREADS", "1"))
        os.environ.setdefault("MKL_NUM_THREADS", os.getenv("MKL_NUM_THREADS", "1"))
        os.environ.setdefault("OPENBLAS_NUM_THREADS", os.getenv("OPENBLAS_NUM_THREADS", "1"))
        os.environ.setdefault("NUMEXPR_NUM_THREADS", os.getenv("NUMEXPR_NUM_THREADS", "1"))
        os.environ.setdefault("VECLIB_MAXIMUM_THREADS", os.getenv("VECLIB_MAXIMUM_THREADS", "1"))
        import torch
        from sentence_transformers import SentenceTransformer

        torch.set_num_threads(int(os.getenv("TORCH_NUM_THREADS", "1")))
        torch.set_num_interop_threads(int(os.getenv("TORCH_NUM_INTEROP_THREADS", "1")))

        self._model = SentenceTransformer(
            str(model_path),
            device=self.device,
            cache_folder=os.getenv("SENTENCE_TRANSFORMERS_HOME", "/app/models"),
        )
        try:
            self._model.max_seq_length = int(os.getenv("BGE_MAX_SEQ_LENGTH", "512"))
        except Exception:
            pass
        self._initialized = True
        self.state = AgentRuntimeState.RUNNING
        logger.info("BGE-M3 agent initialized")

    async def run(self, task: Task) -> Any:
        if self._model is None:
            raise RuntimeError("Model not initialized")
        text = task.payload.get("text", "")
        if not text:
            raise ValueError("Missing 'text' in task payload")
        import torch

        with torch.inference_mode():
            embedding = self._model.encode(
                text,
                normalize_embeddings=True,
                convert_to_numpy=True,
                show_progress_bar=False,
            )
        return {"embedding": embedding.tolist()}

    async def shutdown(self) -> None:
        self._model = None
        self._initialized = False
        logger.info("BGE-M3 agent shut down")
