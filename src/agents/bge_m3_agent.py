import logging
import os
from pathlib import Path
from typing import Any

from src.agent_platform.core.agent import AgentRuntimeState, BaseAgent
from src.agent_platform.core.task import Task

logger = logging.getLogger(__name__)

# Env-var-gated dtype selection. ``BGE_MODEL_DTYPE`` is unset in local dev
# and production, so the model loads in the default float32 (current behavior).
# In CI it is set to ``float16``, halving each worker's BGE-M3 in-RAM footprint
# (~2 GB -> ~1 GB per worker, ~4 GB -> ~2 GB total) so the host-level OOM that
# killed the ``api`` container (see ENGINEERING_AUDIT.md Addendum 8) cannot
# recur on the constrained GitHub-hosted runner.  We keep the mapping as
# strings and resolve to ``torch.dtype`` lazily inside ``initialize()`` so
# that importing this module does not require torch at import time.
_DTYPE_NAMES: dict[str, str] = {
    "float32": "float32",
    "float16": "float16",
    "bfloat16": "bfloat16",
}


class BGEM3Agent(BaseAgent):
    def __init__(self, model_path: str | None = None, device: str = "cpu", **kwargs):
        super().__init__(**kwargs)
        self.model_path: str = model_path or os.getenv("BGE_MODEL_PATH") or "/app/models/bge-m3"
        self.device = device
        self._model: Any = None

    async def initialize(self) -> None:
        model_path = Path(self.model_path)
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

        dtype_name = os.getenv("BGE_MODEL_DTYPE", "float32").lower()
        torch_dtype_name = _DTYPE_NAMES.get(dtype_name, "float32")
        model_kwargs: dict[str, Any] | None = None
        if torch_dtype_name != "float32":
            model_kwargs = {"torch_dtype": getattr(torch, torch_dtype_name)}
            logger.info("Loading BGE-M3 in %s precision", torch_dtype_name)

        self._model = SentenceTransformer(
            str(model_path),
            device=self.device,
            cache_folder=os.getenv("SENTENCE_TRANSFORMERS_HOME", "/app/models"),
            model_kwargs=model_kwargs,
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
