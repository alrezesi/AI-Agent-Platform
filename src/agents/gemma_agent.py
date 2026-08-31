import logging
import os
from pathlib import Path
from typing import Any

from src.agent_platform.core.agent import AgentRuntimeState, BaseAgent
from src.agent_platform.core.task import Task

logger = logging.getLogger(__name__)


class GemmaAgent(BaseAgent):
    def __init__(self, model_path: str | None = None, device: str = "cpu", **kwargs):
        super().__init__(**kwargs)
        self.model_path: str = model_path or os.getenv("GEMMA_MODEL_PATH") or "/app/models/gemma-2-2b-it"
        self.device = device
        self._model: Any = None
        self._tokenizer: Any = None

    async def initialize(self) -> None:
        model_path = Path(self.model_path)
        if not model_path.exists():
            raise RuntimeError(f"Required local model not found: {model_path}")
        if not model_path.is_dir():
            raise RuntimeError(f"Required local model path is not a directory: {model_path}")
        logger.info("Using local Gemma model: %s", model_path)
        import torch
        from transformers import AutoModelForCausalLM, AutoTokenizer

        self._tokenizer = AutoTokenizer.from_pretrained(str(model_path), local_files_only=True)
        self._model = AutoModelForCausalLM.from_pretrained(
            str(model_path),
            local_files_only=True,
            torch_dtype=torch.float32,
        )
        self._model.to(self.device)
        self._initialized = True
        self.state = AgentRuntimeState.RUNNING
        logger.info("Gemma agent initialized")

    async def run(self, task: Task) -> Any:
        if self._model is None or self._tokenizer is None:
            raise RuntimeError("Model not initialized")
        prompt = task.payload.get("prompt", "")
        if not prompt:
            raise ValueError("Missing 'prompt' in task payload")

        inputs = self._tokenizer(prompt, return_tensors="pt")
        inputs = {k: v.to(self.device) for k, v in inputs.items()}
        outputs = self._model.generate(
            **inputs,
            max_new_tokens=task.payload.get("max_tokens", 128),
            temperature=task.payload.get("temperature", 0.7),
            do_sample=True,
        )
        response = self._tokenizer.decode(outputs[0], skip_special_tokens=True)
        if response.startswith(prompt):
            response = response[len(prompt):].lstrip()
        return {"text": response}

    async def shutdown(self) -> None:
        self._model = None
        self._tokenizer = None
        self._initialized = False
        logger.info("Gemma agent shut down")
