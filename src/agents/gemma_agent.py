# src/agents/gemma_agent.py
# Gemma 2 2B text generation agent

import logging
import os
from typing import Any

from src.agent_platform.core.agent import AgentRuntimeState, BaseAgent
from src.agent_platform.core.task import Task

logger = logging.getLogger(__name__)

GEMMA_MODEL_PATH = os.getenv(
    "GEMMA_MODEL_PATH",
    "google/gemma-2-2b-it",
)

class GemmaAgent(BaseAgent):
    def __init__(self, model_path: str = None, device: str = "cpu", **kwargs):
        super().__init__(**kwargs)
        self.model_path = model_path or GEMMA_MODEL_PATH
        self.device = device
        self._model = None
        self._tokenizer = None

    async def initialize(self) -> None:
        logger.info(f"Loading Gemma from: {self.model_path}")
        from transformers import AutoModelForCausalLM, AutoTokenizer
        self._tokenizer = AutoTokenizer.from_pretrained(self.model_path)
        self._model = AutoModelForCausalLM.from_pretrained(
            self.model_path,
            device_map=self.device,
            torch_dtype="auto"
        )
        self._initialized = True
        self.state = AgentRuntimeState.RUNNING
        logger.info("Gemma agent initialized")

    async def run(self, task: Task) -> Any:
        if self._model is None or self._tokenizer is None:
            raise RuntimeError("Model not initialized")
        prompt = task.payload.get("prompt", "")
        if not prompt:
            raise ValueError("Missing 'prompt' in task payload")
        inputs = self._tokenizer(prompt, return_tensors="pt").to(self.device)
        outputs = self._model.generate(
            **inputs,
            max_new_tokens=task.payload.get("max_tokens", 128),
            temperature=task.payload.get("temperature", 0.7),
            do_sample=True,
        )
        response = self._tokenizer.decode(outputs[0], skip_special_tokens=True)
        if response.startswith(prompt):
            response = response[len(prompt):].lstrip()
        return response

    async def shutdown(self) -> None:
        self._model = None
        self._tokenizer = None
        self._initialized = False
        logger.info("Gemma agent shut down")
