# AI Agent Platform

A modular, extensible AI agent platform with multi-agent orchestration, task scheduling, and recovery mechanisms.

## Project Structure

See the full structure in the repository.

## Getting Started

1. Create virtual environment: python -m venv venv
2. Activate: env\Scripts\activate (Windows)
3. Install dependencies: pip install -e .
4. Run: uvicorn src.agent_platform.api.main:app --reload

## Development

- Add new agents in src/agents/
- Core primitives in src/agent_platform/core/
- Extend via plugins in src/agent_platform/plugins/

## License

MIT
