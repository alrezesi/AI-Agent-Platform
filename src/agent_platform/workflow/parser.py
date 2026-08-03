
# Parser for workflow definitions from JSON/YAML

import json
from pathlib import Path
from typing import Dict, Any, Optional
import yaml

from .models import Workflow, WorkflowStep, StepDependency
from .exceptions import WorkflowDefinitionError


class WorkflowParser:
    """
    Parses workflow definitions from JSON or YAML files.
    """

    @staticmethod
    def parse_file(file_path: Path) -> Workflow:
        """
        Parse a workflow definition from a file (JSON or YAML).
        """
        if not file_path.exists():
            raise WorkflowDefinitionError(f"File {file_path} not found")

        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
            if file_path.suffix.lower() in ['.json']:
                data = json.loads(content)
            elif file_path.suffix.lower() in ['.yaml', '.yml']:
                data = yaml.safe_load(content)
            else:
                raise WorkflowDefinitionError(f"Unsupported file format: {file_path.suffix}")

        return WorkflowParser.parse_dict(data)

    @staticmethod
    def parse_dict(data: Dict[str, Any]) -> Workflow:
        """
        Parse a workflow from a dictionary.
        """
        try:
            # Validate required fields
            if 'workflow_id' not in data:
                raise WorkflowDefinitionError("Missing 'workflow_id'")
            if 'name' not in data:
                raise WorkflowDefinitionError("Missing 'name'")
            if 'steps' not in data or not data['steps']:
                raise WorkflowDefinitionError("Missing or empty 'steps'")

            steps = []
            for step_data in data['steps']:
                # Parse dependencies
                dependencies = []
                for dep in step_data.get('dependencies', []):
                    dependencies.append(
                        StepDependency(
                            depends_on=dep['depends_on'],
                            condition=dep.get('condition')
                        )
                    )
                step = WorkflowStep(
                    step_id=step_data['step_id'],
                    name=step_data['name'],
                    description=step_data.get('description'),
                    agent_id=step_data['agent_id'],
                    task_type=step_data['task_type'],
                    payload=step_data.get('payload', {}),
                    timeout_seconds=step_data.get('timeout_seconds', 60),
                    retry_count=step_data.get('retry_count', 0),
                    dependencies=dependencies,
                    output_key=step_data.get('output_key'),
                    fallback_step_id=step_data.get('fallback_step_id')
                )
                steps.append(step)

            workflow = Workflow(
                workflow_id=data['workflow_id'],
                name=data['name'],
                description=data.get('description'),
                version=data.get('version', '1.0.0'),
                steps=steps,
                tenant_id=data.get('tenant_id')
            )
            return workflow

        except KeyError as e:
            raise WorkflowDefinitionError(f"Missing required field in workflow definition: {e}") from e
        except Exception as e:
            raise WorkflowDefinitionError(f"Error parsing workflow: {e}") from e