{
  "workflow_id": "wf_001",
  "name": "Data Processing Workflow",
  "description": "Fetch data, process it, then generate report",
  "steps": [
    {
      "step_id": "fetch",
      "name": "Fetch Data",
      "agent_id": "data_fetcher",
      "task_type": "fetch",
      "payload": {"source": "api", "endpoint": "/data"},
      "timeout_seconds": 30
    },
    {
      "step_id": "process",
      "name": "Process Data",
      "agent_id": "data_processor",
      "task_type": "process",
      "payload": {"operation": "aggregate"},
      "dependencies": [
        {"depends_on": "fetch"}
      ],
      "output_key": "processed_result"
    },
    {
      "step_id": "generate_report",
      "name": "Generate Report",
      "agent_id": "report_generator",
      "task_type": "generate",
      "payload": {"format": "pdf"},
      "dependencies": [
        {"depends_on": "process"}
      ]
    }
  ]
}
