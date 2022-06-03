select
    command_invocation_id,
    artifact_run_id,
    artifact_generated_at,
    node_id,
    result_json
from raw.dbt_artifacts.dbt_run_results_nodes