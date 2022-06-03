select
    command_invocation_id,
    artifact_run_id,
    artifact_generated_at,
    node_id,
    node_json
from raw.dbt_artifacts.dbt_manifest_nodes