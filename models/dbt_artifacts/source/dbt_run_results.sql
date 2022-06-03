select
    command_invocation_id,
    artifact_run_id,
    artifact_generated_at,
    env,
    execution_command,
    was_full_refresh,
    metadata,
    args
from raw.dbt_artifacts.dbt_run_results