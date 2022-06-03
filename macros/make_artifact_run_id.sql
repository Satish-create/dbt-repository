{% macro make_artifact_run_id() %}
    sha2_hex(coalesce(dbt_cloud_run_id::string, command_invocation_id::string), 256)
{% endmacro %}