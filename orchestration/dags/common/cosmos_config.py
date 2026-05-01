from cosmos.config import ProfileConfig, ProjectConfig, ExecutionConfig
from cosmos.constants import ExecutionMode, InvocationMode, SourceRenderingBehavior
from cosmos import DbtTaskGroup, RenderConfig, LoadMode


DBT_PROJECT_PATH = "/opt/airflow/transform" 
DBT_EXECUTABLE_PATH = "/opt/airflow/dbt_venv/bin/dbt"

profile_config = ProfileConfig(
    profile_name="transform", 
    target_name="dev",
    profiles_yml_filepath=f"{DBT_PROJECT_PATH}/profiles.yml"
)

project_config = ProjectConfig(dbt_project_path=DBT_PROJECT_PATH,
                               install_dbt_deps=False,
                               dbt_vars={"airflow_run_id": "{{ run_id }}"}
                               )

execution_config = ExecutionConfig(dbt_executable_path=DBT_EXECUTABLE_PATH,
                                   execution_mode=ExecutionMode.LOCAL,
                                   invocation_mode=InvocationMode.SUBPROCESS)

