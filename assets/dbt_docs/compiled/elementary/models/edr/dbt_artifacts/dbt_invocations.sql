

select * from (
            select
        cast(
            'this_is_just_a_long_dummy_string'
            as varchar
        ) as invocation_id

,
        cast(
            'this_is_just_a_long_dummy_string'
            as varchar
        ) as job_id

,
        cast(
            'this_is_just_a_long_dummy_string'
            as varchar
        ) as job_name

,
        cast(
            'this_is_just_a_long_dummy_string'
            as varchar
        ) as job_run_id

,
        cast(
            'dummy_string' as varchar
        ) as run_started_at

,
        cast(
            'dummy_string' as varchar
        ) as run_completed_at

,
        cast(
            'dummy_string' as varchar
        ) as generated_at

,cast(
            '2091-02-17' as  timestamp(6) 
        ) as created_at

,
        cast(
            'dummy_string' as varchar
        ) as command

,
        cast(
            'dummy_string' as varchar
        ) as dbt_version

,
        cast(
            'dummy_string' as varchar
        ) as elementary_version

,
        cast(
            True as boolean
        ) as full_refresh

,
        cast(
            'this_is_just_a_long_dummy_string'
            as varchar
        ) as invocation_vars

,
        cast(
            'this_is_just_a_long_dummy_string'
            as varchar
        ) as vars

,
        cast(
            'dummy_string' as varchar
        ) as target_name

,
        cast(
            'dummy_string' as varchar
        ) as target_database

,
        cast(
            'dummy_string' as varchar
        ) as target_schema

,
        cast(
            'dummy_string' as varchar
        ) as target_profile_name

,
        cast(
            123456789 as integer
        ) as threads

,
        cast(
            'this_is_just_a_long_dummy_string'
            as varchar
        ) as selected

,
        cast(
            'this_is_just_a_long_dummy_string'
            as varchar
        ) as yaml_selector

,
        cast(
            'dummy_string' as varchar
        ) as project_id

,
        cast(
            'dummy_string' as varchar
        ) as project_name

,
        cast(
            'dummy_string' as varchar
        ) as env

,
        cast(
            'dummy_string' as varchar
        ) as env_id

,
        cast(
            'dummy_string' as varchar
        ) as cause_category

,
        cast(
            'this_is_just_a_long_dummy_string'
            as varchar
        ) as cause

,
        cast(
            'dummy_string' as varchar
        ) as pull_request_id

,
        cast(
            'dummy_string' as varchar
        ) as git_sha

,
        cast(
            'dummy_string' as varchar
        ) as orchestrator

,
        cast(
            'dummy_string' as varchar
        ) as dbt_user

,
        cast(
            'dummy_string' as varchar
        ) as job_url

,
        cast(
            'dummy_string' as varchar
        ) as job_run_url

,
        cast(
            'dummy_string' as varchar
        ) as account_id

,
        cast(
            'this_is_just_a_long_dummy_string'
            as varchar
        ) as target_adapter_specific_fields


        ) as empty_table
        where 1 = 0