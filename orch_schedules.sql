USE SCHEMA SCH_DBT_TEST;

CREATE OR REPLACE TABLE sch_dbt_test.dbt_model_run_selector (
    id NUMBER(10,0) AUTOINCREMENT START 1 INCREMENT 1,
    task_name       STRING,                     -- suffix used in DBT_TASK_<task_name>
    task_schema     STRING,
    run_selector    STRING,                     -- selector or model name
    run_type        STRING,                     -- 'group'|'group-selector'|'individual'
    cron_expression STRING,                     -- Snowflake cron format
    dbt_project     STRING,                     -- project name registered with EXECUTE DBT
    env      STRING DEFAULT 'dev',
    is_active       BOOLEAN,
    created_timestamp  TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP,
    updated_timestamp  TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP
);

-- Procedure that scans active selectors and creates/refreshes Snowflake tasks per selector.
CREATE OR REPLACE PROCEDURE sch_dbt_test.sync_dbt_tasks(dbt_database STRING)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    c_active CURSOR FOR
        SELECT task_name,
               task_schema,
               run_selector,
               run_type,
               cron_expression,
               dbt_project,
               env
        FROM IDENTIFIER(dbt_database || '.sch_dbt_test.dbt_model_run_selector')
        WHERE is_active;

    v_task_name STRING;
    v_task_schema STRING;
    v_sql       STRING;
    -- v_seen      STRING := '';
    v_args      STRING;
    v_resume    STRING;
BEGIN
    FOR rec IN c_active DO
        v_task_name := 'DBT_TASK_' || rec.task_name;
        v_task_schema := rec.task_schema;

        IF (rec.run_type LIKE 'group-selector%') THEN
            v_args := 'run --selector ' || rec.run_selector;
        ELSE
            v_args := 'run --select ' || rec.run_selector;
        END IF;
        v_args := v_args || ' --target ' || rec.env;

        v_sql := 'CREATE OR REPLACE TASK ' || dbt_database || '.' || v_task_schema || '.' || v_task_name || '
                  WAREHOUSE = COMPUTE_WH
                  SCHEDULE = ''' || rec.cron_expression || '''
                  USER_TASK_TIMEOUT_MS = 3600000
                  AS
                  EXECUTE DBT PROJECT ' || rec.dbt_project || '
                      ARGS = ''' || v_args || ''';';

        EXECUTE IMMEDIATE v_sql;

        v_resume := 'ALTER TASK ' || dbt_database || '.' || v_task_schema || '.' || v_task_name || ' RESUME;';

        EXECUTE IMMEDIATE v_resume;
        
    END FOR;

    RETURN 'Tasks created';
END;
$$;


MERGE INTO sch_dbt_test.dbt_model_run_selector AS target
USING (
  SELECT
    'f1' AS task_name,
    'sch_dbt_test' AS task_schema,
    'f1' AS run_selector,
    'individual' AS run_type,
    'USING CRON 0 23 * * * Canada/Pacific' AS cron_expression,
    &{DB_NAME} || '.sch_dbt_test.dbt_object_gh_action' AS dbt_project,
    'prod' AS env,
    TRUE AS is_active
  UNION ALL
  SELECT
    'domain_dependency',
    'sch_dbt_test',
    'path:models/example/dependency',
    'group',
    'USING CRON 0 23 * * * Canada/Pacific',
    &{DB_NAME} || '.sch_dbt_test.dbt_object_gh_action',
    'prod',
    TRUE
) AS source (task_name, task_schema, run_selector, run_type, cron_expression, dbt_project, env, is_active)
ON target.task_name = source.task_name AND target.task_schema = source.task_schema
WHEN NOT MATCHED THEN
  INSERT (task_name, task_schema, run_selector, run_type, cron_expression, dbt_project, env, is_active)
  VALUES (source.task_name, source.task_schema, source.run_selector, source.run_type, source.cron_expression, source.dbt_project, source.env, source.is_active);

-- Call the stored procedure
CALL  dbt_prod.sch_dbt_test.sync_dbt_tasks(&{DB_NAME});
