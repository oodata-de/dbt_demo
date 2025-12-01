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
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
const dbName = DBT_DATABASE;  // parameter auto-uppercased

const selectorTable = `${dbName}.sch_dbt_test.dbt_model_run_selector`;
const selectSql = `
  SELECT task_name, task_schema, run_selector, run_type,
         cron_expression, dbt_project, env
  FROM ${selectorTable}
  WHERE is_active
`;
const rs = snowflake.createStatement({ sqlText: selectSql }).execute();

while (rs.next()) {
  const taskName   = 'DBT_TASK_' + rs.getColumnValue('TASK_NAME');
  const taskSchema = rs.getColumnValue('TASK_SCHEMA');
  const runType    = rs.getColumnValue('RUN_TYPE');
  const selector   = rs.getColumnValue('RUN_SELECTOR');
  const cron       = rs.getColumnValue('CRON_EXPRESSION');
  const dbtProj    = rs.getColumnValue('DBT_PROJECT');
  const env        = rs.getColumnValue('ENV');

  const args = (runType.startsWith('group-selector')
               ? `run --selector ${selector}`
               : `run --select ${selector}`) + ` --target ${env}`;

  const createTaskSql = `
    CREATE OR REPLACE TASK ${dbName}.${taskSchema}.${taskName}
      WAREHOUSE = COMPUTE_WH
      SCHEDULE = '${cron}'
      USER_TASK_TIMEOUT_MS = 3600000
    AS
      EXECUTE DBT PROJECT ${dbtProj}
        ARGS = '${args}'
  `;
  snowflake.createStatement({ sqlText: createTaskSql }).execute();

  const resumeSql = `ALTER TASK ${dbName}.${taskSchema}.${taskName} RESUME`;
  snowflake.createStatement({ sqlText: resumeSql }).execute();
}

return 'Tasks created';
$$;

SET db_name = &{DB_NAME};
MERGE INTO sch_dbt_test.dbt_model_run_selector AS target
USING (
SELECT *
  FROM  
    (VALUES
      ('f1','sch_dbt_test','f1','individual','USING CRON 0 23 * * * Canada/Pacific', CONCAT($db_name, '.sch_dbt_test.dbt_object_gh_action'),'prod',TRUE),
      ('domain_dependency','sch_dbt_test','path:models/example/dependency','group', 'USING CRON 0 23 * * * Canada/Pacific', CONCAT($db_name, '.sch_dbt_test.dbt_object_gh_action'),'prod',TRUE)
  ) AS source
ON target.task_name = source.task_name AND target.task_schema = source.task_schema
WHEN NOT MATCHED THEN
  INSERT (task_name, task_schema, run_selector, run_type, cron_expression, dbt_project, env, is_active)
  VALUES (source.task_name, source.task_schema, source.run_selector, source.run_type, source.cron_expression, source.dbt_project, source.env, source.is_active);

-- Call the stored procedure
CALL  dbt_prod.sch_dbt_test.sync_dbt_tasks(&{DB_NAME});
