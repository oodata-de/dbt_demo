ALTER SCHEMA dbt_dev.sch_dbt_test SET LOG_LEVEL = 'INFO';
ALTER SCHEMA dbt_dev.sch_dbt_test SET TRACE_LEVEL = 'ALWAYS';
ALTER SCHEMA dbt_dev.sch_dbt_test SET METRIC_LEVEL = 'ALL';

ALTER SCHEMA dbt_dev.configs SET LOG_LEVEL = 'INFO';
ALTER SCHEMA dbt_dev.configs SET TRACE_LEVEL = 'ALWAYS';
ALTER SCHEMA dbt_dev.configs SET METRIC_LEVEL = 'ALL';