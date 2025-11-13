CREATE DBT PROJECT "DBT_DEV"."CONFIGS"."DBT_DEMO" FROM $$snow://workspace/USER$DECLOUD5.PUBLIC."dbt_demo_1"/versions/live/$$ DEFAULT_TARGET = 'dev'

EXECUTE DBT PROJECT dbt_dev.configs.dbt_demo ARGS='run --select my_first_dbt_model --target dev';

EXECUTE DBT PROJECT dbt_dev.configs.dbt_demo ARGS='run --select my_first_dbt_model --target dev';
EXECUTE DBT PROJECT dbt_dev.configs.dbt_demo ARGS='run --select my_second_dbt_model --target dev';
EXECUTE DBT PROJECT dbt_dev.configs.dbt_demo ARGS='run --target dev';

ALTER DBT PROJECT "DBT_DEV"."CONFIGS"."DBT_DEMO" ADD VERSION FROM $$snow://workspace/USER$DECLOUD5.PUBLIC."dbt_demo_1"/versions/live/$$

EXECUTE DBT PROJECT dbt_dev.configs.dbt_demo ARGS='run --select my_second_dbt_model --target dev';
EXECUTE DBT PROJECT dbt_dev.configs.dbt_demo ARGS='run --target dev';

