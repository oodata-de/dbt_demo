create or replace api integration int_dbt_demo_outh
    api_provider = git_https_api
    api_allowed_prefixes = ('https://github.com/oodata-de/dbt_demo')
    enabled = true
    allowed_authentication_secrets = all
    api_user_authentication = (type = snowflake_github_app ) -- enable OAuth support
    -- comment='<comment>';

CREATE OR REPLACE API INTEGRATION int_dbt_demo_no_auth
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/oodata-de/dbt_demo')
  ENABLED = TRUE;