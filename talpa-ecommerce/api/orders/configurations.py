def get_snowflake_config():
    # not a good practice to hard-code configurations, use ENVs
    return {
        'user': 'DBT',
        'password': '###',
        'account': '###.eu-north-1.aws',
        'warehouse': 'COMPUTE_WH',
        'database': 'DWH_DEV',
        'schema': 'STAGING'
    }