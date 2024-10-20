"""Collection of schedules"""
# from dagster_dbt import build_schedule_from_dbt_selection
# from .assets import jet_dbt_dbt_assets
# from dagster import schedule
# from source.jobs import load_amazon_reviews


# https://docs.dagster.io/concepts/partitions-schedules-sensors/schedules
# @schedule(
#     cron_schedule="0 1 * * *",
#     job=load_amazon_reviews,
#     execution_timezone="Europe/Amsterdam",
# )
# def every_weekday_1am(context):
#     date = context.scheduled_execution_time.strftime("%Y-%m-%d")
#     return {"ops": {"download_amazon_reviews": {"config": {"date": date}}}}

# schedules = [
#     build_schedule_from_dbt_selection(
#         [jet_dbt_dbt_assets],
#         job_name="materialize_dbt_models",
#         cron_schedule="0 0 * * *",
#         dbt_select="fqn:*",
#     ),
# ]
