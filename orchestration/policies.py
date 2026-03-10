from datetime import timedelta

from dagster import FreshnessPolicy

quarterly_freshness_policy = FreshnessPolicy.cron(
    deadline_cron="0 0 1 */3 *", lower_bound_delta=timedelta(days=92)
)

monthly_freshness_policy = FreshnessPolicy.cron(
    deadline_cron="0 0 1 * *", lower_bound_delta=timedelta(days=31)
)

annual_freshness_policy = FreshnessPolicy.cron(
    deadline_cron="0 0 1 1 *", lower_bound_delta=timedelta(days=365)
)
