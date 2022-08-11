select * 
from {{ metrics.metric(
    metric_name='active_users',
    grain='week',
    dimensions=[],
) }}