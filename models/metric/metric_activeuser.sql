select * 
from {{ metrics.metric(
    metric_name='average_amount',
    grain='week',
    dimensions=[],
) }}