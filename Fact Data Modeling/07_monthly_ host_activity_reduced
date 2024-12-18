
INSERT INTO hosts_cumulated(host, month_start, metric_name, host_activity_datelist)
with yesterday_array as (
	select host, 
		month_start,
		host_activity_datelist,
		'site_hits' as metric_name
	from hosts_cumulated
	where month_start= '2022-01-02'
	and host is not null
) , daily_Aggregate as (
	SELECT
        host,
        DATE(event_time) AS date,
		'site_hits' as metric_name,
		count(1) as num_site_hits
    FROM events
    WHERE
        DATE(event_time) = DATE('2023-01-03')
      	AND user_id IS NOT NULL
    GROUP BY 1,2,3
)
select 
	COALESCE(ya.host, da.host ) as host,
	COALESCE(ya.month_start, DATE_TRUNC('month', da.date)) AS month_start,
	'site_hits' as metric_name,
    CASE 
        WHEN ya.host_activity_datelist IS NOT NULL THEN ya.host_activity_datelist || ARRAY[COALESCE(da.num_site_hits,0)] 
        WHEN ya.host_activity_datelist IS NULL THEN ARRAY_FILL(0, ARRAY[COALESCE (date - DATE(DATE_TRUNC('month', date)), 0)]) || ARRAY[COALESCE(da.num_site_hits,0)]
    END AS host_activity_datelist
from daily_aggregate da
full outer join yesterday_array ya on da.host = ya.host and da.date = ya.month_start
ON CONFLICT (host, month_start)
DO 
    UPDATE SET host_activity_datelist = EXCLUDED.host_activity_datelist;
;
