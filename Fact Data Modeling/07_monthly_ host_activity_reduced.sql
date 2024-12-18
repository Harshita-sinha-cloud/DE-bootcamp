WITH agg AS (
    SELECT metric_name, month_start, 
	ARRAY[SUM(host_activity_datelist[1]), 
		  SUM(host_activity_datelist[2]), 
		  SUM(host_activity_datelist[3])] AS summed_array
    FROM hosts_cumulated
    GROUP BY 1,2
)
-- Select and display the metric_name, date (adjusted by index), and summed value
SELECT 
    metric_name, 
    month_start + CAST(CAST(index - 1 AS TEXT) || ' day' AS INTERVAL) AS adjusted_date,
    elem AS value
FROM agg
CROSS JOIN UNNEST(agg.summed_array) WITH ORDINALITY AS a(elem, index);
