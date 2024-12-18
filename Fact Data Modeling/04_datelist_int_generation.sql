--A datelist_int generation query. Convert the device_activity_datelist column into a datelist_int colu

with users as (
select * from user_devices_cumulated
where curr_date = '2023-01-06'
), series as (
select * from generate_series(DATE('2023-01-02'), DATE('2023-01-31'), INTERVAL '1 day') as series_date
) , placeholder_ints as (
select user_id,
(CASE WHEN device_activity_datelist @> array [DATE(series_date)]
	 THEN CAST(POW(2,31 - (curr_date - DATE(series_date))) AS BIGINT)
	 ELSE 0
END)::BIT(31) AS datelist_int	 
from users cross join series
) 
select 
user_id,
datelist_int,
BIT_COUNT(datelist_int) AS l31,
BIT_COUNT(datelist_int) > 0 AS monthly_active,
BIT_COUNT(datelist_int &
       CAST('11111110000000000000000000000000' AS BIT(31))) > 0 AS weekly_active,
BIT_COUNT(datelist_int &
       CAST('10000000000000000000000000000000' AS BIT(31))) > 0 AS daily_active
from placeholder_ints p;
