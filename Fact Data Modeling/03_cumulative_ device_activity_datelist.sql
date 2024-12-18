--A cumulative query to generate device_activity_datelist from events
  
INSERT INTO user_devices_cumulated (user_id, device_id, browser_type, device_activity_datelist, curr_date)
with yesterday as (
select * 
from user_devices_cumulated 
where curr_date = DATE('2023-01-06' )
), 
today as (
select 
CAST(e.user_id as text) as user_id,
CAST(e.device_id AS TEXT) as device_id,
d.browser_type as browser_type,
DATE(CAST(e.event_time as timestamp)) as date_active
from events e
inner join devices d on CAST(d.device_id AS TEXT) = CAST(e.device_id AS TEXT)
where DATE(event_time)  = '2023-01-07' 
and e.user_id is not null
group by 1,2,3,4
)
select 
coalesce(t.user_id, y.user_id) as user_id,
coalesce(t.device_id, y.device_id) as device_id,
coalesce(t.browser_type, y.browser_type) as browser_type,
CASE WHEN y.device_activity_datelist IS NULL THEN ARRAY[t.date_active]
	 WHEN t.date_active is null then y.device_activity_datelist
	 ELSE y.device_activity_datelist || ARRAY [CAST(t.date_active as TIMESTAMPTZ)]
END	as device_activity_datelist,
coalesce(t.date_active, y.curr_date + INTERVAL '1 day') as curr_date
from today t
full outer join yesterday y on t.user_id = y.user_id and t.device_id = y.device_id
;
