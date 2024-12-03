insert into actors_history_scd(actor, quality_class, is_active, start_year, end_year, current_year)
with previous as(
Select actor, quality_class, is_active, current_year,
lag(quality_class,1) over(partition by actor order by current_year ) as previous_quality_class,
lag(is_active,1) over(partition by actor order by current_year ) as previous_is_active
from actors)
, indicators as (select *,
case 
	when quality_class<>previous_quality_class then 1
	when is_active<>previous_is_active then 1
	ELSE 0
end as class_change_indicator
from previous)
, streaks as (
select *, sum(class_change_indicator) over (partition by actor order by current_year) as streak_identifier
from indicators		
)
select actor,
quality_class,
is_active,
1970 as current_year,
min(current_year) as start_year,
max(current_year) as end_year
from streaks
group by 1,2,3,4
;
