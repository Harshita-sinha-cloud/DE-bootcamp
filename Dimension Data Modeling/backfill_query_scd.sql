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
current_year,
streak_identifier,
min(current_year) as start_year,
max(current_year) as end_year
from streaks
group by 1,2,3,4,5
;
