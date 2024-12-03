create type scd as (
quality_class quality_class,
is_active boolean,
start_year integer,
end_year integer
);

with last_year_scd as(
select * from actors_history_scd where current_year = 1970
and end_year = 1970
),historical_scd as (
select actor,
quality_class,
is_active,
start_year,
end_year
from actors_history_scd
where current_year = 1970 and end_year < 1970
)
,this_year_scd as (
select * from actors_history_scd where current_year = 1971
)
, unchanged_record as (
select 
	t.actor,
	t.quality_class,
	t.is_active,
	t.start_year,
	t.current_year as end_year
from this_year_scd t join last_year_scd l 
on t.actor = l.actor
and t.quality_class = l.quality_class
and t.is_active = l.is_active
)
, changed_records as (
select 
	t.actor,
	t.quality_class,
	t.is_active,
	t.start_year,
	t.current_year as end_year,
	UNNEST(ARRAY[	
		ROW(
		t.quality_class,
		t.is_active,
		t.start_year,
		t.end_year
		)::scd,
		ROW(
		l.quality_class,
		l.is_active,
		l.current_year,
		l.current_year
		)::scd
		]) as records
from this_year_scd t 
left join last_year_scd l 
on t.actor = l.actor
where t.quality_class <> l.quality_class
OR t.is_active <> l.is_active
OR l.actor IS NULL
)
, unnested_change_records as (
	select actor, 
	(records::scd).quality_class,
	(records::scd).is_active,
	(records::scd).start_year,
	(records::scd).end_year
	from changed_records
),
new_records as(
select t.actor,
		t.quality_class,
		t.is_active,
		t.current_year as start_year,
		t.end_year as end_year
from this_year_scd t 
left join last_year_scd l 
on t.actor = l.actor
where l.actor is null)
select * from historical_scd
UNION ALL
select * from unchanged_record
UNION ALL
select * from unnested_change_records
UNION ALL
select * from new_records
;


