create table actors_history_scd(
actor text,
actorid text,
quality_class quality_class,
start_date integer,
end_date integer,
current_year integer,
is_active bool,
PRIMARY KEY (actor, start_date) 
);
