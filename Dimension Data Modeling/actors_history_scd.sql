create table actors_history_scd(
actor text,
quality_class quality_class,
start_year integer,
end_year integer,
current_year integer,
is_active bool,
PRIMARY KEY (actor, start_year, end_year) 
);
