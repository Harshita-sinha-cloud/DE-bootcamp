--Cumulative table generation query: Write a query that populates the actors table one year at a time.
	
INSERT INTO ACTORS
WITH YESTERDAY AS (
	SELECT 
	actor,
	actorid,
	films,
	quality_class,
	is_active,
	current_year
	FROM ACTORS
	WHERE CURRENT_YEAR= 1969
)
, TODAY AS (
	SELECT 
	actor, 
	actorid, 
	array_agg (row(film, votes, rating, filmid)::films) films,
	avg(rating) as avg_rating,
	max(year) as year
	FROM ACTOR_FILMS
	WHERE YEAR = 1970
	group by 1,2
)
select 
	coalesce(t.actor, y.actor) as actor,
	coalesce(t.actorid, y.actorid) as actorid,
	CASE
		WHEN y.films IS NULL THEN t.films
		WHEN t.films IS NOT NULL THEN y.films || t.films 
                ELSE y.films
        END AS films,
	
	CASE 
        WHEN t.avg_rating IS NULL THEN y.quality_class
        WHEN avg_rating > 8 THEN 'star'
        WHEN avg_rating > 7 AND avg_rating <= 8 THEN 'good'
        WHEN avg_rating > 6 AND avg_rating <= 7 THEN 'average'
        WHEN avg_rating <= 6 THEN 'bad'
	END::quality_class as quality_class,
	
	CASE WHEN t.year is not null then TRUE else FALSE END is_active,
	COALESCE(t.year, y.current_year+1) current_year
from today t full outer join yesterday y 
on 
t.actor = y.actor
and t.actorid = y.actorid
;
