-- A query to deduplicate `game_details` from Day 1 so there's no duplicates
with dedup as (
	SELECT g.game_id,
	g.game_date_est,
	g.season,
	gd.start_position,
	g.home_team_id,
	g.visitor_team_id,
	gd.team_id,
	gd.comment,
	min,
	fgm,
	fga,
	fg3m,
	fg3a,
	ftm,
	fta,
	oreb,
	dreb,
	reb,
	ast,
	stl,
	blk,
	gd."TO" AS turnovers,
	pf,
	pts,
	plus_minus,
	row_number() over (partition by GD.game_id, GD.team_id, GD.player_id ORDER BY G.game_date_est desc) rn
	FROM GAME_DETAILS GD
	JOIN GAMES G ON G.GAME_ID = GD.GAME_ID
)
select 
	game_date_est,
	season,
	home_team_id,
	visitor_team_id,
	team_id,
	team_id = home_team_id as dim_at_home_playing,
	start_position,
	COALESCE(POSITION('DNP' in comment),0) > 0 as dim_did_not_play,
	COALESCE(POSITION('DND' in comment),0) > 0 as dim_did_not_dress,
	COALESCE(POSITION('NWT' in comment),0) > 0 as dim_not_with_team,
	comment,
	CAST(split_part(min,':',1) AS REAL) + CAST(split_part(min, ':',2) as REAL)/60 as minutes,
	fgm,
	fga,
	fg3m,
	fg3a,
	ftm,
	fta,
	oreb,
	dreb,
	reb,
	ast,
	stl,
	blk,
	turnovers,
	pf,
	pts,
	plus_minus
	
from dedup
where rn =1
;
