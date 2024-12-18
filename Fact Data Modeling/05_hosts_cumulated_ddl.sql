CREATE TABLE hosts_cumulated (
    host TEXT, 
    host_activity_datelist bigint[], 
    month_start DATE, 
	metric_name TEXT,
    PRIMARY KEY (host,month_start) 
);
