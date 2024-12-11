-- - A DDL for an `user_devices_cumulated` table that has:
--   - a `device_activity_datelist` which tracks a users active days by `browser_type`
--   - data type here should look similar to `MAP<STRING, ARRAY[DATE]>`
--     - or you could have `browser_type` as a column with multiple rows for each user (either way works, just be consistent!)

CREATE TABLE user_devices_cumulated (
	dim_user_id INTEGER,
	dim_device_id INTEGER,
	browser_type TEXT,
	curr_date DATE,
	device_activity_datelist DATE[],
	PRIMARY KEY (dim_user_id, curr_date)
);
