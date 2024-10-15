--Task 5
--You have table with game sessions, their start and end times, also player_id is given.
--if two seesions for one player have difference less than 5 minutes we consider them as one. 
--For example:
--Session_id - 1, player_id - 1, start_time - 2021-01-01 15:21:12, end_time - 2021-01-01 15:31:12
--Session_id - 2, player_id - 1, start_time - 2021-01-01 15:34:35, end_time - 2021-01-01 15:55:18
-- this two sessions will be considered as one with duration of 34 min 6 sec 

CREATE TABLE game_sessions (
    session_id INT PRIMARY KEY,
	player_id int not null,
	country varchar(10) not null,
    start_time datetime2(0) NOT NULL,
    end_time  datetime2(0) NOT NULL
);
--You have to calculate the duration of all sessions for one player within a day in minutes.
--You have to show duration of longest and shortest session for one player within a day in minutes
--You have to show rank of the user within the country. Users with longest duration of all sessions have the higher rank.

INSERT INTO game_sessions VALUES
    (1, 1, 'UK', '2021-01-01 00:01:00', '2021-01-01 00:10:00'),
    (2, 1, 'UK', '2021-01-01 00:12:00', '2021-01-01 01:24:00'),
    (3, 1, 'UK', '2021-01-01 10:01:00', '2021-01-01 15:10:00'),
    (4, 2, 'UK', '2021-01-01 05:01:00', '2021-01-01 06:10:00'),
    (5, 2, 'UK', '2021-01-01 06:14:00', '2021-01-01 08:00:00'),
    (6, 3, 'UK', '2021-01-01 15:01:00', '2021-01-01 15:10:00'),
    (7, 3, 'UK', '2021-01-01 15:20:00', '2021-01-01 16:00:00'),
    (8, 3, 'UK', '2021-01-01 16:45:00', '2021-01-01 23:40:00'),
    (9, 4, 'US', '2021-01-01 00:30:00', '2021-01-01 01:10:00'),
    (10, 4, 'US', '2021-01-01 01:12:00', '2021-01-01 06:25:00'),
    (11, 5, 'US', '2021-01-01 02:10:00', '2021-01-01 02:50:00'),
    (12, 5, 'US', '2021-01-01 06:01:00', '2021-01-01 07:00:00'),
	(13, 1, 'UK', '2021-01-01 01:27:00', '2021-01-01 01:50:00'),
	(14, 4, 'US', '2021-01-01 06:27:00', '2021-01-01 09:10:00'),
	(15, 4, 'US', '2021-01-01 10:10:00', '2021-01-01 10:20:00'),
	(16, 1, 'UK', '2021-01-01 17:10:00', '2021-01-01 17:20:00'),
	(17, 1, 'UK', '2021-01-01 17:22:00', '2021-01-01 17:55:00');


--result
--player_id	  country  duration_of_all_sessions  shortest_session longest_session   rank_of_the_user
--	3			UK				464					   9			  415				   1
--	1			UK				463					   45			  309				   2
--	2			UK				179					   179			  179				   3
--	4			US				530					   10			  520				   1
--	5			US				99					   40			  59				   2