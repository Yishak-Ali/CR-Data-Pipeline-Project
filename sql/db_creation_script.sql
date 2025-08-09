-- CREATE DATABASE clash_royale;
-- GO

USE clash_royale;
GO

-- || DROP OBJECTS IF EXIST || --

-- DROP VIEW IF EXISTS vw_recent_rankings;
-- DROP VIEW IF EXISTS vw_player_clan;
-- DROP TABLE IF EXISTS season_rankings;
-- DROP TABLE IF EXISTS match_cards;
-- DROP TABLE IF EXISTS cards;
-- DROP TABLE IF EXISTS matches;
-- DROP TABLE IF EXISTS seasons;
-- DROP TABLE IF EXISTS players;
-- DROP TABLE IF EXISTS clans;
-- GO

-- || CREATE DATABASE TABLES || --

CREATE TABLE seasons (
	season_id VARCHAR(10) NOT NULL,
	sn_start_date DATETIME2 NOT NULL,
	sn_end_date DATETIME2 NOT NULL,
	CONSTRAINT pk_seasons PRIMARY KEY (season_id),
	CONSTRAINT ck_season_dates CHECK (sn_start_date < sn_end_date)
);
GO

CREATE TABLE season_rankings (
	player_id VARCHAR(20) NOT NULL,
	season_id VARCHAR(10) NOT NULL,
	rank SMALLINT,
	rating SMALLINT,
	CONSTRAINT pk_season_rankings PRIMARY KEY (player_id, season_id)
); 
GO

CREATE TABLE players (
	player_id VARCHAR(20) NOT NULL,
	player_name NVARCHAR(50),
	exp_lvl TINYINT, -- current max level is 70
	road_trophies SMALLINT,
	best_road_trophies SMALLINT,
	wins INT,
	losses INT,
	life_time_battles INT,
	max_challenge_wins TINYINT, -- max 20 possible
	clan_id VARCHAR(20),
	url_encoded_pid VARCHAR(25),
	CONSTRAINT pk_players PRIMARY KEY (player_id),
	CONSTRAINT uq_url_encoded_pid UNIQUE(url_encoded_pid)
);
GO

CREATE TABLE clans (
	clan_id VARCHAR(20) NOT NULL,
	clan_name NVARCHAR(100),
	clan_type VARCHAR(20),
	badge_id VARCHAR(20),
	clan_score INT,
	clan_war_trophies INT,
	clan_location VARCHAR(20),
	required_trophies SMALLINT,
	members TINYINT,
	url_encoded_cid VARCHAR(25),
	CONSTRAINT pk_clans PRIMARY KEY (clan_id)
);
GO

CREATE TABLE cards (
	card_id VARCHAR(20) NOT NULL,
	card_name VARCHAR(50),
	rarity VARCHAR(20),
	elixir_cost TINYINT, -- max 10, 0 represents wildcard elixir value for mirror
	evo_status BIT, -- 0 for no evolution, 1 for evolution
	CONSTRAINT pk_cards PRIMARY KEY (card_id),
	CONSTRAINT ck_elixir_cost CHECK (elixir_cost >= 0 AND elixir_cost <= 10)
);
GO

/* Note this table doesn't store unique matches; it stores distinct perspectives of matches.
Thus, it is possible to have two perspectives or rows for a match if both players are tracked top players in players table) */

CREATE TABLE matches ( 
	match_view_id INT IDENTITY(1,1),
	match_key VARCHAR(50) NOT NULL,
	battle_time DATETIME2 NOT NULL,
	is_win BIT, -- 1 is win
	league TINYINT, -- max 7
	player_id VARCHAR(20) NOT NULL,
	opponent_id VARCHAR(20),
	season_id VARCHAR(10) NOT NULL,
	current_global_rank SMALLINT,
	starting_rating SMALLINT,
	rating_change SMALLINT,
	crowns TINYINT NOT NULL,
	opp_crowns TINYINT NOT NULL,
	king_tower_hp SMALLINT,
	princess_tower1_hp SMALLINT,
	princess_tower2_hp SMALLINT,
	elixir_leaked DECIMAL(5,2),
	CONSTRAINT pk_matches PRIMARY KEY (match_view_id),
	CONSTRAINT uq_match_key UNIQUE (match_key)

);
GO

-- Like with matches table, card data from two perspectives of the same match are possible
CREATE TABLE match_cards (
	match_view_id INT NOT NULL,
	player_id VARCHAR(20) NOT NULL,
	card_id VARCHAR(20) NOT NULL
	CONSTRAINT pk_match_cards PRIMARY KEY (match_view_id, player_id, card_id)
);
GO
	
-- || ESTABLISH TABLE RELATIONSHIPS || --

-- season_rankings to seasons on season_id
ALTER TABLE season_rankings
	ADD CONSTRAINT fk_season_rankings_seasons FOREIGN KEY (season_id) 
		REFERENCES seasons (season_id);

-- season_rankings to players on player_id
ALTER TABLE season_rankings
	ADD CONSTRAINT  fk_season_rankings_players FOREIGN KEY (player_id)
		REFERENCES players (player_id);

-- players to clans on clan_id
ALTER TABLE players
	ADD CONSTRAINT  fk_players_clans FOREIGN KEY (clan_id)
		REFERENCES clans (clan_id);

-- matches to players on player_id
ALTER TABLE matches
	ADD CONSTRAINT  fk_matches_players FOREIGN KEY (player_id)
		REFERENCES players (player_id);

-- matches to seasons on season_id
ALTER TABLE matches
	ADD CONSTRAINT  fk_matches_season FOREIGN KEY (season_id)
		REFERENCES seasons (season_id);

-- match_cards to matches on match_view_id
ALTER TABLE match_cards
	ADD CONSTRAINT fk_match_cards_matches FOREIGN KEY (match_view_id)
		REFERENCES matches (match_view_id);

-- match_cards to players on player_id
ALTER TABLE match_cards
	ADD CONSTRAINT fk_match_cards_players FOREIGN KEY (player_id)
		REFERENCES players (player_id);

-- match_cards to cards on card_id
ALTER TABLE match_cards
	ADD CONSTRAINT fk_match_cards_cards FOREIGN KEY (card_id)
		REFERENCES cards (card_id);


-- || CREATE INDEXES || --

 CREATE NONCLUSTERED INDEX idx_season_rankings_pid
	ON season_rankings (player_id);

 CREATE NONCLUSTERED INDEX idx_matches_pid
	ON matches (player_id);

 CREATE NONCLUSTERED INDEX idx_matches_key
	ON matches (match_key);

 CREATE NONCLUSTERED INDEX idx_players_cid
	ON players (clan_id);

 CREATE NONCLUSTERED INDEX idx_match_card_vid
	ON match_cards (match_view_id);

 CREATE NONCLUSTERED INDEX idx_match_card_composite
	ON match_cards (player_id, card_id);
GO

-- || CREATE VIEWS | --

CREATE VIEW vw_recent_rankings AS -- most recently completed sn rankings
	SELECT TOP 100 s.season_id,
				   p.player_id,
		           p.player_name,
				   s.rank,
				   s.rating
	FROM season_rankings s
	JOIN players p ON s.player_id = p.player_id 
	WHERE season_id = (SELECT MAX(season_id) FROM season_rankings)
	ORDER BY rank;
GO

CREATE VIEW vw_player_clan AS -- view on clans players are in
	SELECT p.player_id,
		   p.player_name,
		   c.clan_name,
		   c.clan_score,
		   c.members
	FROM players p
	JOIN clans c ON p.clan_id = c.clan_id;
GO

-- || CREATE STORED PROCEDURES || --

CREATE OR ALTER PROCEDURE usp_player_win_rate (@player VARCHAR(20), @season VARCHAR(10)) 
AS
-- returns a player's win rate for given season (based on available matches)
BEGIN
	SELECT player_id,
		   CASE 
		   		WHEN COUNT(match_view_id) = 0 THEN NULL
		   		ELSE ROUND((SUM(CAST(is_win AS FLOAT)) / COUNT(match_view_id)) * 100, 2)
		   END AS win_rate
	FROM matches
	WHERE season_id = @season AND player_id = @player
	GROUP BY player_id
END;
GO

CREATE OR ALTER PROCEDURE usp_card_usage_wins (@card VARCHAR(50), @season VARCHAR(10)) 
AS
/* returns how often a given card appears in top player decks and its win rate across 
those matches for a given season */
BEGIN
	SELECT c.card_id,
		   c.card_name,
		   CAST((COUNT(m.match_view_id) * 1.0 / (SELECT COUNT(match_view_id)
		   							             FROM matches
									       		 WHERE season_id = @season)) * 100 AS DECIMAL (5, 2)) AS usage_rate,
		   ROUND(SUM(CAST(is_win AS FLOAT)) / COUNT(m.match_view_id) * 100, 2) AS win_rate 
	FROM cards c
	JOIN match_cards mc ON c.card_id = mc.card_id
	JOIN matches m ON mc.match_view_id = m.match_view_id
	WHERE m.season_id = @season AND c.card_name = @card
	GROUP BY c.card_id, c.card_name
	ORDER BY usage_rate DESC, win_rate DESC
END;
GO