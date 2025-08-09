# set up environment
import json
import pandas as pd
import os
from sqlalchemy import create_engine, text

# create function to create connection to sql db
def get_engine():
    
    # get absolute path to config file relative to current script
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config_path = os.path.join(project_root, 'configs', 'config.json')
    
    with open(config_path) as f:
        config = json.load(f)

    server = config["server"]
    database = config["database"]
    username = config["username"]
    password = config["password"]

    connection_string = (
        f'mssql+pyodbc://{username}:{password}@{server}/{database}'
        '?driver=ODBC+Driver+17+for+SQL+Server'
        '&charset=utf8'
        '&autocommit=True'
    )

    return create_engine(connection_string, connect_args={'unicode_results': True})

# create function to get existing values from any column of db table
def get_existing_data(engine, column, table):
    with engine.connect() as conn:
        result = conn.execute(text(f'SELECT DISTINCT {column} FROM {table}'))
        return [getattr(row, column) for row in result]

# create function to get match_view_id:match_key mapping in matches db table
def get_match_key_mapping (engine):
    with engine.connect() as conn:
        match_key_map = pd.read_sql('SELECT match_view_id, match_key FROM matches;', conn)
        return match_key_map

# create function to insert new rows of data into target db table
def insert_new_rows(engine, source_df, target_table, method):
    source_df.to_sql(name=target_table,
                     con=engine,
                     if_exists='append',
                     index=False, 
                     method=method
    )

# create function to delete rows from db (for deleted / banned players)
def purge_failed_players(engine, failed_players):
    with engine.begin() as conn:
        for player_id in failed_players:
            conn.execute(text("DELETE FROM match_cards WHERE player_id = :pid"), {"pid": player_id})
            conn.execute(text("DELETE FROM matches WHERE player_id = :pid"), {"pid": player_id})
            conn.execute(text("DELETE FROM season_rankings WHERE player_id = :pid"), {"pid": player_id})
            conn.execute(text("DELETE FROM players WHERE player_id = :pid"), {"pid": player_id})

# create function to upsert (update existing + insert new) player data in players db table
def upsert_player_info(engine, df):
     player_info_tuples = df.values.tolist() # query expects column values passed as tuples
     
     query = '''
                MERGE INTO players AS target
                USING (VALUES (?,?,?,?,?,?,?,?,?,?,?)) AS 
                    source (player_id, player_name, exp_lvl, road_trophies, best_road_trophies, wins,
                            losses, life_time_battles, max_challenge_wins, clan_id, url_encoded_pid)
                ON target.player_id = source.player_id
                WHEN MATCHED THEN
                    UPDATE SET
                        player_name = source.player_name,
                        exp_lvl = source.exp_lvl,
                        road_trophies = source.road_trophies,
                        best_road_trophies = source.best_road_trophies,
                        wins = source.wins,
                        losses = source.losses,
                        life_time_battles = source.life_time_battles,
                        max_challenge_wins = source.max_challenge_wins,
                        clan_id = source.clan_id,
                        url_encoded_pid = source.url_encoded_pid
                WHEN NOT MATCHED THEN
                    INSERT (player_id, player_name, exp_lvl, road_trophies, best_road_trophies, wins, 
                            losses, life_time_battles, max_challenge_wins, clan_id, url_encoded_pid)
                        VALUES (source.player_id, source.player_name, source.exp_lvl, source.road_trophies,
                                source.best_road_trophies, source.wins, source.losses, source.life_time_battles,
                                source.max_challenge_wins, source.clan_id, source.url_encoded_pid);                                                            
     '''
     
     with engine.raw_connection().cursor() as cursor:
        cursor.executemany(query, player_info_tuples)
        cursor.connection.commit()

# create function to upsert clan data in clans db table
def upsert_clan_info(engine, df):
    clan_info_tuples = df.values.tolist()

    query = '''
                MERGE INTO clans AS target
                USING (VALUES (?,?,?,?,?,?,?,?,?,?)) AS
                    source (clan_id, clan_name, clan_type, badge_id, clan_score, clan_war_trophies,
                            clan_location, required_trophies, members, url_encoded_cid)
                ON target.clan_id = source.clan_id
                WHEN MATCHED THEN
                    UPDATE SET
                        clan_name = source.clan_name,
                        clan_type = source.clan_type,
                        badge_id = source.badge_id,
                        clan_score = source.clan_score,
                        clan_war_trophies = source.clan_war_trophies,
                        clan_location = source.clan_location,
                        required_trophies = source.required_trophies,
                        members = source.members,
                        url_encoded_cid = source.url_encoded_cid
                WHEN NOT MATCHED THEN
                    INSERT (clan_id, clan_name, clan_type, badge_id, clan_score, clan_war_trophies,
                            clan_location, required_trophies, members, url_encoded_cid)
                        VALUES (source.clan_id, source.clan_name, source.clan_type, source.badge_id, source.clan_score,
                                source.clan_war_trophies, source.clan_location, source.required_trophies,
                                source.members, source.url_encoded_cid);    
     '''
    with engine.raw_connection().cursor() as cursor:
        cursor.executemany(query, clan_info_tuples)
        cursor.connection.commit()

# create function to upsert card data in cards db table
def upsert_card_info(engine, df):
    card_info_tuples = df.where(pd.notnull(df), 0).values.tolist()

    query = '''
                MERGE INTO cards AS target
                USING (VALUES (?, ?, ?, ?, ?)) AS
                    source (card_id, card_name, rarity, elixir_cost, evo_status)
                ON target.card_id = source.card_id
                WHEN MATCHED THEN
                    UPDATE SET
                        card_name = source.card_name,
                        rarity = source.rarity,
                        elixir_cost = source.elixir_cost,
                        evo_status = source.evo_status
                WHEN NOT MATCHED THEN
                    INSERT (card_id, card_name, rarity, elixir_cost, evo_status)
                        VALUES (source.card_id, source.card_name, source.rarity, source.elixir_cost, source.evo_status);
    '''

    with engine.raw_connection().cursor() as cursor:
        cursor.executemany(query, card_info_tuples)
        cursor.connection.commit()