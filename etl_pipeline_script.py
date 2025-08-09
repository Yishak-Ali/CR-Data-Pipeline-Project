# set up environment
import pandas as pd
import json
from src.helper_functions import last_n_completed_seasons, current_plus_n_seasons
from src.db_ops import (get_engine, get_existing_data, get_match_key_mapping, insert_new_rows,
                    purge_failed_players, upsert_player_info, upsert_clan_info, upsert_card_info
)
from src.api_extract import (get_season_rankings, get_player_info, get_clan_info, get_card_info, get_matches_info, 
                         get_match_card_info
)
from datetime import datetime
import logging
import os

# define required file paths
project_root = os.path.dirname(os.path.abspath(__file__))
dropped_players_path = os.path.join(project_root, 'dropped_data', 'dropped_players.json')
log_dir = os.path.join(project_root, 'logs', 'etl_logs')

# store script run logs
os.makedirs(log_dir, exist_ok=True) # makes a folder to store logs
timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S') 
log_path = os.path.join(log_dir, f'etl_log_{timestamp}.log') # new log file for each run

logging.basicConfig( #etl step results, errors, warnings are documented in a log file
    level=logging.INFO, # logging level
    format='%(asctime)s — %(levelname)s — %(message)s',
    handlers=[logging.FileHandler(log_path), logging.StreamHandler()] # sends to etl log and stdout for later cron log
)

engine = get_engine()

# function to run entire etl pipeline
def run_etl_script():
    logging.info('Starting ETL pipeline')

    # 1) populate seasons table db table - must populate before all other tables
    past_df = last_n_completed_seasons(n=3)
    current_df = current_plus_n_seasons(n=0)
    past_and_current_df = pd.concat([past_df, current_df], ignore_index=True).drop_duplicates(subset='season_id') # get current season and last 3 completed seasons

    past_and_current_ids = past_and_current_df['season_id'].tolist()
    existing_seasons = set(get_existing_data(engine, 'season_id', 'seasons')) # checking db for already stored seasons; convert to set for fast membership checking
    all_new_seasons = [s for s in past_and_current_ids if s not in existing_seasons] # initial run will insert last 3 complete sns + current,
                                                                                    # subsequent runs insert newest, current sn, if not yet added 

    if all_new_seasons:
        season_add_df = past_and_current_df[past_and_current_df['season_id'].isin(all_new_seasons)]
        logging.info(f'New seasons to fetch: {all_new_seasons}.')
        insert_new_rows(engine, season_add_df, 'seasons', 'multi') # populate seasons with new seasons data
        logging.info(f'Inserted {len(season_add_df)} new seasons.')
    else:
        logging.info('No new seasons to add')

    # 2) populate clans db table - must populate before players to respect db foreign key constraint (fk)
    all_existing_past_seasons_ids = get_existing_data(engine, 'season_id', 'seasons')[:-1] # splice to remove current season
    season_ranking_df = get_season_rankings(all_existing_past_seasons_ids)
    tracked_players = season_ranking_df['player_id'].unique().tolist() # this will pick up players who are recently terminated or banned after ranking, later filtered out
    tracked_players = [p.replace('#', '%23') for p in tracked_players]

    player_df, failed_players = get_player_info(tracked_players) # clans to ping are driven by the data that will populate players table later

    with open(dropped_players_path, 'r') as f:
        existing_dropped = json.load(f) # load current dropped list

    updated_dropped = list(set(existing_dropped + failed_players))

    with open(dropped_players_path, 'w') as f:
        json.dump(updated_dropped, f) # write failed players (banned or deleted players) into a dropped list to skip pings for in future runs
    logging.warning(f'Added {len(failed_players)} player(s) into dropped list.')

    current_membership = player_df['clan_id'].dropna().unique().tolist() # track clans top players are currently in
    existing_clans = get_existing_data(engine, 'clan_id', 'clans') # track historical clans (where top players have been in) too
    all_tracked_clans = list(set(current_membership + existing_clans))
    all_tracked_clans = [c.replace('#', '%23') for c in all_tracked_clans]

    clan_df, failed_clans = get_clan_info(all_tracked_clans)

    if not clan_df.empty:
        upsert_clan_info(engine, clan_df)
        logging.info(f'Upsert executed on {len(clan_df)} clan rows.')
    else:
        logging.warning('No clans to upsert.')

    if failed_clans:
        logging.error(f'{len(failed_clans)} clan(s) failed to fetch. Retry later: {failed_clans}')

    # 3) populate players db table - must populate before season_rankings to respect fk
    upsert_player_info(engine, player_df)
    logging.info(f'Upsert executed on {len(player_df)} player rows.')

    if failed_players:
        logging.error(f'{len(failed_players)} player(s) failed to fetch. Already flagged for dropping: {failed_players}')

    # 4) populate season_rankings db tables
    past_df = last_n_completed_seasons(n=3)
    existing_season_rankings = set(get_existing_data(engine, 'season_id', 'season_rankings'))

    past_ids = past_df['season_id'].tolist()
    completed_new_seasons = [s for s in past_ids if s not in existing_season_rankings]

    if completed_new_seasons:
        season_ranking_df = get_season_rankings(completed_new_seasons) 
        insert_new_rows(engine, season_ranking_df, 'season_rankings', 'multi')
        logging.info(f'Inserted {len(season_ranking_df)} new season rankings.')
    else:
        logging.info('No new seasons rankings to add.')

    if failed_players:
        purge_failed_players(engine, failed_players) # need to delete any historical or recently added 
                                                     # data associated with dropped players from all tables of db
        logging.warning(f'Removed all database records for {len(failed_players)} player(s): {failed_players}')

    # 5) populate cards db table
    cards_df = get_card_info() 
    upsert_card_info(engine, cards_df)
    logging.info(f'Upsert executed on {len(cards_df)} card rows.')

    # 6) script to populate matches and match_cards db tables
    existing_rank_players = get_existing_data(engine, 'player_id', 'season_rankings')
    existing_rank_players = {p.replace('#', '%23') for p in existing_rank_players}
    match_logs, failed_players, raw_json_store = get_matches_info(existing_rank_players)

    if match_logs.empty:
        logging.warning('No match data returned.')

    else:
        existing_matches = set(get_existing_data(engine, 'match_key', 'matches'))
        match_keys = match_logs['match_key'].tolist()
        new_matches = [m for m in match_keys if m not in existing_matches]

        if new_matches:
            new_matches_df = match_logs[match_logs['match_key'].isin(new_matches)]
            unique_matches = new_matches_df.drop_duplicates(subset=['player_id', 'opponent_id'])
            cnt_unique_battles = len(unique_matches)

            insert_new_rows(engine, new_matches_df, 'matches', None) # must insert before match_cards to respect fk
            logging.info(f'Inserted {len(new_matches_df)} new match views, of which {cnt_unique_battles} are unique matches.')
            
            # mapping the match_view_id identity int generated values in matches db table to match_cards_df rows
            match_key_mapping = get_match_key_mapping(engine)
            match_cards_df = get_match_card_info(raw_json_store)
            new_match_cards_df = match_cards_df[match_cards_df['match_key'].isin(new_matches)]
            new_match_cards_df = pd.merge(new_match_cards_df, match_key_mapping, on='match_key', how='inner')
            new_match_cards_df = new_match_cards_df[['match_view_id', 'player_id', 'card_id']]

            insert_new_rows(engine, new_match_cards_df, 'match_cards', None)
            logging.info(f'Inserted {len(new_match_cards_df)} new rows into match_cards, spanning {int(len(new_match_cards_df)/ 8)} match views.')
        else:
            logging.warning('No new match data.')

    if failed_players:
        logging.error(f'{len(failed_players)} player(s) failed during match fetch. Retry later: {failed_players}')

    logging.info('ETL pipeline complete')

# allows runs of etl function from terminal
if __name__ == "__main__":
    run_etl_script()