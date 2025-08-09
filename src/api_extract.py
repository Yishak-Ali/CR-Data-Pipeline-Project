# set up environment
from configs.config import API_KEY
from src.helper_functions import battle_time_to_sid
import requests
import pandas as pd
import time
import json
import os

# defining required file paths
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
dropped_players_path = os.path.join(project_root, 'dropped_data', 'dropped_players.json')

# define api endpoint and credentials
base_url = 'https://api.clashroyale.com/v1'
api_key = API_KEY.get('Home')
headers = {'Authorization': f'Bearer {api_key}'}

# create function to pull player data into df
def get_player_info (player_ids):
    player_details = []
    failed_ids = [] # will catch any player_ids that fail to pull
    
    for player_id in player_ids:
        try:
            response = requests.get(f'{base_url}/players/{player_id}', headers=headers)

            if response.status_code != 200:
                print(f'Failed request for player {player_id.replace("%23", "#")}: {response.status_code}')
                failed_ids.append(player_id.replace('%23', '#'))
                continue # will continue run even if it fails to fetch data for a particular sn
        
            json_data = response.json()
            if 'tag' not in json_data:
                print(f'Invalid response for player {player_id.replace("%23", "#")}: {json_data}')
                failed_ids.append(player_id.replace('%23', '#'))
                continue
        
            player_info = {'player_id': json_data['tag'], # pull fields of interest
                           'player_name': json_data['name'],
                           'exp_lvl': json_data['expLevel'],
                           'road_trophies': json_data['trophies'],
                           'best_road_trophies': json_data['bestTrophies'],
                           'wins': json_data['wins'],
                           'losses': json_data['losses'],
                           'life_time_battles': json_data['battleCount'],
                           'max_challenge_wins': json_data['challengeMaxWins'],
                           'clan_id': (json_data.get('clan') or {}).get('tag')
            }
            player_details.append(player_info)

        except Exception as e:
            print(f"Exception for player {player_id.replace('%23', '#')}: {e}")
            failed_ids.append(player_id.replace('%23', '#'))
        
        time.sleep(0.1) # stay under API limit

    # create a DataFrame from the top players data    
    players = pd.DataFrame(player_details)

    if not players.empty:
        players['url_encoded_pid'] = players['player_id'].str.replace('#', '%23')
        
    return players, failed_ids

# create function to pull season rankings from api into df
def get_season_rankings (season_ids):
    top_players = []
    with open(dropped_players_path, 'r') as f:
        dropped_players = json.load(f) # load in dynamic list of known deleted or banned players to filter out

    for season in season_ids:
        response = requests.get(f'{base_url}/locations/global/pathoflegend/{season}/rankings/players?limit=100', headers=headers)
        if response.status_code != 200:
            print(f'Failed to fetch data for {season}.')
            continue 

        for player in response.json().get('items', []):
            if player['tag'] not in dropped_players: # apply filter                                                                                                    
                rank_info = {'player_id': player['tag'], 
                            'season_id': season,
                            'rank': player['rank'],
                            'rating': player['eloRating']
                }
                top_players.append(rank_info)
                
    season_rankings = pd.DataFrame(top_players)
    
    return season_rankings

# create function to pull clan data into df
def get_clan_info (clan_ids):
    clan_details = []
    failed_clans = []
    for clan_id in clan_ids:
        try:
            response = requests.get(f'{base_url}/clans/{clan_id}', headers=headers)
            if response.status_code != 200:
                    print(f'Failed request for clan {clan_id.replace("%23", "#")}: {response.status_code}')
                    failed_clans.append(clan_id.replace("%23", "#"))
                    continue
            
            json_data = response.json()
            if 'tag' not in json_data:
                print(f'Invalid response for clan {clan_id.replace("23", "#")}: {json_data}')
                failed_clans.append(clan_id.replace('23', '#'))
                continue

            clan_info = {'clan_id': json_data['tag'],
                        'clan_name': json_data['name'],
                        'clan_type': json_data['type'],
                        'badge_id': json_data['badgeId'],
                        'clan_score': json_data['clanScore'],
                        'clan_war_trophies': json_data['clanWarTrophies'],
                        'clan_location': json_data.get('location').get('name'),
                        'required_trophies': json_data['requiredTrophies'],
                        'members': json_data['members']
            }
            clan_details.append(clan_info)
            
        except Exception as e:
            print(f"Exception for clan {clan_id.replace('%23', '#')}: {e}")
            failed_clans.append(clan_id.replace('%23','#'))

        time.sleep(0.1)
        
    clans = pd.DataFrame(clan_details)
    clans['url_encoded_cid'] = clans['clan_id'].str.replace('#', '%23')
    clans['badge_id'] = clans['badge_id'].astype(str)  # ensure badge_id is string type
    
    return clans, failed_clans

# create function to pull card data into df
def get_card_info():
    card_info = []
    response = requests.get(f'{base_url}/cards', headers=headers)
    if response.status_code != 200:
        print(f'Failed to fetch card data: {response.status_code}')
        return pd.DataFrame()  # return empty DataFrame if request fails
    
    json_data = response.json().get('items', [])
    for card in json_data:
        card_details = {'card_id': card['id'],
                        'card_name': card['name'],
                        'rarity': card['rarity'],
                        'elixir_cost': card.get('elixirCost'), # elixir cost may not be present for all cards
                        'evo_status':  card.get('maxEvolutionLevel', 0) # evolutions have no level thus maxEvolutionLevel = 1 denotes evolution present
        }
        card_info.append(card_details)
        
    cards = pd.DataFrame(card_info)

    # table cleaning
    cards['evo_status'] = cards['evo_status'].astype(bool)
    cards['card_id'] = cards['card_id'].astype(str)
    cards['elixir_cost'] = cards['elixir_cost'].astype('Int64')
    cards['elixir_cost'] = cards['elixir_cost'].mask(cards['elixir_cost'].isnull(), None)
    
    return cards

# create function to pull matches data into df
def get_matches_info(player_ids): 
    matches_info = []
    failed_match_players = []
    raw_json_store = {}

    for player in player_ids:
        try:
            response = requests.get(f'{base_url}/players/{player}/battlelog', headers=headers)
            if response.status_code != 200:
                print(f'failed request for {player.replace("%23", "#")}: {response.status_code}')
                failed_match_players.append(player.replace('%23', '#'))
                continue

            json_data = response.json()
            raw_json_store[player] = json_data # want to also store json_data for later match_cards pull

            for match in json_data:
                team = match.get('team')[0]
                opp = match.get('opponent')[0]

                matches_details = {
                    'battle_time': match.get('battleTime'),
                    'game_mode': match.get('type'),
                    'league': match.get('leagueNumber'),
                    'player_id': team.get('tag'),
                    'opponent_id': opp.get('tag'),
                    'current_global_rank': team.get('globalRank'),
                    'starting_rating': team.get('startingTrophies'),
                    'rating_change': team.get('trophyChange'),
                    'crowns': team.get('crowns'),
                    'opp_crowns': opp.get('crowns'),
                    'king_tower_hp': team.get('kingTowerHitPoints'),
                    'princess_towers_hp': team.get('princessTowersHitPoints'),
                    'elixir_leaked': team.get('elixirLeaked')
                }

                matches_info.append(matches_details)

            time.sleep(0.1)  # stay under API rate limit

        except Exception as e:
            print(f'exception for {player.replace("%23", "#")}: {e}')
            failed_match_players.append(player.replace('%23', '#'))

    matches = pd.DataFrame(matches_info) # important note: each row is not necessarily a distinct match because two top players can play in the same match
                                         # each row is instead a distinct match-player combo; thus it is possible for the same match to be represented twice, once for each perspective
    if matches.empty:
        return matches, failed_match_players, raw_json_store

    # table cleaning
    matches['is_win'] = matches['crowns'] > matches['opp_crowns']
    matches['battle_time'] = pd.to_datetime(matches['battle_time'], format='%Y%m%dT%H%M%S.%fZ',
                                            utc=True, errors='coerce')
    matches['season_id'] = matches['battle_time'].apply(battle_time_to_sid)
    matches['match_key'] = (matches['battle_time'].astype(str) + '_' + matches['player_id'].astype(str)) # this col will be the checked col during ingestion
    matches = matches[matches['game_mode'] == 'pathOfLegend'].copy()
    matches['princess_tower1_hp'] = matches['princess_towers_hp'].apply(lambda x: x[0] if isinstance(x, list) and len(x) > 0 else 0)
    matches['princess_tower2_hp'] = matches['princess_towers_hp'].apply(lambda x: x[1] if isinstance(x, list) and len(x) > 1 else 0)

    matches = matches[[
        'match_key','battle_time', 'is_win', 'league', 'player_id', 'opponent_id', 'season_id',
        'current_global_rank', 'starting_rating', 'rating_change', 'crowns','opp_crowns',
        'king_tower_hp', 'princess_tower1_hp', 'princess_tower2_hp', 'elixir_leaked',
    ]]

    return matches, failed_match_players, raw_json_store 

# create function to pull match cards (deck) data from stored json of battlelogs and into df
def get_match_card_info(raw_json):
    match_card_info = []
    for player in raw_json:
        for match in raw_json[player]:
            for card in match['team'][0]['cards']:
                match_card_details = {'game_mode': match['type'],
                                      'battle_time': match['battleTime'],
                                      'player_id': match['team'][0]['tag'],
                                      'card_id': card['id']
                }
                match_card_info.append(match_card_details)
    match_cards = pd.DataFrame(match_card_info)

    if match_cards.empty:
        return match_cards
    
    # table cleaning
    match_cards['battle_time'] = pd.to_datetime(match_cards['battle_time'], format='%Y%m%dT%H%M%S.%fZ', utc=True)
    match_cards['match_key'] = (match_cards['battle_time'].astype(str) + '_' + match_cards['player_id'].astype(str))
    match_cards['card_id'] = match_cards['card_id'].astype(str)
    match_cards = match_cards[match_cards['game_mode'] == 'pathOfLegend'].copy()
    match_cards.drop(columns=['game_mode', 'battle_time'], inplace=True)

    return match_cards # like matches, two perspectives of a match are possible