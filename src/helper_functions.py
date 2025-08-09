# set up environment
from datetime import datetime, timedelta
from pytz import UTC
import pandas as pd

# create function to generate season id and date range for last n completed sns
def last_n_completed_seasons(n=3, ref_date=None):
    ref_date = ref_date or datetime.utcnow()

    # find the first monday of each month (day of season reset) going back
    months_back = n + 2
    first_mondays = []
    for i in range(months_back):
        m = (ref_date.month - i - 1) % 12 + 1  
        y = ref_date.year if (ref_date.month - i - 1) >= 0 else ref_date.year - 1 # handles cases of backtracking into previous year

        # checks first week of the month to find the first monday
        for d in range(1, 8):
            date = datetime(year=y, month=m, day=d)
            if datetime.weekday(date) == 0: # Monday is denoted as 0
                first_mondays.append(date)
                break
    first_mondays = sorted(first_mondays)

    # build seasons (start date, end date)
    seasons = []
    for i in range(len(first_mondays) - 1):
        start = first_mondays[i] + timedelta(hours=9) # specific start time for seasons
        end = first_mondays[i+1] + timedelta(hours=9) - timedelta(seconds=1)

        if end < ref_date:
            season_id = start.strftime('%Y-%m') # extract season_id from season start datetime
            seasons.append({'season_id': season_id,
                            'sn_start_date': start.replace(tzinfo=UTC),
                            'sn_end_date': end.replace(tzinfo=UTC)
            })
    return pd.DataFrame(seasons)

# create function to generate current and future season_id and date range
def current_plus_n_seasons(n=3, ref_date=None):
    ref_date = ref_date or datetime.utcnow()
    months_forward = n + 4
    
    first_mondays = []
    for i in range(-2, months_forward):
        m = (ref_date.month + i - 1) % 12 + 1
        y = ref_date.year + ((ref_date.month + i - 1) // 12)

        for d in range(1, 8):
            date = datetime(year=y, month=m, day=d)
            if datetime.weekday(date) == 0:
                first_mondays.append(date)
                break
    first_mondays = sorted(set(first_mondays))

    # build seasons as list of dicts with each dict representing a season row
    seasons = []
    for i in range(len(first_mondays) - 1):
        start = first_mondays[i] + timedelta(hours=9)
        end = first_mondays[i+1] + timedelta(hours=9) - timedelta(seconds=1)
        if start <= ref_date <= end: # add current season
            season_id = start.strftime('%Y-%m')
            seasons.append({'season_id': season_id,
                            'sn_start_date': start.replace(tzinfo=UTC),
                            'sn_end_date': end.replace(tzinfo=UTC)
            })
        elif start > ref_date and len(seasons) < n + 1:  # add up to n future
            seasons.append({'season_id': start.strftime('%Y-%m'),
                            'sn_start_date': start.replace(tzinfo=UTC),
                            'sn_end_date': end.replace(tzinfo=UTC)
            })   
    # return as df
    return pd.DataFrame(seasons)

# create function to find season_id based on battle_time
def battle_time_to_sid(battle_time, past_n=3, future_n=3):
    battle_time = pd.to_datetime(battle_time, utc =True)
    past_df = last_n_completed_seasons(n=past_n)
    future_df = current_plus_n_seasons(n=future_n)
    all_seasons_df = pd.concat([past_df, future_df], ignore_index=True).drop_duplicates(subset='season_id')
    
    for _, season in all_seasons_df.iterrows():
        if season['sn_start_date'] <= battle_time <= season['sn_end_date']:
            return season['season_id']
    return None