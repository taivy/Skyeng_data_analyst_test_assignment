import os
import pickle
from more_itertools import unique_everseen
from datetime import datetime, timedelta

import pandas as pd
from redashAPI import RedashAPIClient


def check_if_is_session(session, pages_pattern):
    pages = session.page.values
    # filter and leave only pages we interested in
    pages = list(filter(lambda p: p in pages_pattern, pages))
    # remove duplicates
    # pages = list(set(pages))
    first_pg_passed, second_pg_passed, third_pg_passed = False, False, False

    for page in pages:
        if page == pages_pattern[0]:
            first_pg_passed = True
        elif page == pages_pattern[1] and first_pg_passed:
            second_pg_passed = True
        elif page == pages_pattern[2] and second_pg_passed:
            third_pg_passed = True

    if first_pg_passed and second_pg_passed and third_pg_passed:
        return True
    return False

def get_potential_session_id(x):
    # if time difference between rows surpasses threshold
    # increase potential session id
    
    global current_session_id
    if len(x.values) == 1:
        return current_session_id
    
    diff = x.values[1] - x.values[0]
    
    if diff > time_threshold:
        current_session_id += 1

    return current_session_id


data_file_name = "data.pickle"
result_file_name = "result.json"

host = "https://app.redash.io/polina-reviakina"
api_key = "mtYH9iSJNFqwiw0MA8s7qrEfv5sNGkoNxguWXNlj"
table_name = "test.vimbox_pages"

# load data

if not os.path.exists(data_file_name):
    # query data from redash and save in pickle
    Redash = RedashAPIClient(api_key, host)
    skyeng_source_id = Redash.get("data_sources").json()[0]["id"]
    res = Redash.query_and_wait_result(int(skyeng_source_id), "SELECT * FROM " + table_name, 120)
    data = res.json()
    with open(data_file_name, "wb") as f:
        pickle.dump(data, f)
else:
    # load data form pickle if it exists
    with open(data_file_name, "rb") as f:
        data = pickle.load(f)

rows = data["query_result"]["data"]["rows"]

df = pd.DataFrame(rows)

# convert string with timestamp to float
df["happened_at"] = pd.to_numeric(pd.to_datetime(df["happened_at"])) / 10**9
df = df.sort_values("happened_at", ascending=True)

# print(df.head())

# allowed time between session's actions in seconds
time_threshold = 3600

# order of pages in session
pages_pattern = ["rooms.homework-showcase", "rooms.view.step.content", 
                 "rooms.lesson.rev.step.content"]

current_session_id = 1

# limit of rows to process
# useful for testing on a small part of the dataset
# set to -1 to disable limit
# rows_limit = 100
rows_limit = -1

i = 0
sessions_cnt = 0

result_list = []

grouped = df.groupby("user_id")

# get sessions

for user_id, group in grouped:
    window_size = 1
    group["session_id"] = group["happened_at"].\
        rolling(2, min_periods=1).apply(lambda timestamps: 
                                        get_potential_session_id(timestamps))

    potential_sessions = group.groupby("session_id")
    # user can have multiple sessions. check every
    for session_id, potential_session in potential_sessions:
        if check_if_is_session(potential_session, pages_pattern):
            session = potential_session.reset_index()
            
            session_start = datetime.fromtimestamp(session.loc[0, "happened_at"])
            last_session_visit_ix = session.shape[0]-1
            session_end_ts = session.loc[last_session_visit_ix, "happened_at"]
            session_end = datetime.fromtimestamp(session_end_ts)
            # add 1 hour because "session ends in an hour after last action"
            session_end += timedelta(hours = 1)
            
            new_row = {"user_id": user_id, "session_start": session_start,
                       "session_end": session_end}
            result_list.append(new_row)
            sessions_cnt += 1
    
    i += 1
    current_session_id += 1
    if rows_limit != -1 and i > rows_limit:
        break

users_cnt = len(df["user_id"].unique())
print("Users count:", users_cnt)

print("Sessions count:")
print(sessions_cnt)

result = pd.DataFrame(result_list)
result["session_start"] = pd.to_datetime(result["session_start"]).astype(str)
result["session_end"] = pd.to_datetime(result["session_end"]).astype(str)

print("Result:")
print(result)

with open(result_file_name, "w", encoding="utf-8") as f:
    f.write(result.to_json())
