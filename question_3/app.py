

from re import S
from sqlalchemy.sql import func
import pandas as pd
from sqlalchemy.sql.selectable import subquery

from db import session
from db import Match as m

test_dict = {}
yes_count = 0
no_count = 0
min_match = 0.7
min_diff = 0.2


# --------------------
print("\n\nHello there! Would you like to plaly a game?")
print("To get started, please choose a minimum match score:\n")

min_match = float(input("minimum match: "))

print("\nGreat! One more step - please choose a minimum difference threshold:\n")

min_diff = float(input("minimum difference: "))

# --------------------


# The below query keep returning the whole table, no matter how I change id XD
subq = session.query(m.target_id, func.max(m.score).label("max_score"))\
    .group_by(m.target_id).subquery()
query = session.query(m).join(subq, m.target_id == subq.c.target_id)\
    .where(m.score == subq.c.max_score)

for instance in query:

    key = instance.source_id
    
    if not key in test_dict:
        test_dict[instance.source_id] = [(instance.target_id, instance.score)]
    else:
        test_dict[instance.source_id].append((instance.target_id, instance.score))

starting_len = len(test_dict)

for id in test_dict:
    sort_matches = sorted(test_dict[id], key=lambda x: x[1], reverse=True)
    best = sort_matches[0]
    
    if best[1] < min_match:
        test_dict[id] = (None, 0)        
    elif len(sort_matches) == 1:    
        test_dict[id] = best 
    elif best[1] - sort_matches[1][1] > min_diff:
        test_dict[id] = best
    else:
        test_dict[id] = (None, 0)   

for id in list(test_dict):
    if test_dict[id][1] == 0:
        no_count += 1
        test_dict.pop(id)
    else:
        yes_count += 1
    
print("\n-----\n")
print("Nice work!")
print(f"{yes_count} people matched those parameters!")
print(f"{no_count} people have no match...")
print(f"{(yes_count/starting_len)*100}% of entries matched those parameters")


df = pd.DataFrame.from_dict(test_dict,orient='index',columns=['target_id', 'score'])
df['source_id'] = df.index

df.to_csv('./question3_processed.csv', index=False)