import re

import sqlalchemy as db
from sqlalchemy import Table
from sqlalchemy.ext.declarative import declarative_base
import pandas as pd

from part_a import engine

# connection = engine.connect() ## TODO Don't need this
Base = declarative_base()

engine.execute('ALTER TABLE vf_data ADD COLUMN vf_match Boolean')

class Data(Base):
    __table__ = Table('vf_data', Base.metadata, autoload=True, autoload_with=engine)
   
query = db.select([
    Data.RESPID,
    Data.D101,
    Data.AGE,
    Data.D100,
    Data.SEX
    ])

match_params = engine.execute(query).fetchall()

age_refuse_code = 9999
gender_lex = {
    1: "M",
    2: "F",
    3: "NB",
    4: "O",
    5: "R"
}

match_result = []

for row in match_params:
    row = list(row)
    row[3] = gender_lex[row[3]]
    
    age_match = 2018-row[1]-int(row[2])
    
    if -1 <= age_match <= 1 or row[1] == age_refuse_code:
        if row[3] == row[4] or re.match("[^FM]", row[3]):
            row.append(True)
        else:
            row.append(False)    
    else:
        row.append(False)
    
    update = db.update(Data).values(vf_match = row[5]).where(Data.RESPID == row[0])
    engine.execute(update)
    
query = db.select([Data])
df = pd.read_sql(query,engine)

df.to_csv('./question1b_match.csv', index=False)