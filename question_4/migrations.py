import db
from db import engine, session, Base

import pandas as pd

Base.metadata.drop_all(engine)
Base.metadata.create_all(engine)

q_df = pd.read_csv('./questions.csv').fillna(0)
rd_df = pd.read_csv('./respondent_data.csv').fillna(0)
rw_df = pd.read_csv('./respondent_weights.csv').fillna(0)
r_df = pd.read_csv('./responses.csv').fillna(0)
w_df = pd.read_csv('./weight_schemes.csv').fillna(0)

def to_sql(base, df):
    for index, row in df.iterrows():
        new_row = base(list(row))
        session.add(new_row)

to_sql(db.Question, q_df)
to_sql(db.Response, r_df)
to_sql(db.RespondentDatum, rd_df)

## TODO: Something weird happening where int's are being converted to floats - gotta track it down. temp fix in the Models
to_sql(db.WeightScheme, w_df)
to_sql(db.RespondentWeight, rw_df)

session.commit()
