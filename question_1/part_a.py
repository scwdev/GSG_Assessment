from dotenv import load_dotenv
load_dotenv()

import os, re, sqlalchemy
import pandas as pd

engine = sqlalchemy.create_engine(os.environ.get("PGRES_URI"))
df = pd.read_csv('./question1_data.csv') #keep_default_na=False

vf_list = df["VF_DATA"].tolist()
col_names = re.split("[;=]", vf_list[0])[::2]
data_dict = {}

for names in col_names:
    data_dict[names] = []

for rows in vf_list:
    r_pairs = rows.split(";")
    for pair in r_pairs:
        x = pair.split("=")
        data_dict[x[0]].append(x[1])

for cols in data_dict:
    df[cols] = data_dict[cols]
    
df = df.drop(columns="VF_DATA")
df.to_sql('vf_data', con=engine, index=False, if_exists='replace')

df.to_csv('./question1a_append.csv', index=False)

engine.execute('ALTER TABLE vf_data ADD PRIMARY KEY ("RESPID");')
