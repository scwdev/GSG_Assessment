from dotenv import load_dotenv
load_dotenv()

import os, re, sqlalchemy
import pandas as pd

engine = sqlalchemy.create_engine(os.environ.get("PGRES_URI"))

data_frame = pd.read_csv('./question1_data.csv') #keep_default_na=False

vf_list = data_frame["VF_DATA"].tolist()
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
    data_frame[cols] = data_dict[cols]
    
parsed_data = data_frame.drop(columns="VF_DATA")

parsed_data.to_sql('vf_data', con=engine, index=False, if_exists='replace')

# with engine.connect() as con: ##TODO do we need this?

engine.execute('ALTER TABLE vf_data ADD PRIMARY KEY ("RESPID");')

parsed_data.to_csv('./question1a_append.csv', index=False)
