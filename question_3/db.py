import os, sqlalchemy
from sqlalchemy import Column, Integer, Text, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

import pandas as pd

from dotenv import load_dotenv
load_dotenv()

engine = sqlalchemy.create_engine(os.environ.get("PGRES_URI"))
Session = sessionmaker(bind=engine)
session = Session()

Base = declarative_base()

class Match(Base):
    __tablename__ = "matches"
    id = Column(Integer, primary_key=True, nullable=False)
    source_id = Column(Integer)
    target_id = Column(Text)
    score = Column(Float)
    
    def __init__(self, asdf:list):
        self.source_id = asdf[0]
        self.target_id = asdf[1]
        self.score = asdf[2]
    
    def __repr__(self):
        return f"{self.id}, {self.source_id}, {self.target_id}, {self.score}"
        
data = pd.read_csv('./question3_data.csv').fillna(0)

Base.metadata.drop_all(engine)
Base.metadata.create_all(engine)

for index, row in data.iterrows():
    new_row = Match(list(row))
    session.add(new_row)
session.commit()
