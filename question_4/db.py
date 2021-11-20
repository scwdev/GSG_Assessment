import sqlalchemy
from sqlalchemy import Column, Integer, Float, String, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship

from sqlalchemy.sql.schema import ForeignKey
from sqlalchemy.sql.sqltypes import VARCHAR

from utilities import bool_parse

def bool_parse(s:str):
    if s == "f":
        return False
    if s == "t":
        return True

engine = sqlalchemy.create_engine('postgresql://scwd:ofekdekel@localhost:5432/vf_data')
Session = sessionmaker(bind=engine)
session = Session()

Base = declarative_base()

class Question(Base):
    __tablename__ = "questions"
    
    _id = Column(String, primary_key=True, nullable=False)
    project_id = Column(VARCHAR(64))
    question_id = Column(VARCHAR(1024))
    nummult = Column(Integer)
    row_order = Column(Integer)
    filter = Column(VARCHAR(4096))
    short_name = Column(VARCHAR(1024))
    suppress = Column(Boolean)
    text = Column(VARCHAR(12288))
    type = Column(VARCHAR(1024))
    ## TODO double check this
    responses = relationship('Response') ## One-to-Many
    respondent_data = relationship('RespondentDatum') ## One-to-Many
    
    def __init__(self, l:list):
        self.project_id = l[0]
        self.question_id = l[1]
        self._id = f"{self.project_id}_{self.question_id}"
        self.filter = l[2]
        self.nummult = l[3]
        self.row_order = l[4]
        self.short_name = l[5]
        self.suppress = bool_parse(l[6]) 
        self.text = l[7]
        self.type = l[8]
                

class Response(Base):
    __tablename__ = "responses"
    
    _id = Column(Integer, primary_key=True, nullable=False)
    _q_id = Column(String, ForeignKey('questions._id')) ## Many-to-One
    project_id = Column(VARCHAR(64))
    question_id = Column(VARCHAR(1024))
    response = Column(VARCHAR(1024))
    response_index = Column(Integer)
    suppress = Column(Boolean)
    text = Column(VARCHAR(4096))
    
    def __init__(self, l:list):
        self.project_id = l[0]
        self.question_id = l[1]
        self._q_id = f"{self.project_id}_{self.question_id}"
        self.response = l[2]
        self.response_index = l[3]
        self.suppress = bool_parse(l[4])
        self.text = l[5]
        

class WeightScheme(Base):
    __tablename__ = "weight_schemes"
    
    _id = Column(String, primary_key=True, nullable=False)
    project_id = Column(VARCHAR(64))
    weight_scheme_id = Column(Integer)
    name = Column(VARCHAR(1024))
    final = Column(Boolean)
    respondent_weights = relationship("RespondentWeight")
    
    def __init__(self, l:list):
        self.project_id = int(l[0])
        self.weight_scheme_id = int(l[1])
        self._id = f"{self.project_id}_{self.weight_scheme_id}"
        self.name = l[2]
        self.final = bool_parse(l[3])
            

class RespondentDatum(Base):
    __tablename__ = "respondent_data"
    
    _id = Column(Integer, primary_key=True, nullable=False)
    _q_id = Column(String, ForeignKey('questions._id')) ## One-to-Many
    respondent_id = Column(Integer)
    project_id = Column(VARCHAR(64))
    question_id = Column(VARCHAR(1024))
    response = Column(VARCHAR(1024))
    
    def __init__(self, l:list):
        self.project_id = l[0]
        self.question_id = l[1]
        self._q_id = f"{self.project_id}_{self.question_id}"
        self.respondent_id = l[2]
        self.response = l[3]        
        

class RespondentWeight(Base):
    __tablename__ = "respondent_weights"
    
    _id = Column(Integer, primary_key=True, nullable=False)
    _ws_id = Column(String, ForeignKey('weight_schemes._id'))

    project_id = Column(VARCHAR(64))
    weight_scheme_id = Column(Integer)
    respondent_id = Column(Integer)
    weight = Column(Float) ## Todo read about 'Double' SQL column
    
    def __init__(self, l:list):
        self.project_id = int(l[0])
        self.weight_scheme_id = int(l[1])
        self._ws_id = f"{self.project_id}_{self.weight_scheme_id}"
        self.respondent_id = l[2]
        self.weight = l[3]
