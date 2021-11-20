import db
from db import session

import pandas as pd

res = db.Response
per = db.RespondentDatum
que = db.Question
rwg = db.RespondentWeight
wsc = db.WeightScheme

def gen_weighted_topline(survey_code:str):
    survey = []
    weights = {}
        
    for rw, ws in session.query(rwg, wsc).filter(rwg.project_id == survey_code,
                                                    wsc.project_id == survey_code,
                                                    wsc.final == True,
                                                    rwg._ws_id ==  wsc._id):
        weights[rw.respondent_id] = rw.weight
        
        
    for q, r in session.query(que,res).filter(que._id == res._q_id, que.project_id == survey_code).all():
        row = [q.question_id, q.text, r.text, r.response, 0]
        
        for p in session.query(per).filter(per._q_id == q._id, per.response == r.response).all():
                row[4] += weights[p.respondent_id]
                
        survey.append(row)

    df = pd.DataFrame(survey, columns=["question_id", "question_text", "answer_text", "answer_id", "freq_weighted",])

    agg_df = df.groupby(['question_id', 'answer_id']).agg({'freq_weighted': 'sum'})
    pcts = agg_df.groupby(level=0).apply(lambda x: x / float(x.sum())).reset_index(drop=True)
    
    df['percent_weighted']=pcts.freq_weighted
    
    return df

gen_weighted_topline('18058').to_csv('./question4_topline_18058.csv')
gen_weighted_topline('18390').to_csv('./question4_topline_18390.csv')