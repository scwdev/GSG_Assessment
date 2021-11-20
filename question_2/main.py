
import pandas as pd

map_df = pd.read_csv('./question2_map.csv').fillna(False)
sa_df = pd.read_csv('./question2_survey_A.csv')
sb_df = pd.read_csv('./question2_survey_B.csv')
sc_df = pd.read_csv('./question2_survey_C.csv')

agg_df = pd.DataFrame()

sa_df['source'] = 'A'
sb_df['source'] = 'B'
sc_df['source'] = 'C'

agg_df['source'] = []


for i, col in map_df.iterrows():
    
    def rename_and_append(name:str):
        agg_df[c] = []     
        
        if col[0]:
            sa_df.rename(columns={col[0]: name},inplace=True)
        if col[1]:
            sb_df.rename(columns={col[1]: name},inplace=True)
        if col[2]:
            sc_df.rename(columns={col[2]: name},inplace=True)
        
    if col[0]: 
        if col[1]:    
            if col[2]:
                c = f"{col[0]}_ABC"
                rename_and_append(c)

            else:
                c = f"{col[0]}_AB"
                rename_and_append(c)
                
        elif col[2]:
            c = c = f"{col[0]}_AC"
            rename_and_append(c)
            
        else:
            c = f"{col[0]}_A"
            rename_and_append(c)
        
    elif col[1] and col[2]:
        c = f"{col[1]}_BC"
        rename_and_append(c)
        
    elif col[1]:
        c = f"{col[1]}_B"
        rename_and_append(c)
    
    else:
        c = f"{col[2]}_C"
        rename_and_append(c)


super_survey = pd.concat([sa_df,sb_df,sc_df])
agg_df = pd.concat([agg_df,super_survey], join='inner')


agg_df.to_csv('./question2_combined.csv')
    
 
 
 

