#%%
import pandas as pd
from fuzzywuzzy import fuzz
from tqdm import tqdm
# %%
df = pd.read_csv('edges.tsv', sep="\t")
# %%
df = df[['src','dst','detail']]

#%%
adf = pd.read_csv('annot.txt', sep=', ', engine='python')
#%%
def annot(df, q, c):
    df[c] = [0]*len(df) #create new column
    for idx, row in tqdm(df.iterrows(),total=len(df)):
        key = row.detail
        query = q
        if query.__contains__(' _ '):
             val = fuzz.token_set_ratio(key, query)
        else:
            val = fuzz.partial_ratio(key, query)
        df.at[idx,c] = val

    return df


#%%
for i, r in adf.iterrows():
    q = r.q
    c = r.c
    print(c)
    print(q)
    df = annot(df, q, c)

# %%
df.to_csv('edges_annot.tsv', sep='\t', index=False)
# %%
