#%%
# basic imports
from itertools import combinations
import pandas as pd
import pyodbc
from tqdm import tqdm
import sqlalchemy as sa
import urllib
from sqlalchemy import create_engine, event
from sqlalchemy.engine.url import URL
#%%
# reading and categorizing datasets
source_df = pd.read_csv("drugs_therapy_actions_v2.tsv", sep="\t")
source_valid_drugbank = source_df[~source_df['drugbank_id'].isna()]
source_invalid_drugbank = source_df[source_df['drugbank_id'].isna()]
drug_names = source_invalid_drugbank.drug_name.to_list()
# %%
# connections to mssql
params = urllib.parse.quote_plus('Driver={ODBC Driver 17 for SQL Server};'
                      'Server=DELL-7415-R7;'
                      'Database=InteractDb;'
                      'Trusted_Connection=yes;')

engine = sa.create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))


#%%
results = []
for name in tqdm(drug_names):
    if len(name)>4:
        qry = "SELECT * FROM Drugs WHERE LOWER(name) LIKE \'%" + str(name) + "%\'"
    else:
        qry = "SELECT * FROM Drugs WHERE LOWER(name) = \'" + str(name) + "\'"
    df = pd.read_sql(qry, engine)
    df['source_qry'] = [name] * len(df)
    results.append(df)

result_df = pd.concat(results)
# %%
drug_ids = source_valid_drugbank.drugbank_id.to_list()
results = []
for id in tqdm(drug_ids):
    qry = "SELECT * FROM Drugs WHERE ID = \'" + str(id) +"\'"
    df = pd.read_sql(qry, engine)
    df['source_qry'] = [id] * len(df)
    results.append(df)

result_df_2 = pd.concat(results)

# %%
nodes = pd.concat([result_df_2, result_df])
nodes.to_csv("nodes_mapped_from_db.tsv", sep="\t")

# %%
possible_edges = []
for c in combinations(nodes.ID.to_list(),2):
    possible_edges.append(c)
#%%
edges = []
for u,v in tqdm(possible_edges):
    qry = "SELECT * FROM DrugDrug WHERE (src = \'" + u + "\' AND dst = \'" + v + "\') OR (src = \'" + v + "\' AND dst = \'" + u + "\')"
    df = pd.read_sql(qry, engine)
    edges.append(df)

edges_df = pd.concat(edges)

# %%
