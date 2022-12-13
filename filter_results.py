#%%
import pandas as pd
df = pd.read_csv('edges.tsv', sep='\t')
# %%
df[['src', 'dst', 'detail',]].to_csv('filtered_edges.tsv', sep='\t', index=False)
# %%
