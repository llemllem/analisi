#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Nov 18 15:36:13 2021

@author: guillem
"""

import pandas as pd
from sodapy import Socrata 

# Unauthenticated client only works with public data sets. Note 'None'
# in place of application token, and no username or password:
client = Socrata("analisi.transparenciacatalunya.cat", None)
print("Format of dataset: ", type(client))

# First 2000 results, returned as JSON from API / converted to Python list of
# dictionaries by sodapy.
results = client.get("q2sg-894k", limit=150000) # 2000 is the number of vaccinated patients that we scrape from the web page

# Convert to pandas DataFrame
results_df = pd.DataFrame.from_records(results)
results_df

#%%

dfgroup4=results_df.groupby(["v_fisica", "v_psicologica","v_sexual","v_economica"]).size().reset_index(name="Freq")
total=0
for index,row in results_df.iterrows():
    total=total+1
dfgroup4["%"]=dfgroup4.Freq*100/total
dfgroup4

#Per tots els anys:
fp=results_df.groupby(["v_fisica","v_psicologica"]).size().reset_index(name="Freq")
fp["v_fisica"]=fp["v_fisica"].replace({"No":"No physical","Sí":"Physical"})
fp["v_psicologica"]=fp["v_psicologica"].replace({"No":"No psychological","Sí":"Psychological"})
fp=fp.rename(columns={"v_fisica":"origen","v_psicologica":"desti"})


fs=results_df.groupby(["v_fisica","v_sexual"]).size().reset_index(name="Freq")
fs["v_fisica"]=fs["v_fisica"].replace({"No":"No physical","Sí":"Physical"})
fs["v_sexual"]=fs["v_sexual"].replace({"No":"No sexual","Sí":"Sexual"})
fs=fs.rename(columns={"v_fisica":"origen","v_sexual":"desti"})


fe=results_df.groupby(["v_fisica","v_economica"]).size().reset_index(name="Freq")
fe["v_fisica"]=fe["v_fisica"].replace({"No":"No physical","Sí":"Physical"})
fe["v_economica"]=fe["v_economica"].replace({"No":"No economical","Sí":"Economical"})
fe=fe.rename(columns={"v_fisica":"origen","v_economica":"desti"})


ps=results_df.groupby(["v_psicologica","v_sexual"]).size().reset_index(name="Freq")
ps["v_psicologica"]=ps["v_psicologica"].replace({"No":"No psychological","Sí":"Psychological"})
ps["v_sexual"]=ps["v_sexual"].replace({"No":"No sexual","Sí":"Sexual"})
ps=ps.rename(columns={"v_psicologica":"origen","v_sexual":"desti"})


pe=results_df.groupby(["v_psicologica","v_economica"]).size().reset_index(name="Freq")
pe["v_psicologica"]=pe["v_psicologica"].replace({"No":"No psychological","Sí":"Psychological"})
pe["v_economica"]=pe["v_economica"].replace({"No":"No economical","Sí":"Economical"})
pe=pe.rename(columns={"v_psicologica":"origen","v_economica":"desti"})


se=results_df.groupby(["v_sexual","v_economica"]).size().reset_index(name="Freq")
se["v_sexual"]=se["v_sexual"].replace({"No":"No sexual","Sí":"Sexual"})
se["v_economica"]=se["v_economica"].replace({"No":"No economical","Sí":"Economical"})
se=se.rename(columns={"v_sexual":"origen","v_economica":"desti"})

merge=pd.concat([fp,fs,fe,ps,pe,se])



results2020_df=results_df[results_df["any"]=="2020"]
results2020_df

#%%

###########################################
#Pel 2020:
fp2020=results2020_df.groupby(["v_fisica","v_psicologica"]).size().reset_index(name="Freq")
fp2020["v_fisica"]=fp2020["v_fisica"].replace({"No":"No physical","Sí":"Physical"})
fp2020["v_psicologica"]=fp2020["v_psicologica"].replace({"No":"No psychological","Sí":"Psychological"})
fp2020=fp2020.rename(columns={"v_fisica":"origen","v_psicologica":"desti"})


fs2020=results_df.groupby(["v_fisica","v_sexual"]).size().reset_index(name="Freq")
fs2020["v_fisica"]=fs2020["v_fisica"].replace({"No":"No physical","Sí":"Physical"})
fs2020["v_sexual"]=fs2020["v_sexual"].replace({"No":"No sexual","Sí":"Sexual"})
fs2020=fs2020.rename(columns={"v_fisica":"origen","v_sexual":"desti"})


fe2020=results2020_df.groupby(["v_fisica","v_economica"]).size().reset_index(name="Freq")
fe2020["v_fisica"]=fe2020["v_fisica"].replace({"No":"No physical","Sí":"Physical"})
fe2020["v_economica"]=fe2020["v_economica"].replace({"No":"No economical","Sí":"Economical"})
fe2020=fe2020.rename(columns={"v_fisica":"origen","v_economica":"desti"})


ps2020=results2020_df.groupby(["v_psicologica","v_sexual"]).size().reset_index(name="Freq")
ps2020["v_psicologica"]=ps2020["v_psicologica"].replace({"No":"No psychological","Sí":"Psychological"})
ps2020["v_sexual"]=ps2020["v_sexual"].replace({"No":"No sexual","Sí":"Sexual"})
ps2020=ps2020.rename(columns={"v_psicologica":"origen","v_sexual":"desti"})


pe2020=results2020_df.groupby(["v_psicologica","v_economica"]).size().reset_index(name="Freq")
pe2020["v_psicologica"]=pe2020["v_psicologica"].replace({"No":"No psychological","Sí":"Psychological"})
pe2020["v_economica"]=pe2020["v_economica"].replace({"No":"No economical","Sí":"Economical"})
pe2020=pe2020.rename(columns={"v_psicologica":"origen","v_economica":"desti"})


se2020=results2020_df.groupby(["v_sexual","v_economica"]).size().reset_index(name="Freq")
se2020["v_sexual"]=se2020["v_sexual"].replace({"No":"No sexual","Sí":"Sexual"})
se2020["v_economica"]=se2020["v_economica"].replace({"No":"No economical","Sí":"Economical"})
se2020=se2020.rename(columns={"v_sexual":"origen","v_economica":"desti"})

merge=pd.concat([fp,fs,fe,ps,pe,se])
merge2020=pd.concat([fp2020,fs2020,fe2020,ps2020,pe2020,se2020])



import pandas as pd
import numpy as np
import warnings
import holoviews as hv
from holoviews import opts

from bokeh.plotting import figure, output_file, save

#warnings.filterwarnings("ignore")

hv.extension('bokeh')
#%opts Chord [height=600 width=600 title="Connexions" ]

#chord = hv.Chord(merge)
categories=["Physical","No physical","Psychological","No psychological","Sexual","No sexual","Economical","No economical"]
#categories = list(set(merge["origen"].unique().tolist() + merge["desti"].unique().tolist()))
categories_ds = hv.Dataset(pd.DataFrame(categories, columns=["opcio"]))

%opts Chord [height=700 width=700 title="Connexions" labels="opcio"]
%opts Chord (node_color="opcio" node_cmap="Colorblind" edge_color="origen" edge_cmap="Colorblind")

Chord=hv.Chord((merge, categories_ds))
print(categories)

#warnings.filterwarnings("ignore")

hv.extension('bokeh')
#%opts Chord [height=600 width=600 title="Connexions" ]

#chord = hv.Chord(merge)
categories=["Physical","No physical","Psychological","No psychological","Sexual","No sexual","Economical","No economical"]
#categories = list(set(merge["origen"].unique().tolist() + merge["desti"].unique().tolist()))
categories_ds = hv.Dataset(pd.DataFrame(categories, columns=["opcio"]))

%opts Chord [height=700 width=700 title="Connexions" labels="opcio"]
%opts Chord (node_color="opcio" node_cmap="Colorblind" edge_color="origen" edge_cmap="Colorblind")

Chord=hv.Chord((merge, categories_ds))
print(categories)


save(Chord)