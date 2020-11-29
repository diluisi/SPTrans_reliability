#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Nov 16 01:11:28 2020

@author: diluisi
"""

# Query
import pandas as pd
import dateutil
from pymongo import MongoClient
import numpy as np
import datetime as dt
from scipy.stats import variation
from dask.distributed import Client

def bus_process(bus):
    #query
    dateb = dateutil.parser.parse("2017" + "-" + "1" + "-" + "1" + "T04:00:00")
    datee = dateutil.parser.parse("2017" + "-" + "9" + "-" + "30" + "T22:00:00")
    #buslist = ["702C-10-0"]
    q = {'trip_id': {"$in": [bus]}, "aproxlinkstart": {"$gte": dateb, "$lt": datee}}
    print(bus)
    #q = {'trip_id': {"$in": buslist}}
    conn = MongoClient('172.17.163.239')
    select = pd.DataFrame.from_records(conn['LinkTT2']['ltts'].find(q))            
    conn.close()
    hd = Headway(select)
    hd.beautifier()
    hd.df.to_csv('/home/diego/headway/headway_{}.csv'.format(bus))

    links_unicos = hd.df.link_index.unique() # links unicos 
    viagens_unicas = hd.df[hd.df['link_index']==np.max(links_unicos)].travel_id.unique() # lista de viagens unicas que tem a último link
    ltt_dict = {} # dicionário de ltt por link e por viagem
    # itera nos links
    for link in links_unicos:
        ltt_list = []
        # itera nas viagens
        for viagem in viagens_unicas:
            if link in hd.df[(hd.df['travel_id'] == viagem)].link_index.to_list():
                ltt_list.append((hd.df[(hd.df['travel_id'] == viagem) & (hd.df['link_index']==np.max(links_unicos))].ltt_acc.values[0] - hd.df[(hd.df['travel_id'] == viagem) & (hd.df['link_index']==link)].ltt_acc.values[0]))
            else:
                ltt_list.append(0)
        ltt_dict[str(link)] = ltt_list
    df_ltt = pd.DataFrame.from_dict(ltt_dict)
    df_ltt['travel_id'] = viagens_unicas
    df_ltt.to_csv('/home/diego/ltt/ltt_{}.csv'.format(bus))
    
    lista_de_pontos = hd.df.link_index.unique()
    pt_25_perc = int(np.rint(np.quantile(lista_de_pontos,.25)))
    pt_50_perc = int(np.rint(np.quantile(lista_de_pontos,.50)))
    pt_75_perc = int(np.rint(np.quantile(lista_de_pontos,.75)))
    
    h = [6,7,8,9,10,11,12,13,14,15,16,17,18,19,20]
    w = [0,1,2,3,4]
    months = [3,4,5,6]
    route=[]
    #headway
    mean_geral =[]
    mean_25_perc =[]
    mean_50_perc =[]
    mean_75_perc =[]
    median_geral =[]
    median_25_perc =[]
    median_50_perc =[]
    median_75_perc =[]
    std_geral =[]
    std_25_perc =[]
    std_50_perc =[]
    std_75_perc =[]
    perc95_geral =[]
    perc95_25_perc =[]
    perc95_50_perc =[]
    perc95_75_perc =[]
    cv_geral =[]
    cv_25_perc =[]
    cv_50_perc =[]
    cv_75_perc =[]
    trips_geral =[]
    trips_25_perc =[]
    trips_50_perc =[]
    trips_75_perc =[]
    dt = []
    # lists
    ltt_mean = []
    rbt_mean = []
    ltt_mean_25_perc = []
    rbt_mean_25_perc = []
    ltt_mean_50_perc = []
    rbt_mean_50_perc = []
    ltt_mean_75_perc = []
    rbt_mean_75_perc = []
    ltt_median = []
    rbt_median = []
    ltt_median_25_perc = []
    rbt_median_25_perc = []
    ltt_median_50_perc = []
    rbt_median_50_perc = []
    ltt_median_75_perc = []
    rbt_median_75_perc = []
    ltt_cv = []
    ltt_cv_25_perc = []
    ltt_cv_50_perc = []
    ltt_cv_75_perc = []
    ltt_std = []
    ltt_std_25_perc = []
    ltt_std_50_perc = []
    ltt_std_75_perc = []
    
    # Headway
    hd.df.sort_values(['date'], inplace=True)
    dates = hd.df[hd.df['month'].isin(months) & (hd.df['weekday'].isin(w))].date.unique()
    
    for i in dates:
        # listas de viagen para cada bloco de horário
        travel_h = hd.df[(hd.df['headway']!=0) & (hd.df['hour'].isin(h)) & (hd.df['weekday'].isin(w)) & (hd.df['month'].isin(months)) & (hd.df['date']==i)].travel_id.unique()
        
        # média da distribuição dos headways
        mean_geral.append(hd.df[(hd.df['link_index']==1) & (hd.df['headway']!=0) & (hd.df['hour'].isin(h)) & (hd.df['weekday'].isin(w)) & (hd.df['month'].isin(months)) & (hd.df['date']==i)].headway.mean())
        mean_25_perc.append(hd.df[(hd.df['link_index']==pt_25_perc) & (hd.df['headway']!=0) & (hd.df['hour'].isin(h)) & (hd.df['weekday'].isin(w)) & (hd.df['month'].isin(months)) & (hd.df['date']==i)].headway.mean())
        mean_50_perc.append(hd.df[(hd.df['link_index']==pt_50_perc) & (hd.df['headway']!=0) & (hd.df['hour'].isin(h)) & (hd.df['weekday'].isin(w)) & (hd.df['month'].isin(months)) & (hd.df['date']==i)].headway.mean())
        mean_75_perc.append(hd.df[(hd.df['link_index']==pt_75_perc) & (hd.df['headway']!=0) & (hd.df['hour'].isin(h)) & (hd.df['weekday'].isin(w)) & (hd.df['month'].isin(months)) & (hd.df['date']==i)].headway.mean())

        
        # mediana da distribuição dos headways
        median_geral.append(hd.df[(hd.df['link_index']==1) & (hd.df['headway']!=0) & (hd.df['hour'].isin(h)) & (hd.df['weekday'].isin(w)) & (hd.df['month'].isin(months)) & (hd.df['date']==i)].headway.median())
        median_25_perc.append(hd.df[(hd.df['link_index']==pt_25_perc) & (hd.df['headway']!=0) & (hd.df['hour'].isin(h)) & (hd.df['weekday'].isin(w)) & (hd.df['month'].isin(months)) & (hd.df['date']==i)].headway.median())
        median_50_perc.append(hd.df[(hd.df['link_index']==pt_50_perc) & (hd.df['headway']!=0) & (hd.df['hour'].isin(h)) & (hd.df['weekday'].isin(w)) & (hd.df['month'].isin(months)) & (hd.df['date']==i)].headway.median())
        median_75_perc.append(hd.df[(hd.df['link_index']==pt_75_perc) & (hd.df['headway']!=0) & (hd.df['hour'].isin(h)) & (hd.df['weekday'].isin(w)) & (hd.df['month'].isin(months)) & (hd.df['date']==i)].headway.median())

        # std da distribuição dos headways
        std_geral.append(hd.df[(hd.df['link_index']==1) & (hd.df['headway']!=0) & (hd.df['hour'].isin(h)) & (hd.df['weekday'].isin(w)) & (hd.df['month'].isin(months)) & (hd.df['date']==i)].headway.std())
        std_25_perc.append(hd.df[(hd.df['link_index']==pt_25_perc) & (hd.df['headway']!=0) & (hd.df['hour'].isin(h)) & (hd.df['weekday'].isin(w)) & (hd.df['month'].isin(months)) & (hd.df['date']==i)].headway.std())
        std_50_perc.append(hd.df[(hd.df['link_index']==pt_50_perc) & (hd.df['headway']!=0) & (hd.df['hour'].isin(h)) & (hd.df['weekday'].isin(w)) & (hd.df['month'].isin(months)) & (hd.df['date']==i)].headway.std())
        std_75_perc.append(hd.df[(hd.df['link_index']==pt_75_perc) & (hd.df['headway']!=0) & (hd.df['hour'].isin(h)) & (hd.df['weekday'].isin(w)) & (hd.df['month'].isin(months)) & (hd.df['date']==i)].headway.std())
        

        # 95 percentil da distribuição dos headways
        perc95_geral.append(hd.df[(hd.df['link_index']==1) & (hd.df['headway']!=0) & (hd.df['hour'].isin(h)) & (hd.df['weekday'].isin(w)) & (hd.df['month'].isin(months)) & (hd.df['date']==i)].headway.quantile(0.95))
        perc95_25_perc.append(hd.df[(hd.df['link_index']==pt_25_perc) & (hd.df['headway']!=0) & (hd.df['hour'].isin(h)) & (hd.df['weekday'].isin(w)) & (hd.df['month'].isin(months)) & (hd.df['date']==i)].headway.quantile(0.95))
        perc95_50_perc.append(hd.df[(hd.df['link_index']==pt_50_perc) & (hd.df['headway']!=0) & (hd.df['hour'].isin(h)) & (hd.df['weekday'].isin(w)) & (hd.df['month'].isin(months)) & (hd.df['date']==i)].headway.quantile(0.95))
        perc95_75_perc.append(hd.df[(hd.df['link_index']==pt_75_perc) & (hd.df['headway']!=0) & (hd.df['hour'].isin(h)) & (hd.df['weekday'].isin(w)) & (hd.df['month'].isin(months)) & (hd.df['date']==i)].headway.quantile(0.95))

        # coefficient of variation
        cv_geral.append(hd.df[(hd.df['link_index']==1) & (hd.df['headway']!=0) & (hd.df['hour'].isin(h)) & (hd.df['weekday'].isin(w)) & (hd.df['month'].isin(months)) & (hd.df['date']==i)].headway.std() / hd.df[(hd.df['link_index']==1) & (hd.df['headway']!=0) & (hd.df['hour'].isin(h)) & (hd.df['weekday'].isin(w)) & (hd.df['month'].isin(months)) & (hd.df['date']==i)].headway.mean())
        cv_25_perc.append(hd.df[(hd.df['link_index']==pt_25_perc) & (hd.df['headway']!=0) & (hd.df['hour'].isin(h)) & (hd.df['weekday'].isin(w)) & (hd.df['month'].isin(months)) & (hd.df['date']==i)].headway.std() / hd.df[(hd.df['link_index']==pt_25_perc) & (hd.df['headway']!=0) & (hd.df['hour'].isin(h)) & (hd.df['weekday'].isin(w)) & (hd.df['month'].isin(months)) & (hd.df['date']==i)].headway.mean())
        cv_50_perc.append(hd.df[(hd.df['link_index']==pt_50_perc) & (hd.df['headway']!=0) & (hd.df['hour'].isin(h)) & (hd.df['weekday'].isin(w)) & (hd.df['month'].isin(months)) & (hd.df['date']==i)].headway.std() / hd.df[(hd.df['link_index']==pt_50_perc) & (hd.df['headway']!=0) & (hd.df['hour'].isin(h)) & (hd.df['weekday'].isin(w)) & (hd.df['month'].isin(months)) & (hd.df['date']==i)].headway.mean())
        cv_75_perc.append(hd.df[(hd.df['link_index']==pt_75_perc) & (hd.df['headway']!=0) & (hd.df['hour'].isin(h)) & (hd.df['weekday'].isin(w)) & (hd.df['month'].isin(months)) & (hd.df['date']==i)].headway.std() / hd.df[(hd.df['link_index']==pt_75_perc) & (hd.df['headway']!=0) & (hd.df['hour'].isin(h)) & (hd.df['weekday'].isin(w)) & (hd.df['month'].isin(months)) & (hd.df['date']==i)].headway.mean())     

        # number of trips
        trips_geral.append(hd.df[(hd.df['link_index']==1) & (hd.df['headway']!=0) & (hd.df['hour'].isin(h)) & (hd.df['weekday'].isin(w)) & (hd.df['month'].isin(months)) & (hd.df['date']==i)].headway.count())
        trips_25_perc.append(hd.df[(hd.df['link_index']==pt_25_perc) & (hd.df['headway']!=0) & (hd.df['hour'].isin(h)) & (hd.df['weekday'].isin(w)) & (hd.df['month'].isin(months)) & (hd.df['date']==i)].headway.count())
        trips_50_perc.append(hd.df[(hd.df['link_index']==pt_50_perc) & (hd.df['headway']!=0) & (hd.df['hour'].isin(h)) & (hd.df['weekday'].isin(w)) & (hd.df['month'].isin(months)) & (hd.df['date']==i)].headway.count())
        trips_75_perc.append(hd.df[(hd.df['link_index']==pt_75_perc) & (hd.df['headway']!=0) & (hd.df['hour'].isin(h)) & (hd.df['weekday'].isin(w)) & (hd.df['month'].isin(months)) & (hd.df['date']==i)].headway.count())
        
        # mean
        ltt_mean.append(df_ltt[df_ltt['travel_id'].isin(travel_h)]['1'].mean())
        rbt_mean.append((df_ltt[df_ltt['travel_id'].isin(travel_h)]['1'].quantile(0.95)-df_ltt[df_ltt['travel_id'].isin(travel_h)]['1'].mean()))
        ltt_mean_25_perc.append(df_ltt[df_ltt['travel_id'].isin(travel_h)][str(pt_25_perc)].mean())
        rbt_mean_25_perc.append((df_ltt[df_ltt['travel_id'].isin(travel_h)][str(pt_25_perc)].quantile(0.95)-df_ltt[df_ltt['travel_id'].isin(travel_h)][str(pt_25_perc)].mean()))
        ltt_mean_50_perc.append(df_ltt[df_ltt['travel_id'].isin(travel_h)][str(pt_50_perc)].mean())
        rbt_mean_50_perc.append((df_ltt[df_ltt['travel_id'].isin(travel_h)][str(pt_50_perc)].quantile(0.95)-df_ltt[df_ltt['travel_id'].isin(travel_h)][str(pt_50_perc)].mean()))
        ltt_mean_75_perc.append(df_ltt[df_ltt['travel_id'].isin(travel_h)][str(pt_75_perc)].mean())
        rbt_mean_75_perc.append((df_ltt[df_ltt['travel_id'].isin(travel_h)][str(pt_75_perc)].quantile(0.95)-df_ltt[df_ltt['travel_id'].isin(travel_h)][str(pt_75_perc)].mean()))

        # median
        ltt_median.append(df_ltt[df_ltt['travel_id'].isin(travel_h)]['1'].median())
        rbt_median.append((df_ltt[df_ltt['travel_id'].isin(travel_h)]['1'].quantile(0.95)-df_ltt[df_ltt['travel_id'].isin(travel_h)]['1'].median()))
        ltt_median_25_perc.append(df_ltt[df_ltt['travel_id'].isin(travel_h)][str(pt_25_perc)].median())
        rbt_median_25_perc.append((df_ltt[df_ltt['travel_id'].isin(travel_h)][str(pt_25_perc)].quantile(0.95)-df_ltt[df_ltt['travel_id'].isin(travel_h)][str(pt_25_perc)].median()))
        ltt_median_50_perc.append(df_ltt[df_ltt['travel_id'].isin(travel_h)][str(pt_50_perc)].median())
        rbt_median_50_perc.append((df_ltt[df_ltt['travel_id'].isin(travel_h)][str(pt_50_perc)].quantile(0.95)-df_ltt[df_ltt['travel_id'].isin(travel_h)][str(pt_50_perc)].median()))
        ltt_median_75_perc.append(df_ltt[df_ltt['travel_id'].isin(travel_h)][str(pt_75_perc)].median())
        rbt_median_75_perc.append((df_ltt[df_ltt['travel_id'].isin(travel_h)][str(pt_75_perc)].quantile(0.95)-df_ltt[df_ltt['travel_id'].isin(travel_h)][str(pt_75_perc)].median()))
        
        # coefficient of variation
        ltt_cv.append(df_ltt[df_ltt['travel_id'].isin(travel_h)].agg(variation)['1'])
        ltt_cv_25_perc.append(df_ltt[df_ltt['travel_id'].isin(travel_h)].agg(variation)[str(pt_25_perc)])
        ltt_cv_50_perc.append(df_ltt[df_ltt['travel_id'].isin(travel_h)].agg(variation)[str(pt_50_perc)])
        ltt_cv_75_perc.append(df_ltt[df_ltt['travel_id'].isin(travel_h)].agg(variation)[str(pt_75_perc)])
        
        # standard deviation
        ltt_std.append(df_ltt[df_ltt['travel_id'].isin(travel_h)].std()['1'])
        ltt_std_25_perc.append(df_ltt[df_ltt['travel_id'].isin(travel_h)].std()[str(pt_25_perc)])
        ltt_std_50_perc.append(df_ltt[df_ltt['travel_id'].isin(travel_h)].std()[str(pt_50_perc)])
        ltt_std_75_perc.append(df_ltt[df_ltt['travel_id'].isin(travel_h)].std()[str(pt_75_perc)])
        
        #route
        route.append(bus)
        
        # dates
        dt.append(i)
        
    df_final = pd.DataFrame({'linhas':route,
                             'date': dt,
                             'mean_geral': mean_geral,
                             'mean_25_perc': mean_25_perc,
                             'mean_50_perc': mean_50_perc,
                             'mean_75_perc': mean_75_perc,
                             'median_geral': median_geral,
                             'median_25_perc': median_25_perc,
                             'median_50_perc': median_50_perc,
                             'median_75_perc': median_75_perc,
                             'std_geral': std_geral,
                             'std_25_perc': std_25_perc,
                             'std_50_perc': std_50_perc,
                             'std_75_perc': std_75_perc,
                             'perc95_geral': perc95_geral,
                             'perc95_25_perc': perc95_25_perc,
                             'perc95_50_perc': perc95_50_perc,
                             'perc95_75_perc': perc95_75_perc,
                             'cv_geral': cv_geral,
                             'cv_25_perc': cv_25_perc,
                             'cv_50_perc': cv_50_perc,
                             'cv_75_perc': cv_75_perc,
                             'trips_geral': trips_geral,
                             'trips_25_perc': trips_25_perc,
                             'trips_50_perc': trips_50_perc,
                             'trips_75_perc': trips_75_perc,
                             'ltt_mean': ltt_mean,
                             'rbt_mean': rbt_mean,
                             'ltt_mean_25_perc': ltt_mean_25_perc,
                             'rbt_mean_25_perc': rbt_mean_25_perc,
                             'ltt_mean_50_perc': ltt_mean_50_perc,
                             'rbt_mean_50_perc': rbt_mean_50_perc,
                             'ltt_mean_75_perc': ltt_mean_75_perc,
                             'rbt_mean_75_perc': rbt_mean_75_perc,
                             'ltt_median': ltt_median,
                             'rbt_median': rbt_median,
                             'ltt_median_25_perc': ltt_median_25_perc,
                             'rbt_median_25_perc': rbt_median_25_perc,
                             'ltt_median_50_perc': ltt_median_50_perc,
                             'rbt_median_50_perc': rbt_median_50_perc,
                             'ltt_median_75_perc': ltt_median_75_perc,
                             'rbt_median_75_perc': rbt_median_75_perc,
                             'ltt_cv': ltt_cv,
                             'ltt_cv_25_perc': ltt_cv_25_perc,
                             'ltt_cv_50_perc': ltt_cv_50_perc,
                             'ltt_cv_75_perc': ltt_cv_75_perc,
                             'ltt_std': ltt_std, 
                             'ltt_std_25_perc': ltt_std_25_perc,
                             'ltt_std_50_perc': ltt_std_50_perc,
                             'ltt_std_75_perc': ltt_std_75_perc})
    
    df_final.to_csv('/home/diego/processado/processado_{}.csv'.format(bus))
    
    return df_final

# Classe para geração dos headways
class Headway():
    def __init__(self, df, min_hour=4, max_hour=22):
        self.df = df    
        self.min_hour = min_hour
        self.max_hour = max_hour
            
    def beautifier(self):        
        # adding new columns
        self.df['month']= self.df.aproxlinkstart.dt.month
        self.df['date'] = self.df.aproxlinkstart.dt.date
        self.df['day']= self.df.aproxlinkstart.dt.day
        self.df['hour']= self.df.aproxlinkstart.dt.hour
        self.df['minute']= self.df.aproxlinkstart.dt.minute
        self.df['weekday']= self.df.aproxlinkstart.dt.weekday
        #self.df = pd.merge(self.df, self.df_link(), on='link')
        
        #travel id
        self.df = self.travel()        
        # sorting
        self.df.sort_values(['aproxlinkstart','link_index'], axis=0,ascending=True, inplace=True)               
        # working 4am to 10pm
        self.df = self.df[self.df.hour.isin(np.arange(self.min_hour,self.max_hour))]
        self.df = self.headway()
        self.df = self.holiday(HOLIDAYS_2017)
        
        # adding ltt cum
        self.df['ltt_acc'] = self.accum_ltt()
    
    def travel(self):
        df1 = self.df.copy()[self.df['link_index']!=0]
        df1['teste'] = df1.aproxlinkstart + pd.to_timedelta(df1.ltt, unit='s')
        df1.sort_values(['bus_id','teste','aproxlinkstart'], axis=0,ascending=True, inplace=True)
        trv = []
        t = 0
        for index, row in df1.iterrows():
            if row['link_index'] == 1:
                t+=1
                trv.append(t)
            else:
                trv.append(t)
        df1['travel_id'] = trv
        return df1
        
    def accum_ltt(self):
        self.df.sort_values(['travel_id', 'link_index'], axis = 0, ascending = True, inplace = True)
        df_1 = self.df[['travel_id', 'ltt','link_index']].copy()
        df_1.drop(['link_index'], axis = 1, inplace = True)
        df_2 = df_1.groupby('travel_id').cumsum()
        df_3 = df_2.values.tolist()
        flat_list = [item for sublist in df_3 for item in sublist]
        return flat_list
        
    def headway(self):
        # headway
        df_1 = self.df.copy()[self.df['link_index']==1]
        
        # datas
        datas = df_1.date.unique()
        
        # primeiras viagens do dia
        ftd = []
        for data in datas:
            ftd.append(df_1.loc[df_1['aproxlinkstart']== df_1[df_1['date']==data].aproxlinkstart.min()].travel_id.values)        
        np.asarray(ftd).ravel()
        
        df_1['headway'] = (df_1['aproxlinkstart'] - df_1['aproxlinkstart'].shift()).dt.total_seconds()
        for i in range(2,self.df['link_index'].max() + 1):
            df_2 = self.df.copy()[self.df['link_index']==i]
            df_2['headway'] = (df_2['aproxlinkstart'] - df_2['aproxlinkstart'].shift()).dt.total_seconds()
            df_1 = pd.concat([df_1,df_2])
        
        # zerando as primeiras viagens
        for index, row in df_1.iterrows():
            if row['travel_id'] in ftd:
                df_1.loc[index, 'headway'] = 0        
        return df_1
       
    def holiday(self,HOLIDAYS):
        # exclude holidays
        return self.df[~(self.df['date'].isin(HOLIDAYS))]

if __name__ == '__main__':
    
    HOLIDAYS_2017 = [dt.date(2017,1,1), dt.date(2017,1,25), dt.date(2017,2,27),dt.date(2017,2,28),
                 dt.date(2017,3,1), dt.date(2017,4,14), dt.date(2017,4,21), dt.date(2017,5,1),
                 dt.date(2017,6,15), dt.date(2017,6,16), dt.date(2017,7,9), dt.date(2017,9,7),
                 dt.date(2017,9,8)]
    
    #sample = pd.read_csv('sample_routes_SP.csv')
    #buslist = sample.route.tolist()
    buslist = ['574J-10-0','6L03-10-0']
    
    client = Client(n_workers=4, threads_per_worker=1)
    retorno = client.map(bus_process,buslist)
    df = pd.concat(client.gather(retorno))
    df.to_csv('df_final.csv')