#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Sep 26 09:47:10 2020

@author: diluisi
"""
import os
import urllib
#import pandas as pd

#gerador de datas
#x = pd.date_range("01-01-2017","31-12-2017")
#with open('dates.txt', 'w') as f:
#    for item in x:
#        f.write("%s\n" % item)



csvfile = 'dates.txt'
targetdir = '/home/diluisi/Documentos/Doutorado/Codigo/Mapas/demanda2017/'

with open(csvfile) as links:
    for link in links:
        filename = link.split('/')[-1].strip()
        filepath = os.path.join(targetdir, filename)
        print('Downloading %s \n\t .. to %s' % (link.strip(), filepath))
        urllib.request.urlretrieve(link, filename)  # For Python 3
