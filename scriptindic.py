#!/usr/bin/env python

"""scriptindic.py: plusieurs scripts calcul indicateurs qualité et agrégation 6 min pour données HMVL """
# patrick gendre 24/06/20

import arrow
import psycopg2
import pandas as pd
import numpy as np
from dateutil import tz

def lirejourhmvl(jour,host,port,dbname,username,pwd):
	# import arrow
	# création d'un dataframe pour les données d'une journée
	# à partir des données en base dans la table hmvl Marius de la DirMed cf. github.com/patgendre/hmvl
	# USAGE : hmvl=lirejourhmvl("2020-04-26",host,port,dbname,username,pwd)
	conn = psycopg2.connect("host='{}' port={} dbname='{}' user={} password={}".format(host, port, dbname, username, pwd))
	lendemain=arrow.get(jour, 'YYYY-MM-DD').shift(days=1).format("YYYY-MM-DD")
	sql = "select * from hmvl where hdt >= '"+jour+"' and hdt < '"+lendemain+"';"
	x = pd.read_sql_query(sql, conn)
	conn = None
	# on supprime les colonnes hdt0 et id pour s'aligner sur les colonnes du fichier csv
	x=x.drop(columns=['hdt0','id'])
	x['hdt']=x['hdt'].apply(lambda x: x.tz_convert('Europe/Paris'))
	# on remplace par des types de données moins gourmands en mémoire suite aux gains de perf constatés sur données csv
	x['station']=x['station'].astype('category')
	x['status']=x['status'].astype('category')
	x['voie']=x['voie'].astype('category')
	x['statuttr']=x['statuttr'].astype('category')
	x['vitesse']=x['vitesse'].astype('float32')
	x['longueur']=x['longueur'].astype('float32')
	return x
	# TODO convertir les types de données pour limiter la mémoire : category, float32

# il est possible de lire aussi les données depuis un fichier csv produit par lirejourhmvl
def lirecsvhmvl(nomfichier):
    print(pd.Timestamp.now())
    hmvl = pd.read_csv(nomfichier,
                       dtype= {"station":'category', "status":'category', 
                               "voie":'category', "vitesse": 'float32', "longueur" : 'float32', "statuttr":'category'} ,
                       parse_dates=['hdt'], cache_dates=True, date_parser=lambda x: pd.to_datetime(x, utc=True))
    print(str(pd.Timestamp.now())+' après lecture du csv')
    hmvl['hdt']=hmvl['hdt'].dt.tz_convert('Europe/Paris')
    return hmvl
# les champs hdt0 et ID ne figurent pas dans le fichier csv: le champ hdt est un string qu'il faut convertir en timestamp
# PB : LENTEUR de traitement du fichier csv d'une journée, en juillet env 7M lignes, 330 Mo
# apparemment résolu par la dernière version avec date_parser=lambda x: pd.to_datetime(x, utc=True)
# la lecture de ne prend plus que 30 secondes au lieu de 10 minutes
# il reste ensuite à convertir hdt en timezone Paris

# la lecture prend quelques secondes mais la conversion du champ hdt string en datetime prend plusieurs minutes dans pandas
# malgré la recherche de solutions https://stackoverflow.com/questions/29882573/pandas-slow-date-conversion/59682653#59682653
# pas trouvé de solution:
# 1) lecture du csv avec conversion directe des datetime
# hmvl=pd.read_csv(chemin+'2020-07-22.csv',parse_dates=['hdt'], cache_dates=True, infer_datetime_format=True)
# 2) lecture d'abord du csv puis conversion
# hmvl=pd.read_csv(chemin+'2020-07-22.csv')
# hmvl['hdt']=pd.to_datetime(hmvl['hdt'],cache=True,infer_datetime_format=True)
# 2) lecture csv puis conversion via arrow
# from dateutil import tz
# hmvl['hdt']=hmvl['hdt'].apply(lambda x: arrow.get(x, tzinfo=tz.gettz('Europe/Paris')).datetime)
# 4) choix d'un format a priori
# hmvl=pd.read_csv(chemin+'2020-07-22.csv')
# hmvl['hdt']=pd.to_datetime(hmvl['hdt'],cache=True,format='%Y-%m-%dT%H:%M:%S.%f')
# ne fonctionne pas car LE FORMAT DES DATES N'EST PAS UNIFORME:
# - pour les stations Labocom est ajouté la timezone (+02:00)
# - quand on est sur une seconde "pile", les microsecondes .SSSSSS ne sont pas présentes
# à voir si en uniformisant le format (dans le script d'export csv) ça permettrai une conversion plus rapide
#   des strings en datetime avec un format fixe, mais quelques tests semblent montrer que non?


def indicqualite(hmvl):
	# création d'un dataframe d'indicateurs qualité, exportable ensuite en CSV et/ou en BD postgres
	# seuils de longueurs et vitesse aberrantes "en dur"
	Lmin=0.5
	Lmax=25.0
	Vmax=250.0
	# ajout d'une colonne heure pour les regroupements
	nbmes=hmvl.assign(heure=pd.to_datetime(hmvl['hdt']).dt.to_period('H'))
	nbmes=nbmes.groupby(['station','heure']).count().sort_values(by='station')
	nbmes=nbmes.rename(columns={'hdt':'nb_mes','vitesse':'nbmesvit','longueur':'nbmeslong'})
	nbmes=nbmes[['nb_mes','nbmesvit','nbmeslong']]
	status2=hmvl[hmvl["status"]=="2"][["station","hdt"]]
	status2=status2.assign(heure=pd.to_datetime(status2['hdt']).dt.to_period('H'))
	status2=status2.groupby(['station','heure']).count().sort_values(by='station')
	status2=status2.rename(columns={'hdt':'nb_status2'})
	status1=hmvl[hmvl["status"]=="1"][["station","hdt"]]
	status1=status1.assign(heure=pd.to_datetime(status1['hdt']).dt.to_period('H'))
	status1=status1.groupby(['station','heure']).count().sort_values(by='station')
	status1=status1.rename(columns={'hdt':'nb_status1'})
	status34=hmvl[hmvl["status"]=="34"][["station","hdt"]]
	status34=status34.assign(heure=pd.to_datetime(status34['hdt']).dt.to_period('H'))
	status34=status34.groupby(['station','heure']).count().sort_values(by='station')
	status34=status34.rename(columns={'hdt':'nb_status34'})
	qualite=pd.merge(nbmes,status2,on=['station','heure'],how='outer')
	qualite=pd.merge(qualite,status1,on=['station','heure'],how='outer')
	qualite=pd.merge(qualite,status34,on=['station','heure'],how='outer')
	qualite=qualite.sort_index()
	sansvoie=hmvl[hmvl["voie"].isna()][["station","hdt","statuttr"]]
	sansvoie=sansvoie.assign(heure=pd.to_datetime(sansvoie['hdt']).dt.to_period('H'))
	sansvoie=sansvoie.groupby(['station','heure']).count().sort_values(by='station')
	sansvoie=sansvoie.rename(columns={'hdt':'nb_sansvoie'})['nb_sansvoie']
	qualite=pd.merge(qualite,sansvoie,on=['station','heure'],how='outer')
	long_aberrante=hmvl[(hmvl['longueur']<Lmin) | (hmvl['longueur']>Lmax)][["station","hdt"]]
	long_aberrante=long_aberrante.assign(heure=pd.to_datetime(long_aberrante['hdt']).dt.to_period('H'))
	long_aberrante=long_aberrante.groupby(['station','heure']).count().sort_values(by='station')
	long_aberrante=long_aberrante.rename(columns={'hdt':'nb_l_aberr'})
	qualite=pd.merge(qualite,long_aberrante,on=['station','heure'],how='outer')
	vit_aberrante=hmvl[hmvl['vitesse']>Vmax][["station","hdt"]]
	vit_aberrante=vit_aberrante.assign(heure=pd.to_datetime(vit_aberrante['hdt']).dt.to_period('H'))
	vit_aberrante=vit_aberrante.groupby(['station','heure']).count().sort_values(by='station')
	vit_aberrante=vit_aberrante.rename(columns={'hdt':'nb_v_aberr'})
	qualite=pd.merge(qualite,vit_aberrante,on=['station','heure'],how='outer')
	qualite=qualite.fillna(0.0)
	# calcul des taux de "trames" (fichiers RD) absents
	hmvl = hmvl.set_index('hdt')
	x=hmvl.groupby(['station'])['status'].resample('6S').size()
	# on compte le nombre de lignes mesurées pour chaque période de secondes
	# si on trouve 0 le fichier 6 secondes n'a pas été transmis, si on trouve >0 le fichier n'a pas été transmis pour cette horodate
	x[x>0] = 1
	x=x.reset_index()
	y=x.assign(heure=x['hdt'].dt.to_period('H'))
	y=y.set_index('hdt')
	y=y.groupby(['station','heure']).sum()
	y=y.rename(columns={'status':'taux_trames_absentes'})
	y=(100.*(1.0-y/600.0)).round(2)
	y=y.reset_index()
	qualite=qualite.reset_index()
	qualite=pd.merge(qualite,y,on=['station','heure'],how='outer')
	return qualite

def agreg6(x):
	#agrégation de mesures 6 minutes d'un dataframe x représentant un jour de données hmvl lues par lirejourhmvl
	x=x[x["voie"].notna()]
	# on enlève les mesures sans voie
	x=x[(x["status"]=="0")|(x["status"].isna())][["hdt","station","voie","vitesse","longueur","statuttr"]]
	# on enlève les mesures de status 1,2,3,4
	Lmin=0.5
	Lmax=25.0
	Vmax=250.0
	x=x[(x['vitesse']<Vmax)]
	x=x[(x['longueur']>Lmin)]
	x=x[(x['longueur']<Lmax)]
	# on enlève les vitesses et longueurs aberrantes
	x = x.set_index('hdt')
	x['vitesse']=np.where(np.isclose(0.0,x['vitesse'].values),1.0,x['vitesse'].values)    
	x['invvit']=x['vitesse'].apply(lambda v: 1/v)
	moy6=x.groupby(['station'])['longueur','invvit'].resample('6Min').mean()
	moy6['invvit']=moy6['invvit'].apply(lambda v: 1/v)
	# je n'ai pas réussi à faire marcher la fonction stats.hmean de scipy
	moy6q=x.groupby(['station'])['vitesse'].resample('6Min').size()
	moy6=pd.merge(moy6,moy6q,on=['station','hdt'],how='outer')
	moy6=moy6.rename(columns={'invvit':'v6','longueur':'l6','vitesse':'q6'})
	moy6=moy6.apply(lambda x: round(x,2))
	return moy6

## CODE tramesmanquantes OBSOLETE
def tramesmanquantes(x,jour):
	# comptage des trames manquantes dans une journée de données hmvl lue dans un dataframe x avec la fn lirejourhmvl
	h0=x['hdt0'].unique()
	t0=[]
	jours0=[]
	for t in h0:
		t0.append(arrow.get(t).format("YYYY-MM-DD HH:mm:ss"))
		jours0.append(arrow.get(t).format("YYYY-MM-DD"))
	if jour not in set(jours0):
		print(jour+" n'est pas dans les jours du dataframe :"+str(set(jours0)))
		return
	s6=pd.date_range(jour, periods=6000, freq='6S')
	t6=[]
	for s in s6:
		t6.append(arrow.get(s).format("YYYY-MM-DD HH:mm:ss"))
	trames6absentes=set(t6)-set(t0)
	trames6communes=set(t6)&set(t0)
	trames6enplus=set(t0)-set(t6)
	return len(trames6communes, trames6enplus, trames6absentes)

# CODE ecrire qualite OBSOLETE : a priori pas de besoin de stocker les indicateurs en base
# les indicateurs seront stockés dans des fichiers journaliers
def ecrirequalite(q,pwd):
	# fonction écrivant les indicateurs qualité en base : dataframe produit par la fonction indicqualite
	# suppose qu'existe la table indic avec le même schéma (colonnes)
	from sqlalchemy import create_engine
	q=q.reset_index()
	q.nb_status2=q.nb_status2.astype('int')
	q.nb_status1=q.nb_status1.astype('int')
	q.nb_status34=q.nb_status34.astype('int')
	q.nb_sansvoie=q.nb_sansvoie.astype('int')
	q.station=q.station.astype('string')
	q.heure=q.heure.apply(lambda d: pd.to_datetime(str(d)))
	engine = create_engine('postgresql+psycopg2://dirmed:'+pwd+'@localhost:5432/hmvl')
	connection = engine.connect()
	q.to_sql('indic', connection, index=False, if_exists='append')
	connection.close()

# CODE ecrire qualite OBSOLETE : a priori pas de besoin de stocker les données 6 minutes en base
# les indicateurs 6' seront stockées dans des fichiers journaliers
def ecrireagreg6(m,pwd):
	# fonction écrivant les données agrégées 6' en base : en entrée dataframe produit par la fonction agreg6
	# suppose qu'existe la table moy6 avec le même schéma (colonnes)
	# pour l'instant l'horodate est encodée comme une string en base
	from sqlalchemy import create_engine
	m=m.reset_index()
	m.station=m.station.astype('string')
	engine = create_engine('postgresql+psycopg2://dirmed:'+pwd+'@localhost:5432/hmvl')
	connection = engine.connect()
	m.to_sql('moy6', connection, index=False, if_exists='append')
	connection.close()

def tocsv(m,file):
	# fonction écrivant les données agrégées 6' en base : en entrée dataframe q produit par la fonction indicqualite
	#                                                      ou dataframe m produit par la fonction agreg6
	#   et nom du fichier
	# suppose qu'existe la table moy6 avec le même schéma (colonnes)
	# pour l'instant l'horodate est encodée comme une string en base
	m=m.reset_index()
	m.to_csv(path_or_buf=file, index=False)