#!/usr/bin/env python

"""qualif6mibjourhmvl.py: 
 lecture d'un jour de données Marius en base postgres et écriture de données agrégées 6min et indicateurs de qualité
 cf. github.com/patgendre/hmvl """

# patrick gendre 30/05/20
# 
import pandas as pd
import matplotlib
import psycopg2
import datetime
import csv
import click

# pour utiliser : copier coller le code dans une console python OU
# ligne de commande pour la fonction jourhmvl2pg
# on verra ensuite comment gérer proprement l'import de code/ les package 

# ATTENTION : BROUILLON PAS UTILISABLE POUR L'INSTANT...

@click.command()
@click.option('--jour', prompt='jour:', help='string HHHH-MM-AA')
@click.option('--pwd', prompt='Mdp base hmvl:', help='mot de passe postgresql')

def jour6min(jour,pwd):
# jour "AAAA-MM-JJ"
# pwd : mot de passe de la base postgres dirmed
	host="localhost"
	port=5432
	dbname="hmvl"
	username="dirmed"
	#pwd="votremotdepasse"
	veille=(datetime.date(*map(int, jour.split('-'))) - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
	lendemain=(datetime.date(*map(int, jour.split('-'))) + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
	conn = psycopg2.connect("host='{}' port={} dbname='{}' user={} password={}".format(host, port, dbname, username, pwd))
	sql = "select * from hmvl where hdt<'"+lendemain+"' and hdt>'"+veille+"';"
	hmvl = pd.read_sql_query(sql, conn)
	conn = None
	print(str(len(hmvl))+ " lignes lues pour le "+jour)
	hmvl['hdt']=hmvl['hdt'].apply(lambda x: x.tz_convert('Europe/Paris'))
	hmvl['hdt0']=hmvl['hdt0'].apply(lambda x: x.tz_convert('Europe/Paris'))
	nbmes=hmvl.assign(jour=pd.to_datetime(hmvl['hdt']).dt.to_period('D'))
	nbmes=nbmes.groupby(['jour','station']).count().sort_values(by='hdt')
	nbmes=nbmes.rename(columns={'hdt':'nb_mes','vitesse':'nbmesvit','longueur':'nbmeslong'})
	nbmes=nbmes[['nb_mes','nbmesvit','nbmeslong']]
	status2=hmvl[hmvl["status"]=="2"][["hdt","station"]]
	status2=status2.assign(jour=pd.to_datetime(status2['hdt']).dt.to_period('D'))
	status2=status2.groupby(['jour','station']).count().sort_values(by='hdt')
	status2=status2.rename(columns={'hdt':'nb_status2'})
	status1=hmvl[hmvl["status"]=="1"][["hdt","station"]]
	status1=status1.assign(jour=pd.to_datetime(status1['hdt']).dt.to_period('D'))
	status1=status1.groupby(['jour','station']).count().sort_values(by='hdt')
	status1=status1.rename(columns={'hdt':'nb_status1'})
	status34=hmvl[hmvl["status"]=="34"][["hdt","station"]]
	status34=status34.assign(jour=pd.to_datetime(status34['hdt']).dt.to_period('D'))
	status34=status34.groupby(['jour','station']).count().sort_values(by='hdt')
	status34=status34.rename(columns={'hdt':'nb_status34'})
	qualite=pd.merge(nbmes,status2,on=['jour','station'],how='outer')
	qualite=pd.merge(qualite,status1,on=['jour','station'],how='outer')
	qualite=pd.merge(qualite,status34,on=['jour','station'],how='outer')

	# on enlève les mesures des status en erreur : 1,2,3,4
	hmvl=hmvl[(hmvl["status"]=="0")|(hmvl["status"].isna())][["id","hdt0","hdt","station","voie","vitesse","longueur","statuttr"]]
	sansvoie=hmvl[hmvl["voie"].isna()][["hdt","station","statuttr"]]
	sansvoie=sansvoie.assign(jour=pd.to_datetime(sansvoie['hdt']).dt.to_period('D'))
	sansvoie=sansvoie.groupby(['station','jour']).count().sort_values(by='hdt')
	sansvoie=sansvoie.rename(columns={'hdt':'nb_sansvoie'})['nb_sansvoie']
	qualite=pd.merge(qualite,sansvoie,on=['jour','station'],how='outer')
	qualite=qualite.fillna(0.0)
	qualite=qualite.sort_index()
	qualite.nb_status2=qualite.nb_status2.astype(int)
	qualite.nb_status1=qualite.nb_status1.astype(int)
	qualite.nb_status34=qualite.nb_status34.astype(int)
	qualite.nb_sansvoie=qualite.nb_sansvoie.astype(int)
	qualite=qualite.reset_index()
	qualite.jour=qualite.jour.astype('string')
	qualite.station=qualite.station.astype('string')
	print("indicateurs qualité calculés")
	# manque à ajouter le nb de doublons hmvl (à calculer ci-dessous) et éventuellement à les enlever ensuite de hmvl
	# puis à écrire en base (ou en CSV)
	# on enlève les mesures sans voie (+ de fait ni longueur, ni vitesse)
	hmvl=hmvl[hmvl["voie"].notna()][["id","hdt0","hdt","station","voie","vitesse","longueur","statuttr"]]
	hmvl = hmvl.set_index('hdt')
	moy6m=hmvl.groupby(['station'])['vitesse','longueur'].resample('6Min').mean()
	moy6q=hmvl.groupby(['station'])['vitesse'].resample('6Min').size()
	moy6m=moy6m.rename(columns={'vitesse':'v6','longueur':'l6'})
	moy6q=moy6q.rename('q6')
	moy6=pd.merge(moy6m,moy6q,on=['hdt','station'],how='outer')
	print("moyennes 6 minutes calculées")
	# manque à écrire en base
	from sqlalchemy import create_engine
	engine = create_engine('postgresql+psycopg2://dirmed:marius@localhost:5432/hmvl')
	connection = engine.connect()
	moy6.to_sql('moy6', connection, if_exists='append')
	qualite.to_sql('indic',connection, index=False,if_exists='append')
	connection.close()
	print("tables moy6 et indic mises à jour en base")
	# manque à écrire en base le XLS du référentiel avec les coordonnées pour tester geopandas
if __name__ == '__main__':
	jour6min()
