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
	veille=(datetime.date(*map(int, j.split('-'))) - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
	lendemain=(datetime.date(*map(int, j.split('-'))) + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
	conn = psycopg2.connect("host='{}' port={} dbname='{}' user={} password={}".format(host, port, dbname, username, pwd))
	sql = "select * from hmvl where hdt<'"+lendemain+"' and hdt>='"+veille+"';"
	hmvl = pd.read_sql_query(sql, conn)
	conn = None
	nbmes=hmvl.assign(jour=pd.to_datetime(hmvl['hdt']).dt.to_period('D'))
	nbmes=nbmes.groupby(['jour','station']).count().sort_values(by='hdt')
	nbmes=nbmes.rename(columns={'hdt':'nb_mes','vitesse':'nbmesvit','longueur':'nbmeslong'})
	nbmes=nbmes[['nb_mes','nbmesvit','nbmeslong']]
	status2=hmvl[hmvl["status"]=="2"][["hdt","station"]]
	status2=status2.assign(jour=pd.to_datetime(status2['hdt']).dt.to_period('D'))
	status2=status2.groupby(['jour','station']).count().sort_values(by='hdt')
	status2=status2.rename(columns={'hdt':'nb_status2'})
	qualite=pd.merge(nbmes,status2,on=['jour','station'],how='outer')
	hmvl=hmvl[hmvl["status"]=="0"][["id","hdt","station","voie","vitesse","longueur","statuttr"]]
	sansvoie=hmvl[hmvl["voie"].isna()][["hdt","station","statuttr"]]
	sansvoie=sansvoie.assign(jour=pd.to_datetime(sansvoie['hdt']).dt.to_period('D'))
	sansvoie=sansvoie.groupby(['station','jour']).count().sort_values(by='hdt')
	sansvoie=sansvoie.rename(columns={'hdt':'nb_sansvoie'})['nb_sansvoie']
	qualite=pd.merge(qualite,sansvoie,on=['jour','station'],how='outer')
	qualite.fillna(0.0)
	# manque à ajouter le nb de doublons hmvl (à calculer ci-dessous) et éventuellement à les enlever ensuite de hmvl
	# puis à écrire en base (ou en CSV)
	hmvl=hmvl[hmvl["voie"].notna()][["id","hdt","station","voie","vitesse","longueur","statuttr"]]

	moy6m=hmvl.groupby(['station'])['vitesse','longueur'].resample('6Min').mean()
	moy6q=hmvl.groupby(['station'])['vitesse'].resample('6Min').size()
	# manque à écrire en base

	# manque à écrire en base le XLS du référentiel avec les coordonnées pour tester geopandas
if __name__ == '__main__':
	jour6min()
