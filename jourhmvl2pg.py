#!/usr/bin/env python

"""jourhmvl2pg.py: lecture de fichiers du répertoire d'un jour hmvl VRU Marius6 secondes et écriture dans une BD pg, cf. github.com/patgendre/hmvl """

# patrick gendre 07/04/20
# 
import click
import os
import datetime
import csv
import psycopg2
import psycopg2.extras
# cf. tentative d'optimisation https://www.psycopg.org/docs/extras.html#fast-execution-helpers
##import hmvlRD2pg

# on verra ensuite comment gérer proprement les imports / les package 
#@click.command()
#@click.option("--jour", default="2020-04-02", help="Jour à importer dans postgresql AAAA-MM-JJ.")
#@click.option("--p",help="mot de passe")
def lirehmvl(f,u,p):
	with open(f,'r') as ff:
		# header : horodate en clair et en temps unix
		dt_texte=ff.readline()
		if dt_texte=='':
			print("ATTENTION fichier VIDE: "+f)
			return
		else:
			dt_texte=dt_texte[:-1]
		dt_unix0=datetime.datetime.fromtimestamp(float(ff.readline()[0:10]))
		liste_mesures=[]
		postgres_insert_query = "INSERT INTO hmvl (horodate_id,hdt,station,status,voie,vitesse,longueur,statutTR) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
		connection = psycopg2.connect(user=u, password=p, host="127.0.0.1", \
			port="5432", database="hmvl")
		cursor = connection.cursor()
		# lecture du fichier, une ligne par trame hmvl
		for ligne in ff:
			indexstn=ligne[0:4]
			etatstn=ligne[4]
			n=(len(ligne)-6)//11
			# pour une ligne sans mesure, on garde traces des trames vides, pour des diagnostics
			# on sauve aussi le statut du dernier caractère de la ligne (absent si la trame est vide, 5ème caractère à 2)
			# le dernier caractère étant \n, c'est en fait l'avant dernier qu'il faut lire
			statutTR=ligne[-2:-1]
			if statutTR=="2": statutTR=None
			if n==0:
				mesure = (dt_texte,dt_unix0.isoformat(),indexstn,etatstn,None,None,None,statutTR)
				liste_mesures.append(mesure)
			for i in range(n):
				c11=ligne[6+i*11:17+i*11]
				numvoie=c11[0]
				dt_unix=dt_unix0+datetime.timedelta(seconds=int(c11[1:3]),microseconds=int(c11[3:5])*10000)
				vitesse=c11[5:8]
				if vitesse=='   ' or vitesse=="":
					vitesse=None
				else:
					vitesse=float(vitesse)
				longueur=c11[8:11]
				if longueur=='' or longueur=="   ":
					longueur=None
				else:
					longueur=float(longueur)*0.1
				mesure = (dt_texte,dt_unix.isoformat(),indexstn,etatstn,numvoie,vitesse,longueur,statutTR)
				liste_mesures.append(mesure)
	# ESSAI de connexion à postgres et écriture en une fois : pour une raison inconnue le execute_values ne rend pas la main??
	# cf. execute_values
	postgres_insert_query = "INSERT INTO hmvl (horodate_id,hdt,station,status,voie,vitesse,longueur,statutTR) VALUES %s"
	connection = psycopg2.connect(user=u, password=p, host="127.0.0.1", \
		port="5432", database="hmvl")
	cursor = connection.cursor()
	psycopg2.extras.execute_values(cursor,postgres_insert_query, liste_mesures,page_size=1000)
	print("Insertion de "+str(len(liste_mesures))+" lignes pour "+f)
	connection.commit()
	cursor.close()
	connection.close()

def jourhmvl2pg(jour,p):
	# code factorisé pour rdc_0 et rdc_1 : rdc est 'rdc_0' ou 'rdc_1'
	def lirerdc(jour,rep,p,rdc):
		path_rdc=jour+"/"+rep+"/"+rdc
		print ("répertoire "+rep)
		print("nombre de fichiers trouvés dans "+rdc+": "+str(len(os.listdir(path_rdc))))
		if not os.path.exists(path_rdc):
			print(path_rdc+" n'existe pas.")
			return
		for fichier in os.listdir(path_rdc):
			if fichier=="RD_PAS":
				f=open(path_rdc+"/"+fichier,"r")
				texte=f.readline()
				if len(texte)>0:
					texte=texte[:-1]
				print("Contenu de RD_PAS: "+texte)
				continue
			if fichier[:2]!="RD":
				print("fichier "+fichier+" pas RD??")
			else:
				lirehmvl(path_rdc+"/"+fichier,"dirmed",p)
	# boucle sur les répertoires
	# TODO lire aussi les fichiers Labocom
	print("lecture du jour "+jour+"dans postgresql.")
	if len(jour)!= 10:
		print("format de date d'entrée: AAAA-MM-JJ")
		return
	if not os.path.exists(jour):
		print("Le répertoire "+jour+" n'existe pas.")
	for rep in os.listdir(jour):
		print(datetime.datetime.now().time())
		lirerdc(jour,rep,p,"rdc_0")
		lirerdc(jour,rep,p,"rdc_1")
	print(datetime.datetime.now().time())

