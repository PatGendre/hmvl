#!/usr/bin/env python

"""jourhmvl2pg.py: lecture de fichiers du répertoire d'un jour hmvl VRU Marius6 secondes et écriture dans une BD pg, cf. github.com/patgendre/hmvl """

# patrick gendre 07/04/20
# 
import click
import pathlib
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
def lirehmvl2pg(f,u,p,stations,log=False):
	with open(f,'r') as ff:
		# header du fichier RD: horodate en clair et en temps unix
		# u, p : user password d'accès à la BD postgres hmvl
		# stations : dict des noms des stations passé en paramètre optionnel
		# log : enregistrement de la lecture dans la table log_imports
		dt_texte=ff.readline()
		if dt_texte=='':
			print("ATTENTION fichier VIDE: "+f)
			return
		else:
			dt_texte=dt_texte[:-1]
		dt_unix0=datetime.datetime.fromtimestamp(float(ff.readline()[0:10]))
		liste_mesures=[]
		# stations peut être passé en paramètre pour ne pas être lu n fois
		if stations is None:
			connection = psycopg2.connect(user=u, password=p, host="127.0.0.1", \
				port="5432", database="hmvl")
			# lecture des codes des stations
			cursor = connection.cursor()
			cursor.execute("SELECT * FROM code_station;")	
			codes=cursor.fetchall()
			stations={}
			for c in codes:
				stations[c[0]]=c[1]
			connection.commit()
			cursor.close()
			connection.close()
		# lecture du fichier, une ligne par trame hmvl
		for ligne in ff:
			indexstn=int(ligne[0:4])
			if indexstn not in stations:
				#print("WARNING: station "+str(indexstn)+ " pas trouvée!")
				indexstn=ligne[0:4]
			else:
				indexstn=stations[indexstn]
			etatstn=ligne[4]
			n=(len(ligne)-6)//11
			# pour une ligne sans mesure, on garde traces des trames vides, pour des diagnostics
			# on sauve aussi le statut du dernier caractère de la ligne (absent si la trame est vide, 5ème caractère à 2)
			# le dernier caractère étant \n, c'est en fait l'avant dernier qu'il faut lire
			statutTR=ligne[-2:-1]
			if etatstn=="2": statutTR=None
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
	connection.commit()
	# log de l'import
	if log:
		postgres_insert_query = "INSERT INTO log_imports (fichier,horodate,nbmes) VALUES (%s,%s,%s)"
		nbmes=len(liste_mesures)
		print(str(datetime.datetime.now())+" Insertion de "+str(nbmes)+" lignes pour "+f)
		cursor = connection.cursor()
		cursor.execute(postgres_insert_query,(f,datetime.datetime.now(),nbmes))
		connection.commit()
	cursor.close()
	connection.close()


def lirelabocom(f,u,p):
	# lecture d'un fichier de mesures individuelles au format CSV Labocom
	# f : fichier de mesures CSV
	# u utilisateurs, p mot de passe
	# jour,heure : heure de démarrage de la lecture
	# on déduit jour et heure du nom du chemin du fichier qui doit être AAAA-MM-JJ/HH-MM/labocom

	# à revoir ensuite : PAtHLIB
	liste_mesures=[]
	if os.path.exists(f):
		print(f+" : fichier inexistant.")
		return
	jour=str.split(f,'/')[0]
	heure=str.split(f,'/')[1]
	datetime.datetime.fromtimestamp()
	with open(f) as csvfile:
		reader = csv.DictReader(csvfile,separator=";")
		for row in reader:
			if row['REQUETE']!="MI 1":
				continue
			dt_texte=jour+" "+heure+":00"
			dt_unix0=datetime.strptime(dt_texte,"%Y-%m-%d %H-%M-%S")
			# lecture du fichier, une ligne par trame hmvl
			indexstn=row['RGS']
			# le nom de la station peut aussi être déduit du fichier
			if indexstn!=f[3:6]:
				print("WARNING station différente de : "+f[3:6])
			# valeur arbitraire pour ce status qui existe dans les fichiers RD et pas dans les fichiers LABOCOM
			etatstn=None
			reponse=row['REPONSE']
			# pour une ligne sans mesure, on garde traces des trames vides, pour des diagnostics
			# on sauve aussi le statut du dernier caractère de la ligne (absent si la trame est vide, 5ème caractère à 2)
			# le dernier caractère étant \n, c'est en fait l'avant dernier qu'il faut lire
			if reponse[0:2]!="T:":
				print("WARNING: trame sans T: !!!")
				continue
			reponse=reponse[2:]
			statutTR=reponse[:-1]
			n=(len(reponse)-6)//11
			if n==0:
				mesure = (dt_texte,dt_unix0.isoformat(),indexstn,etatstn,None,None,None,statutTR)
				liste_mesures.append(mesure)
			for i in range(n):
				c11=reponse[6+i*11:17+i*11]
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
	connection.commit()
	cursor.close()
	# enregistrement de la lecture effectuée dans la table Log_imports
	cursor = connection.cursor()
	postgres_insert_query = "INSERT INTO log_imports (fichier,horodate,nbmes) VALUES %s"
	nmesures=len(liste_mesures)
	print(str(datetime.datetime.now())," Insertion de "+str(nmesures)+" lignes pour "+f)
	log_import=(f,datetime.datetime.now(),nmesures)
	connection.commit()
	cursor.close()
	connection.close()

from pathlib import Path
def jourhmvl2pg(jour,pwd,racine="..",exportcsv=False):
	# code factorisé pour rdc_0 et rdc_1 : rdc est 'rdc_0' ou 'rdc_1'
	def lirerdc(jour,rep,pwd,rdc,stations,racine="..",exportcsv=False):
		# jour nom du répertoire AAAA-MM-JJ
		# rep nom du sous répetoire HH-MM
		# pwd mot de passe de la base hmvl postgresql
		# rdc : "rdc_0" ou "rdc_1" pour les 2 frontaux de recueil de données VRU
		# racine : chemin initial pour aboutir au répertoire jour depuis l'exécution du code
		# exportcsv=True appelle lirehmvl2csv au lieu de lirehmvl2pg
		path0=Path(racine)
		path_rdc=path0 / jour / rep / rdc
		if not path_rdc.exists:
			print(str(path_rdc)+" n'existe pas.")
			return
		print ("répertoire "+rep)
		print("nombre de fichiers trouvés dans "+rdc+": "+str(len(list(path_rdc.glob('**/*')))))
		# on ne teste pas s'il y a des fichiers qui ne commencent pas par RD
		for fichier in list(path_rdc.glob('**/RD*')):
			if str(fichier)[-6:]=="RD_PAS":
				f=open(fichier,"r")
				texte=f.readline()
				if len(texte)>0:
					texte=texte[:-1]
				print("Contenu de RD_PAS: "+texte)
				continue
			if exportcsv:
				lirehmvl2csv(str(fichier))
			else:
				lirehmvl2pg(str(fichier),"dirmed",pwd,stations,True)
	path0=Path(racine)
	if len(jour)!= 10:
		print("format de date d'entrée: AAAA-MM-JJ")
		return
	if not Path.exists(path0 / jour):
		print("Le répertoire "+jour+" n'existe pas.")
		return
	# pas besoin de lire les codes de station pour l'export CSV mais on les lit quand même
	# stations est passé en paramètre à lirehmvl2pg pour ne pas être lu n fois
	connection = psycopg2.connect(user="dirmed", password=pwd, host="127.0.0.1", \
		port="5432", database="hmvl")
	# lecture des codes des stations
	cursor = connection.cursor()
	cursor.execute("SELECT * FROM code_station;")	
	codes=cursor.fetchall()
	stations={}
	for c in codes:
		stations[c[0]]=c[1]
	connection.commit()
	cursor.close()
	connection.close()
	# boucle sur les répertoires du jour 'HH-MM'
	# TODO lire aussi les fichiers Labocom
	for rep in list((path0 / jour).glob('*')):
		print(datetime.datetime.now().time())
		lirerdc(jour,str(rep),pwd,"rdc_0",stations)
		lirerdc(jour,str(rep),pwd,"rdc_1",stations)
	print(datetime.datetime.now().time())

