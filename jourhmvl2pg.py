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
def hmvl2pg(f,u,p,stations,log=False):
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
			# il faudrait tester que n(len(ligne)-7)%11=0 càd que les trames comptent bien nx11 caractères
			# pour les données RD jusqu'à présent on n'a pas rencontré de pb mais on l'a constaté sur labocom
			n=(len(ligne)-7)//11
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
	postgres_insert_query = "INSERT INTO hmvl (id,horodate_id,hdt,station,status,voie,vitesse,longueur,statutTR) VALUES %s"
	connection = psycopg2.connect(user=u, password=p, host="127.0.0.1", \
		port="5432", database="hmvl")
	index_query = "SELECT MAX(id) from hmvl;"
	cursor = connection.cursor()
	cursor.execute(index_query)
	maxid=cursor.fetchall()[0][0]
	if maxid is None: maxid=0
	psycopg2.extras.execute_values(cursor,postgres_insert_query, liste_mesures,
		template="(DEFAULT,%s,%s,%s,%s,%s,%s,%s,%s)",page_size=1000)
	connection.commit()
	# log de l'import
	if log:
		postgres_insert_query = "INSERT INTO log_imports (fichier,horodate,nbmes,firstid,lastid) VALUES (%s,%s,%s,%s,%s)"
		nbmes=len(liste_mesures)
		print(str(datetime.datetime.now())+" Insertion de "+str(nbmes)+" lignes pour "+f)
		cursor = connection.cursor()
		# on suppose qu'il n'y aura pas eu d'écritures simultanées dans la base!
		cursor.execute(postgres_insert_query,(f,datetime.datetime.now(),nbmes,maxid+1,maxid+len(liste_mesures)))
		connection.commit()
	cursor.close()
	connection.close()

def labocom2pg(jour,rep,pwd,u="dirmed",log=True):
	# lecture d'un fichier de mesures individuelles au format CSV Labocom
	# cf. le wiki https://github.com/PatGendre/hmvl/wiki/Fichiers-Labocom-(autres-stations)/
	# on déduit jour et heure du nom du chemin du fichier qui doit être AAAA-MM-JJ/HH-MM/labocom
	# string AAAA-MM-JJ
	# rep chemon complet depuis le répertoire courant, ex. "../2020-04-02/labocom"
	# pwd : mot de passe d'accès à la base hmvl sur postgresql
	rep=pathlib.Path(rep)
	if not rep.is_dir():
		print(str(rep)+" n'existe pas.")
		return
	print(str(rep))
	for fichier in list(rep.glob('**/*')):
		x= str.split(fichier.name,'_')
		if len(x)!=3:
			#print (fichier.name + " n'a pas un nom attendu, on ne le lit pas.")
			continue
		if x[2][-4:]!=".csv":
			#print (fichier.name + " n'a pas un nom attendu, on ne le lit pas.")
			continue
		# ATTENTION  on suppose que les noms RGS labocom sont en MAJUSCULES???
		rgs=str.upper(x[1])
		jj=str.upper(x[0])
		liste_mesures=[]
		with open(fichier) as csvfile:
			reader = csv.DictReader(csvfile,delimiter=";")
			for row in reader:
				if row['REQUETE']!="MI 1":
					continue
				dt_texte=jour+" "+row['HORODATE']
				dt_unix0=datetime.datetime.strptime(dt_texte,"%Y-%m-%d %H:%M:%S")
				# lecture du fichier, une ligne par trame hmvl
				indexstn=row['RGS']
				# le nom de la station peut aussi être déduit du fichier
				if indexstn!=rgs:
					print("WARNING station différente de : "+rgs)
				# valeur arbitraire pour ce status qui existe dans les fichiers RD et pas dans les fichiers LABOCOM
				etatstn=None
				reponse=row['REPONSE']
				# pour une ligne sans mesure, on garde traces des trames vides, pour des diagnostics
				# on sauve aussi le statut du dernier caractère de la ligne (absent si la trame est vide, 5ème caractère à 2)
				# le dernier caractère étant \n, c'est en fait l'avant dernier qu'il faut lire
				if reponse is None or reponse=="":
					print("WARNING: trame vide !!!")
					continue		
				if reponse[0:2]!="T:":
					print("WARNING: trame sans T: !!!")
					continue
				statutTR=reponse[-1:]
				n=(len(reponse)-3)//11
				if ((len(reponse)-3)%11)!=0:
					print("WARNING : ligne avec un nb de caractères non multiple de 11")
					continue
				if n==0:
					mesure = (dt_texte,dt_unix0.isoformat(),indexstn,etatstn,None,None,None,statutTR)
					liste_mesures.append(mesure)
				for i in range(n):
					c11=reponse[2+i*11:13+i*11]
					numvoie=c11[0]
					s=c11[1:3]
					ms=c11[3:5]
					if s=="" or s=="  " or ms=="" or ms=="  ":
						dt_unix=None
					else:
						dt_unix=dt_unix0+datetime.timedelta(seconds=int(s),microseconds=int(ms)*10000)
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
					if dt_unix is None:
						continue
						# le champ hdt en base est un TIMESTAMPTZ NOT NULL, on n'enregistre la ligne vide
					else:
						mesure = (dt_texte,dt_unix.isoformat(),indexstn,etatstn,numvoie,vitesse,longueur,statutTR)
						liste_mesures.append(mesure)
		postgres_insert_query = "INSERT INTO hmvl (id,horodate_id,hdt,station,status,voie,vitesse,longueur,statutTR) VALUES %s"
		connection = psycopg2.connect(user=u, password=pwd, host="127.0.0.1", \
			port="5432", database="hmvl")
		cursor = connection.cursor()
		index_query = "SELECT MAX(id) from hmvl;"
		cursor = connection.cursor()
		cursor.execute(index_query)
		maxid=cursor.fetchall()[0][0]
		if maxid is None: maxid=0
		psycopg2.extras.execute_values(cursor,postgres_insert_query, liste_mesures,
			template="(DEFAULT,%s,%s,%s,%s,%s,%s,%s,%s)",page_size=1000)
		connection.commit()
		cursor.close()
		# enregistrement de la lecture effectuée dans la table Log_imports
	# log de l'import
		if log:
			postgres_insert_query = "INSERT INTO log_imports (fichier,horodate,nbmes,firstid,lastid) VALUES (%s,%s,%s,%s,%s)"
			nbmes=len(liste_mesures)
			print(str(datetime.datetime.now())," Insertion de "+str(nbmes)+" lignes pour "+str(fichier.name))
			cursor = connection.cursor()
			# on suppose qu'il n'y aura pas eu d'écritures simultanées dans la base!
			cursor.execute(postgres_insert_query,(str(fichier),datetime.datetime.now(),nbmes,maxid+1,maxid+nbmes))
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
		# exportcsv=True appelle lirehmvl2csv au lieu de hmvl2pg
		path0=Path(racine)
		path_rdc=path0 / jour / rep / rdc
		if not path_rdc.exists:
			print(str(path_rdc)+" n'existe pas.")
			return
		print ("répertoire "+rep)
		print("nombre de fichiers trouvés dans "+rdc+": "+str(len(list(path_rdc.glob('**/*')))))
		# on ne teste pas s'il y a des fichiers qui ne commencent pas par RD
		for fichier in list(path_rdc.glob('**/RD*')):
			nomfichier=fichier.name
			if len(nomfichier)!=9:
				print("Fichier "+nomfichier+ " ignoré")
				continue
			if nomfichier[-3:]==".dd" or nomfichier[-4:]==".dup":
				print(nomfichier+": doublon")
			if exportcsv:
				hmvl2csv(str(fichier))
			else:
				hmvl2pg(str(fichier),"dirmed",pwd,stations,True)
	path0=Path(racine)
	if len(jour)!= 10:
		print("format de date d'entrée: AAAA-MM-JJ")
		return
	if not Path.exists(path0 / jour):
		print("Le répertoire "+jour+" n'existe pas.")
		return
	# pas besoin de lire les codes de station pour l'export CSV mais on les lit quand même
	# stations est passé en paramètre à hmvl2pg pour ne pas être lu n fois
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
		labocom2pg(jour,str(rep),pwd)
	print(datetime.datetime.now().time())

