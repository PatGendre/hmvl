#!/usr/bin/env python

"""jourhmvl2pg.py: lecture de fichiers du répertoire d'un jour hmvl VRU Marius6 secondes et écriture dans une BD pg, cf. github.com/patgendre/hmvl """

# patrick gendre 07/04/20

# en principe fonctionne encore mais OBSOLETE ET PLUS MIS A JOUR !! :
# utiliser plutôt jourhmvl2csv.py puis importer en base par un COPY
# 
import click
import pathlib
import datetime
import arrow
import csv
import psycopg2
import psycopg2.extras
# cf. tentative d'optimisation https://www.psycopg.org/docs/extras.html#fast-execution-helpers
# import hmvlRD2pg

# pour utiliser : copier coller le code dans une console python OU
# ligne de commande pour la fonction jourhmvl2pg
# on verra ensuite comment gérer proprement l'import de code/ les package 

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
		dt_unix0=arrow.get(ff.readline()[0:10]).datetime
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
			n=(len(ligne)-8)//11
			# pour une ligne sans mesure, on garde traces des trames vides, pour des diagnostics
			# on sauve aussi le statut du dernier caractère de la ligne (absent si la trame est vide, 5ème caractère à 2)
			# le dernier caractère étant \n, c'est en fait l'avant dernier qu'il faut lire
			statutTR=ligne[-2:-1]
			if etatstn=="2": statutTR=None
			if n<=0:
				mesure = (dt_texte,dt_unix0.isoformat(),indexstn,etatstn,None,None,None,statutTR)
				liste_mesures.append(mesure)
				continue
			# on suppose que le * est à supprimer (reprise de connexion https://github.com/PatGendre/hmvl/issues/16)
			ligne=ligne.replace('*','')
			# tester que n(len(ligne)-7)%11=0 càd que les trames comptent bien nx11 caractères
			if ((len(ligne)-8)%11)!=0:
				print("ERREUR TRAME pas multiple de 11 caractères")
				print(ligne)
				continue
			# lecture des trames "normales"
			for i in range(n):
				c11=ligne[6+i*11:17+i*11]
				numvoie=c11[0]
				dt_unix=dt_unix0+datetime.timedelta(seconds=int(c11[1:3]),microseconds=int(c11[3:5])*10000)
				vitesse=c11[5:8]
				if vitesse=='   ' or vitesse=="":
					vitesse=None
					continue
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
		print(str(arrow.utcnow())+" Insertion de "+str(nbmes)+" lignes pour "+f)
		cursor = connection.cursor()
		# on suppose qu'il n'y aura pas eu d'écritures simultanées dans la base!
		cursor.execute(postgres_insert_query,(f,datetime.datetime.now(),nbmes,maxid+1,maxid+len(liste_mesures)))
		connection.commit()
	cursor.close()
	connection.close()

def labocom2pg(jour,racine,pwd,u="dirmed",log=True):
	# lecture des fichiers de mesures individuelles au format CSV Labocom pour un jour donné
	# cf. le wiki https://github.com/PatGendre/hmvl/wiki/Fichiers-Labocom-(autres-stations)/
	# avec des fichiers dans un sous-répertoire AAAA-MM-JJ situé dans racine/Labocom
	# pwd : mot de passe d'accès à la base hmvl sur postgresql
	# actuellement il y a 8 stations labocom à la DIRMED, et un fichier par jour et par station
	racine=pathlib.Path(racine)
	if not racine.is_dir():
		print(str(racine)+" n'existe pas.")
		return
	rep = racine / "Labocom" / jour
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
				dta,dtm,dtj=str.split(jour,'-')
				# attention l'heure dans les csv labocom est en heure locale pas en UTC comme le timestamp des fichiers RD
				dth,dtmi,dts=str.split(row['HORODATE'],':')
				dt=datetime.datetime(int(dta),int(dtm),int(dtj),int(dth),int(dtmi),int(dts))
				#dt_unix0=datetime.datetime.strptime(dt_texte,"%Y-%m-%d %H:%M:%S")
				dt_unix0=arrow.Arrow.fromdatetime(dt,"Europe/Paris")
				dt_texte=dt_unix0.format(fmt='YYYY-MM-DD HH:mm:ssZZ', locale='fr')
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
				# on suppose que le * est à supprimer (reprise de connexion https://github.com/PatGendre/hmvl/issues/16)
				reponse=reponse.replace('*','')
				if ((len(reponse)-3)%11)!=0:
					print("ERREUR : ligne avec un nb de caractères non multiple de 11")
					print(reponse)
					continue
				n=(len(reponse)-3)//11
				if n<=0:
					mesure = (dt_texte,dt_unix0.isoformat(),indexstn,etatstn,None,None,None,statutTR)
					liste_mesures.append(mesure)
					continue
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
@click.command()
@click.option('--jour', prompt='jour:', help='string HHHH-MM-AA')
@click.option('--pwd', prompt='Mot de passe BD:', help='Password base hmvl')
@click.option('--racine', prompt='Répertoire', default="..", help='Exemple . , .. , "../MM-AA" etc.')
@click.option('--exportcsv', default=False, help="True si on export des fichiers csv au lieu d'importer en base")
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
		path_rdc=path0 / rdc / jour / rep
		if not path_rdc.exists:
			print(str(path_rdc)+" n'existe pas.")
			return
		print ("répertoire "+rep)
		print("nombre de fichiers trouvés: "+str(len(list(path_rdc.glob('**/RD*')))))
		# on ne teste pas s'il y a des fichiers qui ne commencent pas par RD
		for fichier in list(path_rdc.glob('**/RD*')):
			nomfichier=fichier.name
			# le nom de fichier est en principe RDxxx_100 ou 200
			if len(nomfichier)!=9:
				print("Fichier "+nomfichier+ " ignoré")
				continue
			# pas d'extension
			if "." in nomfichier or nomfichier[:2]!="RD" or not(nomfichier[-4:] in ["_100","_200"]):
				print(nomfichier+": doublon ou mauvais type de fichier")
				continue
			# temporaire : nom "en dur" jour+".csv" pour le test d'export csv d'un jour
			if exportcsv:
				hmvl2csv(str(fichier),jour+".csv",a_or_w='a')
			else:
				hmvl2pg(str(fichier),"dirmed",pwd,stations,True)
	path0=Path(racine)
	if len(jour)!= 10:
		print("format de date d'entrée: AAAA-MM-JJ")
		return
	if not Path.exists(path0 / "rdc_0" / jour):
		print("Le répertoire rdc_0/"+jour+" n'existe pas.")
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
	if exportcsv:
		header=["dt_texte","dt_unix","station","status","voie","vitesse","longueur","statutTR"]
		with open(jour+".csv","w") as fcsv:
			fwriter = csv.writer(fcsv, delimiter=',', quotechar='"')
			fwriter.writerow(header)
	for rep in list((path0 / "rdc_0" / jour).glob('*')):
		print(datetime.datetime.now().time())
		lirerdc(jour,rep.name,pwd,"rdc_0",stations,racine,exportcsv)
	for rep in list((path0 / "rdc_1" / jour).glob('*')):
		print(datetime.datetime.now().time())
		lirerdc(jour,rep.name,pwd,"rdc_1",stations,racine,exportcsv)
	if exportcsv:
		labocom2csv(jour,jour+".csv",racine="..",a_or_w='a')
	else:
		labocom2pg(jour,racine,pwd)
	print(datetime.datetime.now().time())

if __name__ == '__main__':
	jourhmvl2pg()
