#!/usr/bin/env python

"""jourhmvl2csv.py: lecture de fichiers du répertoire d'un jour hmvl VRU Marius6 secondes et écriture dans une BD pg, cf. github.com/patgendre/hmvl """

# patrick gendre 14/05/20
# 
import datetime
import csv
import click
import pathlib
import arrow
import psycopg2

# pour utiliser : copier coller le code dans une console python OU
# ligne de commande pour la fonction jourhmvl2pg
# on verra ensuite comment gérer proprement l'import de code/ les package 

def hmvl2csv(f,nomcsv,nomlog=None,stations=None,a_or_w='w'):
	#f string du nom du fichier (avec chemin)
	#nomcsv string du fichier csv à créer (a_or_w="a") ou auquel ajouter à la fin (a_or_w="a")
	#   le contenu du fichier hmvl
	# si en mode append "a", il faut que le fichier nomcsv existe!
	# nomlog : string du chemin vers le fichier log, peut être vide
	#     à utiliser uniquement avec jourhmvl2csv, en mode append
	# stations : utilisé uniquement avec jourhmvl : 
	#     dict qui traduit le code station en adresse rgs à 3 lettres
	#     si stations est None (export individuel d'un fichier CSV) ATTENTION
	#        l'export csv contiendra le code station (entier) pas l'adresse rgs
	#        donc ne pourra pas être directement importé en base hmvl
	if nomcsv is None: nomcsv=f+".csv"
	if a_or_w!='w': a_or_w='a'
	with open(f,'r') as ff:
		dt_texte=ff.readline()
		if dt_texte=='':
			print("ATTENTION fichier VIDE: "+f)
			return
		else:
			dt_texte=dt_texte[:-1]
		dt_unix0=datetime.datetime.fromtimestamp(float(ff.readline()[0:10]))
		liste_mesures=[]
		chars = set('0123456789 *@BbMABDHJ')
		# lecture du fichier, une ligne par trame hmvl
		for ligne in ff:
			indexstn=int(ligne[0:4])
			if not(stations is None):
				if indexstn not in stations:
					#print("WARNING: station "+str(indexstn)+ " pas trouvée!")
					indexstn=ligne[0:4]
				else:
					indexstn=stations[indexstn]
			etatstn=ligne[4]
			# on suppose que le * est à supprimer (reprise de connexion https://github.com/PatGendre/hmvl/issues/16)
			reponse=ligne[6:-1]
			reponse=reponse.replace('*','')
			n=len(reponse)//11
			# pour une ligne sans mesure, on garde traces des trames vides, pour des diagnostics
			# on sauve aussi le statut du dernier caractère de la ligne (absent si la trame est vide, 5ème caractère à 2)
			# le dernier caractère étant \n, c'est en fait l'avant dernier qu'il faut lire
			statutTR=ligne[-2:-1]
			if etatstn=="2": statutTR=None
			if n==0:
				mesure = (dt_texte,dt_unix0.isoformat(),indexstn,etatstn,None,None,None,statutTR)
				liste_mesures.append(mesure)
			if not all((c in chars) for c in reponse):
				# chaine avec des caractères interdits:
				print ("CARACTERES INTERDITS DANS LA TRAME "+reponse)
				continue
				# on passe au suivant
			reponse=reponse.replace('*','')
			for i in range(n):
				c11=reponse[i*11:17+i*11]
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
					longueur=round(float(int(longueur))*0.1,1)
				mesure = (dt_texte,dt_unix.isoformat(),indexstn,etatstn,numvoie,vitesse,longueur,statutTR)
				liste_mesures.append(mesure)
	with open(nomcsv,a_or_w) as fcsv:
		header=["dt_texte","dt_unix","station","status","voie","vitesse","longueur","statutTR"]
		fwriter = csv.writer(fcsv, delimiter=',', quotechar='"')
		if a_or_w=='w':
			fwriter.writerow(header)
		for mesure in liste_mesures:
			fwriter.writerow(mesure)
	if not(nomlog is None):
		if pathlib.Path(nomlog).exists:
			with open(nomlog,"a") as flog:
				fw=csv.writer(flog,delimiter=',', quotechar='"')
				fw.writerow((f,datetime.datetime.now(),len(liste_mesures)))
	print(f+" Export CSV de "+str(len(liste_mesures))+" mesures.")

def labocom2csv(jour,f,nomcsv,nomlog=None,a_or_w='w'):
	if nomcsv is None: nomcsv=f+".csv"
	if a_or_w!='w': a_or_w='a'
	# jour: string "AAAA-MM-JJ" nécessaire car les fichiers CSV ne contiennent pas l'indication de la date
	# f: string du nom du fichier (avec chemin)
	# nomcsv string du fichier csv à créer (a_or_w="a") ou auquel ajouter à la fin (a_or_w="a")
	#   le contenu du fichier labocom
	# si en mode append "a", il faut que le fichier nomcsv existe!
	# nomlog : string du chemin vers le fichier log, peut être vide
	#     à utiliser uniquement avec jourhmvl2csv, en mode append

	fichier=pathlib.Path(f)
	# on suppose que f est effectivement un fichier csv au format labocom!
	# testé aussi avant l'appel par replabocom2csv
	x= str.split(fichier.name,'_')
	if len(x)!=3:
		print (fichier.name + " n'a pas un nom attendu, on ne le lit pas.")
		return
	if x[2][-4:]!=".csv":
		print (fichier.name + " n'a pas un nom attendu, on ne le lit pas.")
		return
	# ATTENTION  on suppose que les noms RGS labocom sont en MAJUSCULES???
	rgs=str.upper(x[1])
	jj=str.upper(x[0])
	liste_mesures=[]
	chars = set('0123456789 *@BbMADHJT:')
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
			if not all((c in chars) for c in reponse):
				# chaine avec des caractères interdits:
				print ("CARACTERES INTERDITS DANS LA TRAME "+reponse)
				for c in reponse:
					if not(c in chars):
						print(c)				
				continue
				# on passe au suivant

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
					longueur=round(float(int(longueur))*0.1,1)
				if dt_unix is None:
					continue
					# le champ hdt en base est un TIMESTAMPTZ NOT NULL, on n'enregistre la ligne vide
				else:
					mesure = (dt_texte,dt_unix.isoformat(),indexstn,etatstn,numvoie,vitesse,longueur,statutTR)
					liste_mesures.append(mesure)
	with open(nomcsv,a_or_w) as fcsv:
		header=["dt_texte","dt_unix","station","status","voie","vitesse","longueur","statutTR"]
		fwriter = csv.writer(fcsv, delimiter=',', quotechar='"')
		# on n'ajoute pas l'en-tête si on en est mode append "a"
		if a_or_w=='w':
			fwriter.writerow(header)
		for mesure in liste_mesures:
			fwriter.writerow(mesure)
	if not(nomlog is None):
		if pathlib.Path(nomlog).exists:
			with open(nomlog,"a") as flog:
				fw=csv.writer(flog,delimiter=',', quotechar='"')
				fw.writerow((f,datetime.datetime.now(),len(liste_mesures)))
	print(f+" Export CSV de "+str(len(liste_mesures))+" mesures.")

def replabocom2csv(jour,rep,nomcsv,nomlog):
	if nomcsv is None: nomcsv=f+".csv"
	# lecture de tous les fichiers de mesures individuelles au format CSV Labocom pour un jour donné
	# cf. le wiki https://github.com/PatGendre/hmvl/wiki/Fichiers-Labocom-(autres-stations)/
	# avec des fichiers dans un sous-répertoire AAAA-MM-JJ situé dans racine/Labocom
	# jour string "AAAA-MM-JJ"
	# rep sring du chemin vers le répertoire contenant les données
	# nomcsv string du chemin du fichier vers lequel ajouter 'en mode append' les données labocom
	# nomlog string du chemin listant les fichiers ajoutés au csv 
	# pwd : mot de passe d'accès à la base hmvl sur postgresql
	# actuellement il y a 8 stations labocom à la DIRMED, et un fichier par jour et par station
	chemin=pathlib.Path(rep)
	if not chemin.is_dir():
		print(rep+" n'existe pas.")
		return
	print ("répertoire "+rep)
	print("nombre de fichiers trouvés: "+str(len(list(chemin.glob('**/*')))))
	print(datetime.datetime.now().time())
	for fichier in list(chemin.glob('**/*')):
		labocom2csv(jour,str(fichier),nomcsv,nomlog=nomlog,a_or_w='a')

def rephmvl2csv(rep,nomcsv,nomlog,stations):
# lecture d'un répertoire HH-MM de fichiers au format hmvl
# rep : string chemin complet nom du sous répertoire rdc_0 ou rdc_1/jour/HH-MM
# nomcsv : string du chemin vers le fichier csv
# le fichier nomcsv est supposé exister 
# nomlog : string du chemin vers le fichier log
#   et on lui ajoute tous les csv générés à partir des fichiers hmvl RD pour le répertoire rep
# stations : utilisé uniquement avec jourhmvl : 
#     dict qui traduit le code station en adresse rgs à 3 lettres
#     si stations est None (export individuel d'un fichier CSV) ATTENTION
#        l'export csv contiendra le code station (entier) pas l'adresse rgs
#        donc ne pourra pas être directement importé en base hmvl
	chemin=pathlib.Path(rep)
	if not chemin.exists:
		print(str(chemin)+" n'existe pas.")
		return
	print ("répertoire "+rep)
	print("nombre de fichiers trouvés: "+str(len(list(chemin.glob('**/RD*')))))
	print(datetime.datetime.now().time())
	# on ne teste pas s'il y a des fichiers qui ne commencent pas par RD
	for fichier in list(chemin.glob('**/RD*')):
		nomfichier=fichier.name
		# le nom de fichier est en principe RDxxx_100 ou 200
		if len(nomfichier)!=9:
			print("Fichier "+nomfichier+ " ignoré")
			continue
		# pas d'extension
		if "." in nomfichier or nomfichier[:2]!="RD" or not(nomfichier[-4:] in ["_100","_200"]):
			print(nomfichier+": doublon ou mauvais type de fichier")
			continue
		hmvl2csv(str(fichier),nomcsv,nomlog=nomlog,stations=stations,a_or_w='a')

@click.command()
@click.option('--jour', prompt='jour:', help='string HHHH-MM-AA')
@click.option('--nomcsv', prompt='Nom du fichier csv:', help='Nom du fichier csv à créer')
@click.option('--nomlog', prompt='Nom du fichier log:', help='Nom du fichier log à créer (optionnel)')
@click.option('--pwd', prompt='Mdp base hmvl:', help='mot de passe postgresql')
@click.option('--racine', prompt='Répertoire des données hmvl', 
	default="..", help='Répertoire des données hmvl: . , .. , "../MM-AA" etc.')

def jourhmvl2csv(jour,nomcsv,nomlog,pwd,racine=".."):
# jour "AAAA-MM-JJ"
# nomcsv : string du fichier csv (avec chemin) à créer pour les données de la journée jour
# nomlog : string du chemin vers le fichier log
# racine : répertoire d'où partir pour trouver les donneés rdc_0, rdc_1, labocom avec un sous-répertoire par jour
	print(datetime.datetime.now().time())
	path0=pathlib.Path(racine)
	if len(jour)!= 10:
		print("format de date d'entrée: AAAA-MM-JJ")
		return
	if not pathlib.Path.exists(path0 / "rdc_0" / jour):
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
	# en-tête du fichier log
	if not(nomlog is None):
		if pathlib.Path(nomlog).exists:
			print("ATTENTION : le fichier log existe,"+nomlog+" on va écrire à la fin fichier")
		else:
			with open(nomlog,"w") as flog:
				fw=csv.writer(flog,delimiter=',', quotechar='"')
				fw.writerow(("fichier","horodate_exportcsv","nb_mesures"))
	# boucle sur les répertoires du jour 'HH-MM'
	header=["dt_texte","dt_unix","station","status","voie","vitesse","longueur","statutTR"]
	with open(nomcsv,"w") as fcsv:
		fwriter = csv.writer(fcsv, delimiter=',', quotechar='"')
		fwriter.writerow(header)
	for rep in list((path0 / "rdc_0" / jour).glob('*')):
		print(datetime.datetime.now().time())
		rephmvl2csv(str(rep),nomcsv,nomlog,stations)
	for rep in list((path0 / "rdc_1" / jour).glob('*')):
		print(datetime.datetime.now().time())
		rephmvl2csv(str(rep),nomcsv,nomlog,stations)
	replabocom2csv(jour,str(path0/"Labocom"/jour),nomcsv,nomlog)
	print(datetime.datetime.now().time())

if __name__ == '__main__':
	jourhmvl2csv()
