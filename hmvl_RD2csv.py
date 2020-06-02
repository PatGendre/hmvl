#!/usr/bin/env python

"""hmvl-RD2csv.py: conversion de fichiers hmvl VRU Marius6 secondes en csv, cf. github.com/patgendre/hmvl """

# patrick gendre 24/03/20

# en principe fonctionne encore mais OBSOLETE ET PLUS MIS A JOUR:
# utiliser plutôt jourhmvl2csv.py puis importer en base par un COPY
# 
#import click
import datetime
import csv

#@click.command()
#@click.option("--f", default="RD297_200", help="Nom du fichier hmvl VRU RDxxx.")
def hmvl2csv(f,nomcsv,a_or_w='w'):
	# seul paramètre : le nom du fichier
	# il faudrait ajouter un paramètre pour le répertoire d'écriture
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
		# lecture du fichier, une ligne par trame hmvl
		for ligne in ff:
			indexstn=ligne[0:4]
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
	with open(nomcsv,a_or_w) as fcsv:
		header=["dt_texte","dt_unix","station","status","voie","vitesse","longueur","statutTR"]
		fwriter = csv.writer(fcsv, delimiter=',', quotechar='"')
		if a_or_w=='w':
			fwriter.writerow(header)
		for mesure in liste_mesures:
			fwriter.writerow(mesure)
	print(str(datetime.datetime.now())+" Export CSV de "+str(len(liste_mesures))+" mesures.")

def labocom2csv(jour,nomcsv,a_or_w='w'):
	# seul paramètre : le nom du fichier
	# il faudrait ajouter un paramètre pour le répertoire d'écriture
	if nomcsv is None: nomcsv=f+".csv"
	if a_or_w!='w': a_or_w='a'
	# lecture de tous les fichiers de mesures individuelles au format CSV Labocom pour un jour donné
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
		if a_or_w=='w':
			nomf=nomcsv
		else:
			nomf=nomcsv
		with open(nomf,a_or_w) as fcsv:
			header=["dt_texte","dt_unix","station","status","voie","vitesse","longueur","statutTR"]
			fwriter = csv.writer(fcsv, delimiter=',', quotechar='"')
			if a_or_w=='w':
				fwriter.writerow(header)
			for mesure in liste_mesures:
				fwriter.writerow(mesure)
		print(str(datetime.datetime.now())+" Export CSV de "+str(len(liste_mesures))+" mesures.")
