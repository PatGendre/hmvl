#!/usr/bin/env python

"""hmvl-RD2csv.py: conversion de fichiers hmvl VRU Marius6 secondes en csv, cf. github.com/patgendre/hmvl """

# patrick gendre 24/03/20
# 
#import click
import datetime
import csv

#@click.command()
#@click.option("--f", default="RD297_200", help="Nom du fichier hmvl VRU RDxxx.")
def lirehmvl2csv(f):
	# seul paramètre : le nom du fichier
	# il faudrait ajouter un paramètre pour le répertoire d'écriture
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
	with open(f+".csv",'w') as fcsv:
		header=["dt_texte","dt_unix","station","status","voie","vitesse","longueur","statutTR"]
		fwriter = csv.writer(fcsv, delimiter=',', quotechar='"')
		fwriter.writerow(header)
		for mesure in liste_mesures:
			fwriter.writerow(mesure)
	print(datetime.datetime.now()+" Export CSV de "+str(len(liste_mesures))+" mesures.")

