#!/usr/bin/env python

"""hmvl-RD2csv.py: conversion de fichiers hmvl VRU Marius6 secondes en csv, cf. github.com/patgendre/hmvl """

# patrick gendre 24/03/20
# 
import click
import os
import datetime
import csv

@click.command()
@click.option("--f", default="RD297_200", help="Nom du fichier hmvl VRU RDxxx.")
def lirehmvl(f):
	with open(f,'r') as ff, open(f+".csv",'w') as fcsv:
		header=["dt_texte","dt_unix","indexstn","status","voie","vitesse","longueur"]
		fwriter = csv.writer(fcsv, delimiter=',', quotechar='"')
		fwriter.writerow(header)
		# header : horodate en clair et en temps unix
		dt_texte=ff.readline()[:-1]
		dt_unix0=datetime.datetime.fromtimestamp(float(ff.readline()[0:10]))
		# une ligne par trame hmvl
		for ligne in ff:
			indexstn=ligne[0:4]
			etatstn=ligne[4]
			n=(len(ligne)-6)//11
			# pour une ligne sans mesure on garde traces des trames vides, pour des diagnostics
			if n==0:
				mesure = [dt_texte,dt_unix0.isoformat(),indexstn,etatstn,"","",""]
				fwriter.writerow(mesure)
			for i in range(n):
				c11=ligne[6+i*11:17+i*11]
				numvoie=c11[0]
				dt_unix=dt_unix0+datetime.timedelta(seconds=int(c11[1:3]),microseconds=int(c11[3:5])*10000)
				v=c11[5:8]
				l=c11[8:11]
				mesure=[dt_texte,dt_unix.isoformat(),indexstn,etatstn,numvoie,v,l]
				fwriter.writerow(mesure)

if __name__ == '__main__':
	lirehmvl()

