#!/usr/bin/env python

"""hmvl-RD2pg.py: lecture de fichiers hmvl VRU Marius6 secondes et écriture dans une BD pg, cf. github.com/patgendre/hmvl """

# patrick gendre 03/04/20
# 
import click
import os
import datetime
import csv
import psycopg2

@click.command()
@click.option("--f", default="RD297_200", help="Nom du fichier hmvl VRU RDxxx.")
@click.option("--u", default="dirmed",help="Nom de l'utilisateur postgres")
@click.option("--p",help="mot de passe")
# on se connecte sur localhost à une BD posgtres existante hmvl ; cela pourrait être paramétrable
def lirehmvl(f,u,p):
	with open(f,'r') as ff:
		# header : horodate en clair et en temps unix
		dt_texte=ff.readline()[:-1]
		dt_unix0=datetime.datetime.fromtimestamp(float(ff.readline()[0:10]))
		# connexion à postgres
		connection = psycopg2.connect(user=u, password=p, host="127.0.0.1", \
			port="5432", database="hmvl")
		cursor = connection.cursor()
		postgres_insert_query = "INSERT INTO hmvl (horodate_id,hdt,station,status,voie,vitesse,longueur) VALUES (%s,%s,%s,%s,%s,%s,%s)"
		# une ligne par trame hmvl
		for ligne in ff:
			indexstn=ligne[0:4]
			etatstn=ligne[4]
			n=(len(ligne)-6)//11
			# pour une ligne sans mesure, on garde traces des trames vides, pour des diagnostics
			if n==0:
				mesure = (dt_texte,dt_unix0.isoformat(),indexstn,etatstn,None,None,None)
				cursor.execute(postgres_insert_query, mesure)
				connection.commit()
			for i in range(n):
				c11=ligne[6+i*11:17+i*11]
				numvoie=c11[0]
				dt_unix=dt_unix0+datetime.timedelta(seconds=int(c11[1:3]),microseconds=int(c11[3:5])*10000)
				vitesse=c11[5:8]
				if vitesse=='   ' or vitesse=="":
					vitesse=None
				else:
					print (vitesse+ " l:"+str(len(vitesse)))
					vitesse=float(vitesse)
				longueur=c11[8:11]
				if longueur=='' or longueur=="   ":
					longueur=None
				else:
					longueur=float(longueur)*0.1
				mesure = (dt_texte,dt_unix.isoformat(),indexstn,etatstn,numvoie,vitesse,longueur)
				cursor.execute(postgres_insert_query, mesure)
				connection.commit()

if __name__ == "__main__":
	lirehmvl()

