#!/usr/bin/env python

"""calculs_hmvl.py: lecture d'un fichier csv issu d'une lecture de données HMVL DIRMED (RD/Labocom) et exports des indicateurs produits chaque jour"""
# enchainements en ligne de commande des calculs quotidiens à effectuer avec les données hmvl de la DIRMED
# cf. https://github.com/patgendre/hmvl/wiki
# patrick gendre 31/08/20

import arrow
import psycopg2
import pandas as pd
import numpy as np
from dateutil import tz
import click
import scriptindic as scr 
import pathlib


@click.command()
@click.option('--nomfichier', prompt='Nom du fichier csv:', help='Nom du fichier csv à lire')

def calculs_hmvl(nomfichier):
	hmvl=scr.lirecsvhmvl(nomfichier)
	print(nomfichier+" lu.")
	p=pathlib.Path(nomfichier)
	# on écrit tous les fichiers de la journée dans le même répertoire que le fichier csv de départ avec un préfixe différent
	print("calcul des indicateurs de la journée.")
	qualite=scr.indicqualite(hmvl)
	scr.tocsv(qualite,str(p.parent/('indiq-'+p.name)))
	print("calcul des moyennes 6 minutes.")
	moy6=scr.agreg6(hmvl)
	scr.tocsv(moy6,str(p.parent/('moy6-'+p.name)))
	print("calcul des distances et temps inter-véhiculaires par heure et par voie.")
	tdiv1H=scr.agregtdiv1H(hmvl)
	scr.tocsv(tdiv1H,str(p.parent/('tdiv1H-'+p.name)))
	print("messages d'alertes sur la qualité du recueil.")
	texte=scr.alertes(qualite)
	with open(str(p.parent/('alertes-'+p.name)), 'w') as out_file:
		out_file.write(texte)
if __name__ == '__main__':
	calculs_hmvl()