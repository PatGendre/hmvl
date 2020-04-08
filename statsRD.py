#!/usr/bin/env python

"""statsRD.py: lecture des répertoires RD d'un jour donné"""

# patrick gendre 08/04/20
# import click
import os
import datetime
import csv
import pandas as pd

def statsRD(jour="2020-04-02",heure="10-30",rdc="rdc_0"):
	path=jour+"/"+heure+"/"+rdc
	RD=os.listdir(path)
	n=len(RD)
	#n*jour
	sansPAS="RD_PAS" not in RD
	if not sansPAS:
		with open(path+"/RD_PAS","r") as RD_PAS:
			sansPAS=RD_PAS.readline()
	pasRD=[x[0:2]!="RD" for x in RD]
	ext100=[x[-4:]=="_100" for x in RD]
	ext200=[x[-4:]=="_200" for x in RD]
	extbad=[x[-4:]==".bad" for x in RD]
	extbadlx=[x[6:13]=="_BadIx_" for x in RD]
	lines=[len(open(path+"/"+x).readlines()) for x in RD]
	df = pd.DataFrame({'pasRD': pasRD, 'ext100': ext100, 'ext200': ext200,
		'extbad': extbad, 'extbadix': extbadix, 'lines': lines})
	return  df


def statsRD(jour="2020-04-02",heure="10-30"):
	for rep in os.listdir(jour):
		RD=os.listdir(jour+"/"+rep)
		n=len(RD)
		#n*jour
		path=jour+"/"+rep+"/"
		sansPAS="RD_PAS" not in RD
		is not sansPAS:
			with open(jour+"/"+RD+"/RD_PAS","r") as RD_PAS
				sansPAS=readline(RD_PAS)
		pasRD=[x[0:2]!="RD" for x in RD]
		ext100=[x[-4:]=="_100" for x in RD]
		ext200=[x[-4:]=="_200" for x in RD]
		extbad=[x[-4:]==".bad" for x in RD]
		extbadix=[x[6:13]=="_Badlx" for x in RD]
		lines=[len(open(path+x).readlines() for x in RD)]

