import pandas as pd
import matplotlib
import psycopg2
host="localhost"
port=5432
dbname="hmvl"
username="dirmed"
pwd="marius"
conn = psycopg2.connect("host='{}' port={} dbname='{}' user={} password={}".format(host, port, dbname, username, pwd))
sql = "select * from hmvl;"
hmvl = pd.read_sql_query(sql, conn)
conn = None
hmvl["hdt"] = pd.to_datetime(hmvl["hdt"])

hmvl["voie"].value_counts() 
hmvl["statustr"].value_counts() 

vitesse=hmvl["hdt","vitesse","station"]