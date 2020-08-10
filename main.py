import hashlib
from pathlib import Path
from sincrawl.implementa import RodaRDH
from sincrawl.misc_sin.spiders.a_dona_aranha import RDH
import pandas as pd
from xlrd.biffh import XLRDError
from os import unlink
import psycopg2


def download():
    aranha = RodaRDH()
    aranha.roda()


def localiza_plan():
    locais = []
    rdh = RDH()
    urls, datas = rdh.get_site()
    for url in urls:
        h = hashlib.sha1()
        h.update(url.encode('utf-8'))
        locais.append(Path('full/%s.xlsx' % h.hexdigest()))
    return locais, datas


def trata_rdh(local, data):
    rdh = pd.read_excel(local, header=3, usecols="E,N:AA")
    rdh.loc[0, 'POSTO'] = 'x'
    rdh.loc[1, 'POSTO'] = 'y'
    rdh.loc[2, 'POSTO'] = 'z'

    rdh.set_index('POSTO', inplace=True)
    for i, col in rdh.iteritems():
        if i == 'Unnamed: 13':

            rdh.loc['x', i] = str(rdh.loc['x', i]) + ' ' + \
                str(rdh.loc['z', i])

        else:

            rdh.loc['x', i] = str(rdh.loc['x', i]) + ' ' + \
                str(rdh.loc['y', i]) + ' ' + str(rdh.loc['z', i])

    rdh.columns = rdh.loc['x', :]
    rdh.drop(['x', 'y', 'z',
              "% MLT - Percentual da vazão média de longo termo do mês correspondente.",
              "NIVEL RES. - Nivel d'água de montante (reservatório), às 24h.",
              "ARM. - Percentual de volume útil armazenado no reservatório, às 24h.",
              "ESP. -  Percentual de volume útil máximo para o dia, com vistas à operação ",
              "             de controle de cheias.",
              "ND",
              "TUR. - Vazão turbinada média diária.",
              "VER. - Vazão vertida média diária.",
              "OTR. - Vazão restituída ao rio a jusante, por meio de outras estruturas."
              ], axis=0, inplace=True)
    rdh['Data'] = data.strftime("%Y-%m-%d")
    rdh.set_index('Data', append=True, inplace=True)

    return rdh


download()
locais, datas = localiza_plan()
try:
    rdh = trata_rdh(locais[0], datas[0])
except XLRDError:
    unlink(locais[0])
    rdh = trata_rdh(locais[1], datas[1])
    locais.pop(1)
    datas.pop(1)
finally:
    locais.pop(0)
    datas.pop(0)


for i, local in enumerate(locais):
    aux = trata_rdh(local, datas[i])
    rdh = pd.concat([rdh, aux], axis=0)

rdh.fillna(0, inplace=True)


rdh.to_csv('rdh.csv')


conn = psycopg2.connect(
    "dbname='ENERGIA' host='192.168.0.251' user='script' password='batata'")
cur = conn.cursor()

cur.execute("delete from rdh;")

with open("rdh.csv", 'r') as fp:

    header = fp.readline()
    cur.copy_from(fp, "rdh", ",", "-")


cur.execute("insert into rdh_base\
                select * from rdh\
            on conflict (posto,dia) do nothing")
conn.commit()
cur.close()
conn.close()
