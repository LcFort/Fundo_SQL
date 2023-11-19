from bs4 import BeautifulSoup as soup
from time import sleep
import pandas as pd
# from urllib import request
from requests import get
import os

def pr():
    print({'SEX':[["DSAD4",'DSADAS3'],['SDSA1',"DASD2"]]})
    for i in range(0,1):
        break
    return 0

pr()

link = r'https://www2.bmf.com.br/pages/portal/bmfbovespa/boletim1/SistemaPregao1.asp?pagetype=pop&caminho=Resumo%20Estat%EDstico%20-%20Sistema%20Preg%E3o&Data=&Mercadoria=DI1'

Raw = get(link, headers={'user-agent': 'Mozilla/5.0'})
Page = soup(Raw.content, 'html.parser')

# lista_1 = Page.findAll('td', {'id':'MercadoFut0'})
# # lista_tr = lista_1.findAll('tr')

# for i in lista_1:
#     x = i.findAll('table')
#     print(type(i), i.text)
#     for i in x:
#         print(i)

# lista_1 = Page.find_all('tbody')

# with open('DSD.txt', 'w', encoding='utf-8') as arq:
#     for i in Page:
#         arq.write(str(i))
#     for i in lista_1:
#         arq.write(i)

x = pd.read_html(Raw.content, decimal=',', thousands='.')
x = pd.concat(x)
x.to_csv('S.csv')
# print(x[x.columns[-3]:x.columns[-1]])

# for i in y:
#     print(str(i)+'\n\n\n')

# with open('Saves.txt', 'w', encoding='utf-8') as arq:
#     arq.write(str(Rqst.text))
