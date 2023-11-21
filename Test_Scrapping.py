import pandas as pd
from requests import get

link = r'https://www2.bmf.com.br/pages/portal/bmfbovespa/boletim1/Ajustes1.asp?txt'+f'Data={pd.to_datetime("2023-11-17").date().strftime(r"%d/%m/%y")}' # m/d/y

Raw = get(link, headers={'user-agent': 'Mozilla/5.0'})

x = pd.read_html(Raw.content, decimal=',', thousands='.')
x = pd.concat(x)
x = x
x.to_csv('Teste.csv')
x.columns = x.iloc[18]
print(pd.to_datetime(x.iloc[17][0].split(': ')[1], dayfirst = True).date()))
x = x.iloc[19:]
x['Mercadoria'] = x['Mercadoria'].ffill().str.split(' - ').apply(lambda x: x[0])
x = x.set_index('Mercadoria')
x.to_csv('Teste.csv')
