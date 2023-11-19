import pandas as pd
import yfinance as yf
import datetime as dt
from pandas_datareader import data as web
import bs4
from numpy import array, ndarray

yf.pdr_override()

class Prices:
    def __init__(self, ativos = None, start = dt.datetime.today().date(), end = dt.datetime.today().date()):
        '''
        Introdução ao código
        
        Ativos: recomendado lista
        '''
        
        if type(ativos) in [type([]), type(array([]))]:
            self.ativos = ativos
        else:
            raise ValueError('Adicione um ativo.')
        
        self.start = start
        self.end = end
        
    def get_tickers_yf(self):
        return web.get_data_yahoo(self.ativos, self.start, self.end)['Adj Close']   
  
    def get_fut_data(self):
        return 0
    
    def get_titulos_br(self):
        return 0
    
    def get_currency(self):
        return 0
    
    def get_cdi(self):
        DI = f'https://api.bcb.gov.br/dados/serie/bcdata.sgs.11/dados?formato=csv&dataInicial={(pd.to_datetime(self.start).strftime("%d/%m/%y"))}&dataFinal={(pd.to_datetime(self.end).strftime("%d/%m/%y"))}'
        Download = pd.read_csv(DI)
        Download.columns = ['DI']
        Download.index = Download.index.str.rstrip(f';"0')
        Download.index = pd.to_datetime(Download.index, dayfirst=True)
        Download['DI'] = Download['DI'].str.rstrip(f'"').astype(int)/100000000
        return Download


if __name__ == '__main__':
    x = Prices([]).get_cdi()
    print(x)
