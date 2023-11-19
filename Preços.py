import pandas as pd
import yfinance as yf
import datetime as dt
from pandas_datareader import data as web
import bs4

yf.pdr_override()

class Prices:
    def __init__(self, ativos = None, start = dt.datetime.today().date(), end = dt.datetime.today().date()):
        '''
        Introdução ao código
        
        Ativos: {ticker: tipo}
        '''
        self.ativos = ativos
        if self.ativos == None:
            raise ValueError('Adicione um ativo.')
        
        self.start = start
        self.end = end
        
    def get_tickers_brazil(self):
        return web.get_data_yahoo(self.ativos, self.start, self.end)['Adj Close']
    
    def get_tickers_us(self):
        return 0
    
    def get_fut_data(self):
        return 0
    
    def get_titulos_br(self):
        return 0
    
    def get_currency(self):
        return 0
    
    def get_cdi(self):
        DI = f'https://api.bcb.gov.br/dados/serie/bcdata.sgs.11/dados?formato=csv&dataInicial={pd.to_datetime(self.start)}&dataFinal={pd.to_datetime(self.end)}'
        Download = pd.read_csv(DI)
        Download.columns = ['DI']
        Download.index = Download.index.str.rstrip(f';"0')
        Download['DI'] = Download['DI'].str.rstrip(f'"').astype(int)/100000000
        return Download

Prices('PRIO3.SA').get_tickers_brazil()
