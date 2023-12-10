'''
Se puxam os ativos via este código
Adicionarei formas para puxar ativos no Investimenting.com (TiOc1, por exemplo)

Por enquanto,
somente possível via Yahoo Finance, API BCB e Scrappers.
'''

import pandas as pd
import yfinance as yf
import datetime as dt
from pandas_datareader import data as web
from requests import get
from numpy import array
from time import sleep

yf.pdr_override()

#################################################################################################################################
#################################################################################################################################

class Prices:
    def __init__(self, ativos = None, start = dt.datetime.today(), end = dt.datetime.today()):
        '''
        Introdução ao código
        
        ativos: recomendado formato lista
        '''
        
        # Filtra a forma aceitável de fornecer os ativos
        if type(ativos) in [type([]), type(array([]))]:
            self.ativos = ativos
        else:
            raise ValueError('Adicione um ativo e no formato correto (Lista ou np.Array).')
        
        # Data inicial e final
        self.start = start
        self.end = end
        
        # Define feriados para puxar somente os dias em que a B3 está aberta
        try:
            self.feriados = pd.read_excel('download_controle.xlsx', sheet_name='Feriados').apply(pd.to_datetime)
        except:
            ValueError('Erro na para puxar os feriados. Certifique-se de ter a planilha de controle baixada.')
            
#################################################################################################################################
#################################################################################################################################

    def get_tickers_yf(self):
        '''
        Baixa diretamente da biblioteca do Yahoo Finance
        
        Exclui os dados baixados em dia de B3 off
        '''
        Data = web.get_data_yahoo(self.ativos, self.start, pd.to_datetime(self.end) + dt.timedelta(1))['Adj Close']
        Data.index = pd.to_datetime(Data.index).date
        Data = Data[~Data.index.isin(self.feriados)]
        return Data
    
#################################################################################################################################
#################################################################################################################################

    def get_fut_data(self, date=None): # date = "m/d/y"
        '''
        Faz scrapping da página de todos os ativos. (Curva_FUT)
        
        Se "date" é uma lista, faz scrapping de todas essas datas. (Parte 1)
        Caso contrário, faz do dia somente. (Parte 3)
        
        Se "date" for nada (None), faz da última atualização. (Parte 2)
        '''
        
        # Define os produtos (derivativos)
        if self.ativos == []:
            pass
        else:
            produtos = pd.DataFrame([[i.split('-')[0], i.split('-')[1]] for i in self.ativos], columns=['Mercadoria', 'Vct'])
        
        feriados = self.feriados[self.feriados.columns[0]].apply(lambda x: x.strftime(r"%d/%m/%y")).unique()
        
        ##################################################################################################    
        # Parte 1
        ################################################################################################## 
        if type(date) == type([]): 
            Data = pd.DataFrame()
            for day in date:
                # Correção de data (date) para o padrão "dd/mm/yy", caso necessário
                try:
                    day = pd.to_datetime(day, format=r'%Y-%m-%d').date().strftime(r"%d/%m/%y")
                except:
                    day = pd.to_datetime(day, dayfirst=True).date().strftime(r"%d/%m/%y")
                    
                # Pula o dia se for feriado
                if day in feriados:
                    continue

                # Verifica se a data (date) for hoje e fornece o link correto
                if day == dt.datetime.today().date().strftime(r"%d/%m/%y") or abs((dt.datetime.strptime(day, r"%d/%m/%y").replace(hour=21, minute=0, second=0) - dt.datetime.today()).total_seconds())/3600 <24:
                    link = r'https://www2.bmf.com.br/pages/portal/bmfbovespa/boletim1/Ajustes1.asp?txt'  
                else:
                    try:
                        link = r'https://www2.bmf.com.br/pages/portal/bmfbovespa/boletim1/Ajustes1.asp?txt'+f'Data={day}' # m/d/y
                    except:
                        raise ValueError("Houve algum problema no código de Scrapping (Preços.py). Verifique a Parte 3 da função get_fut_data()")
                
                # Download do Html da página
                Raw = get(link, headers={'user-agent': 'Mozilla/5.0'})

                # Leitura da página
                Data_Dia = pd.concat(pd.read_html(Raw.content, decimal=',', thousands='.'))
                
                # Confirmação se foi possível baixar os dados
                if Data_Dia.iloc[-1][0] == "Não há dados para a data consultada.":
                    print(f'Há um problema na data (Fim de semana, feriado, B3 não abriu, ...), confira novamente. {day}')
                
                # Confirmação se o dia puxado é o mesmo que o fornecido
                try:            
                    day_confirm = pd.to_datetime(Data_Dia.iloc[17][0].split(': ')[1], dayfirst = True).date()
                except IndexError as err:
                    Data_Dia.to_csv('Error.csv')
                    print(f'{err, day}')
                    continue
                if pd.to_datetime(day, dayfirst=True).date() != day_confirm:
                    print(f'Dias diferentes:\nDia fornecido {day}; dia que foi nos fornecido {day_confirm}')
                
                ### Limpeza e organização dos dados
                # Pega a partir da coluna (Mercadoria, Vct, ...)
                Data_Dia = Data_Dia.iloc[18:]
                # Deixa a primeira linha (Mercadoria, Vct, ...) como coluna
                Data_Dia.columns = Data_Dia.iloc[0]
                # Dropa a primeira linha (já desnecessária)
                Data_Dia = Data_Dia.drop(0)
                # Separa o ticker da mercadoria de sua descrição
                Data_Dia['Mercadoria'] = Data_Dia['Mercadoria'].ffill().str.split(' - ').apply(lambda x: x[0])
                # Adiciona a data (date) dos dados como index
                Data_Dia.index = [pd.to_datetime(day, dayfirst=True).date() for i in range(len(Data_Dia['Vct'].values))]
                
                # Junta os dados baixados do dia (date) aos dias anteriores
                Data = pd.concat([Data, Data_Dia])
                
        ##################################################################################################    
        # Parte 2
        ################################################################################################## 
        elif date == None: 
            try:
                link = r'https://www2.bmf.com.br/pages/portal/bmfbovespa/boletim1/Ajustes1.asp?txt' # m/d/y
            except:
                raise ValueError("Houve algum problema no código de Scrapping (Preços.py). Verifique a Parte 2 da função get_fut_data()")
            
            # Download do Html da página
            Raw = get(link, headers={'user-agent': 'Mozilla/5.0'})
            
            # Leitura da página
            Data = pd.concat(pd.read_html(Raw.content, decimal=',', thousands='.'))
            
            # Dia que foi puxado
            day = pd.to_datetime(Data.iloc[17][0].split(': ')[1], dayfirst = True).date()
            
            ### Limpeza e organização dos dados
            # Pega a partir da coluna (Mercadoria, Vct, ...)
            Data = Data.iloc[18:]
            # Deixa a primeira linha (Mercadoria, Vct, ...) como coluna
            Data.columns = Data.iloc[0]
            # Dropa a primeira linha (já desnecessária)
            Data = Data.drop(0)
            # Separa o ticker da mercadoria de sua descrição
            Data['Mercadoria'] = Data['Mercadoria'].ffill().str.split(' - ').apply(lambda x: x[0])
            # Adiciona a data (date) dos dados como index
            Data.index = [day for i in range(len(Data['Vct'].values))]
            
        ##################################################################################################    
        # Parte 3
        ################################################################################################## 
        elif type(date) == type(""): 
            # Correção de data (date) para o padrão "dd/mm/yy", caso necessário
            try:
                day = pd.to_datetime(date, format=r'%Y-%m-%d').date().strftime(r"%d/%m/%y")
            except:
                day = pd.to_datetime(date, dayfirst=True).date().strftime(r"%d/%m/%y")
            
            # Verifica se a data (date) for hoje e fornece o link correto
            if day == dt.datetime.today().date().strftime(r"%d/%m/%y"):
                link = r'https://www2.bmf.com.br/pages/portal/bmfbovespa/boletim1/Ajustes1.asp?txt'
            else:
                try:  
                    link = r'https://www2.bmf.com.br/pages/portal/bmfbovespa/boletim1/Ajustes1.asp?txt'+f'Data={day}' # m/d/y
                except:
                    raise ValueError("Houve algum problema no código de Scrapping (Preços.py). Verifique a Parte 3 da função get_fut_data()")

            # Download do Html da página
            Raw = get(link, headers={'user-agent': 'Mozilla/5.0'})
            
            # Leitura da página
            Data = pd.concat(pd.read_html(Raw.content, decimal=',', thousands='.'))

            # Confirmação se foi possível baixar os dados
            if Data_Dia.iloc[-1][0] == "Não há dados para a data consultada.":
                print(f'Há um problema na data (Fim de semana, feriado, B3 não abriu, ...), confira novamente. {day}')
                
            # Confirmação se o dia puxado é o mesmo que o fornecido
            day_confirm = pd.to_datetime(Data.iloc[17][0].split(': ')[1], dayfirst = True)
            if pd.to_datetime(day, dayfirst=True) != day_confirm:
                print(f'Dias diferentes:\nDia fornecido {day}; dia que foi nos fornecido {day_confirm}')
            
            ### Limpeza e organização dos dados
            # Pega a partir da coluna (Mercadoria, Vct, ...)
            Data = Data.iloc[18:] 
            # Deixa a primeira linha (Mercadoria, Vct, ...) como coluna
            Data.columns = Data.iloc[0] 
            # Dropa a primeira linha (já desnecessária)
            Data = Data.drop(0) 
            # Separa o ticker da mercadoria de sua descrição
            Data['Mercadoria'] = Data['Mercadoria'].ffill().str.split(' - ').apply(lambda x: x[0])
            # Adiciona a data (date) dos dados como index
            Data.index = pd.to_datetime([day_confirm for i in range(len(Data['Vct'].values))]).date
            
        ##################################################################################################
        # Parte 4 (Error)
        ################################################################################################## 
        else: 
            ValueError("Não foi possível identificar a data (date) fornecida. Favor, leia o código 'Preços.py' get_fut_data()")
            
        ##################################################################################################    
        # Finalização
        ##################################################################################################   
        Data = Data[~Data.index.isin(self.feriados)]
        
        for col in Data.columns[-4:]:
            Data[col] = pd.to_numeric(Data[col])
           
        Data.loc[(Data[Data.columns[-2]]<0), Data.columns[-1]] *= -1 

        Vencimentos = {'F':1, 'G': 2, 'H':3, 'J':4, 'K':5, 'M':6, 'N':7, 'Q':8, 'U':9, 'V':10, 'X':11, 'Z':12}
        
        # Espaço para a elaboração de datas de vencimento automáticas

        if self.ativos == []:
            
            return Data # Curva_FUT
        else:
            Data_1 = Data[Data.set_index(['Mercadoria', 'Vct']).index.isin(produtos.set_index(['Mercadoria', 'Vct']).index)]
            return Data, Data_1 # Curva_FUT, FUT
        
#################################################################################################################################
#################################################################################################################################

    def get_titulos_br(self):
        '''
        Falta fazer
        '''
        return 0
    
#################################################################################################################################
#################################################################################################################################

    def get_currency(self):
        '''
        Maioria da para puxar com YF
        PTAX talvez não e CNY
        Fácil
        '''
        return 0
    
#################################################################################################################################
################################################################################################################################# 
 
    def get_cdi(self):
        '''
        Utiliza a API do Banco Central do Brasil para puxar o DI overnight
        '''
        
        # Link para a API, baixando em CSV, da data Start até End
        DI = f'https://api.bcb.gov.br/dados/serie/bcdata.sgs.11/dados?formato=csv&dataInicial={(pd.to_datetime(self.start).strftime(r"%d/%m/%y"))}&dataFinal={(pd.to_datetime(self.end).strftime(r"%d/%m/%y"))}'
        
        # Baixa o arquivo da API e lê
        Download = pd.read_csv(DI)
        
        # Tratamento
        Download.columns = ['CDI']
        Download.index = Download.index.str.rstrip(f';"0')
        Download.index = pd.to_datetime(Download.index, dayfirst=True).date
        Download['CDI'] = Download['CDI'].str.rstrip(f'"').astype(int)/100000000
        Download = Download[~Download.index.isin(self.feriados)]
        # Retorna dados
        return Download
    
#################################################################################################################################
#################################################################################################################################

# Se executado diretamente
if __name__ == '__main__':
    print(dt.datetime.today())
    # Exemplo de pleno funcionamento
    y=Prices([''], start = '2023-11-14', end='2023-11-20').get_cdi()
    x = Prices(['DI1-F24', 'DI1-F25']).get_fut_data([pd.to_datetime(i).strftime(r'%d/%m/%y') for i in y.index])
    # x = Prices(['ASAI3.SA']).get_tickers_yf()
    print(x, '\n\n')
    for index in x.index:
        x.loc[index, 'Ticker'] = x.loc[index, 'Mercadoria']+'-'+x.loc[index, 'Vct']
    x = x[[x.columns[-2], x.columns[-1]]]

    x = x[['Ticker', 'Valor do Ajuste por Contrato (R$)']].pivot(columns='Ticker', values='Valor do Ajuste por Contrato (R$)')
    
    print(x)