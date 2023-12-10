import pandas as pd
import datetime as dt
#################################################################################################################################
#################################################################################################################################
class Add_ativos:
    def __init__(self, conn, cursor, start = None, end = None):
        '''
        Pega as informações diretamente do Excel do controle
        Em caso de erro, verificar isso primeiramente
        Outro caso pode ser erro para puxar os ativos
        '''
        
        # Tira aviso do Pandas sobre o método
        pd.options.mode.chained_assignment = None
        
        # Se conecta ao servidor
        self.conn = conn
        self.cursor = cursor
        
        # Puxando as alocações
        self.cursor.execute(f"USE sirius")
        
        try:
            self.cursor.execute("SELECT data, ativo, classe FROM alocacoes;")
            Ativos = pd.DataFrame([[i[0], i[1], i[2]] for i in self.cursor.fetchall()], columns=['Data', 'Ativos', 'Tipo']).drop_duplicates('Ativos')
        except cnn.Error as err:
            print(f"Erro ao selecionar tabela: {err}")
        
        # Definindo self.ativos
        self.ativos = Ativos[['Ativos', 'Tipo']]
        
        # Definindo self.start
        if start == None:
            self.start = Ativos['Data'].iloc[0]
        else:
            self.start = start

        # Definindo self.end
        self.end = (dt.datetime.today().date() if dt.datetime.today().time() >= dt.datetime.today().replace(hour=21, minute=0, second=0, microsecond=0).time() else dt.datetime.today().date() - dt.timedelta(days=1))

#################################################################################################################################
#################################################################################################################################
    def get_data(self):
        '''
        Aqui se puxa os dados dos ativos
        A parte que realmente faz os download se encontra em "Preços.py"
        Facilmente incrementável (se limita a preços e variações nos ativos, nada mais)
        '''

        # Download de ativos presentes no Yahoo Finance (B3 e tickers americanos)
        Ativos_YF = self.ativos.set_index('Tipo').loc[['Ações Listadas na B3', 'Ações Americanas']]
        Ativos_YF.loc['Ações Listadas na B3'] = Ativos_YF.loc['Ações Listadas na B3'].apply(lambda x: x+".SA")
        Ativos_YF = list(Ativos_YF['Ativos'].values)
        YF = Prices(ativos=Ativos_YF+['AMZO34.SA'], start=self.start, end=self.end).get_tickers_yf()
        YF.columns = pd.Series(YF.columns).str.replace('.SA', '')

        # Scrapping de variações dos futuros na B3
        Ativos_FUT = self.ativos.set_index('Tipo').loc[['Futuros']]
        Ativos_FUT = list(Ativos_FUT['Ativos'].values)
        Curva_FUT, FUT = Prices(Ativos_FUT).get_fut_data([pd.to_datetime(i).strftime(r'%d/%m/%y') for i in YF.index])
        FUT.loc[:, 'Ticker'] = FUT.loc[:, 'Mercadoria'] + '-' + FUT.loc[:, 'Vct']
        FUT = FUT[['Ticker', 'Valor do Ajuste por Contrato (R$)']].pivot(columns='Ticker', values='Valor do Ajuste por Contrato (R$)')

        # Download (via API BCB) do CDI overnight
        CDI = Prices(['CDI'], start=self.start, end=self.end).get_cdi()
        
        # Une os DataFrames e retorna a curva dos derivativos + histórico
        return Curva_FUT, pd.concat([YF.loc[CDI.index], CDI.loc[CDI.index]], axis=1).ffill().join(FUT, how='inner')
#################################################################################################################################
#################################################################################################################################

# Se executado diretamente               
if __name__ == "__main__":
    from Preços import Prices
    import mysql.connector as cnn
    from os import listdir, getcwd
    
    # Configurações para conexão no servidor
    config = {'user':'root', 'password':'Pipoca123', 'host':'localhost', 'raise_on_warnings':True}
    conn = cnn.connect(**config)
    cursor = conn.cursor()
    
    # Fechar conexão do servidor
    def Fechar(conn=conn, cursor=cursor):
        cursor.close()
        conn.close()

    # Função
    x, y = Add_ativos(conn, cursor).get_data() # x = Curva_FUT; y = histórico
    
    # Fecha a conexão
    Fechar()

# Caso rodado indiretamente
else:
    # Questões de interpretação da interface
    from Prices.Preços import Prices