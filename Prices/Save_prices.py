import pandas as pd
from datetime import datetime as dt

class Add_ativos:
    def __init__(self, conn, cursor):
        '''
        Pega as informações diretamente do Excel do controle
        Em caso de erro, verificar isso primeiramente
        Outro caso pode ser erro para puxar os ativos
        '''
        self.conn = conn
        self.cursor = cursor
        
        self.cursor.execute(f"USE sirius")
        
        try:
            self.cursor.execute("SELECT data, ativo, classe FROM alocacoes;")
            Ativos = pd.DataFrame([[i[0], i[1], i[2]] for i in self.cursor.fetchall()], columns=['Data', 'Ativos', 'Tipo']).drop_duplicates('Ativos')
        except cnn.Error as err:
            print(f"Erro ao selecionar tabela: {err}")
        
        self.start = Ativos['Data'].iloc[0]
        self.end = dt.today().date()
        
        self.ativos = Ativos[['Ativos', 'Tipo']]

    def get_data(self):
        '''
        Aqui se puxa os dados dos ativos
        A parte que realmente faz os download se encontra em "Preços.py"
        Facilmente incrementável (se limita a preços e variações nos ativos, nada mais)
        
        Posteriormente, adicionarei nova função para as curvas de futuros da B3
        '''
        
        # Download de ativos presentes no Yahoo Finance (B3 e tickers americanos)
        Ativos_YF = self.ativos.set_index('Tipo').loc[['Ações Listadas na B3', 'Ações Americanas']]
        Ativos_YF.loc['Ações Listadas na B3'] = Ativos_YF.loc['Ações Listadas na B3'].apply(lambda x: x+".SA")
        Ativos_YF = list(Ativos_YF['Ativos'].values)
        YF = Prices(ativos=Ativos_YF, start=self.start, end=self.end).get_tickers_yf()
        YF.columns = pd.Series(YF.columns).str.replace('.SA', '')

        # Scrapping de variações dos futuros na B3
        Ativos_FUT = self.ativos.set_index('Tipo').loc[['Futuros']]
        Ativos_FUT = list(Ativos_FUT['Ativos'].values)
        Curva_FUT = Prices(Ativos_FUT).get_fut_data([pd.to_datetime(i).strftime(r'%d/%m/%y') for i in YF.index])
        FUT = Curva_FUT
        for index in FUT.index:
            FUT.loc[index, 'Ticker'] = FUT.loc[index, 'Mercadoria']+'-'+FUT.loc[index, 'Vct']
        FUT = FUT[['Ticker', 'Valor do Ajuste por Contrato (R$)']].pivot(columns='Ticker', values='Valor do Ajuste por Contrato (R$)')

        # Download (via API BCB) do CDI overnight
        CDI = Prices(['CDI'], start=self.start, end=self.end).get_cdi()
        
        # Une os DataFrames e retorna a função

        return pd.concat([YF.loc[CDI.index], CDI.loc[CDI.index]], axis=1).join(FUT, how='inner').ffill()

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
    x = Add_ativos(conn, cursor).get_data()
    print(x)
    # Fecha a conexão
    Fechar()

# Caso rodado indiretamente
else:
    # Questões de interpretação da interface
    from Prices.Preços import Prices