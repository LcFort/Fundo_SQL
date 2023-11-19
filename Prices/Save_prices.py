import pandas as pd
from datetime import datetime as dt

class Add_ativos:
    def __init__(self, conn, cursor):
        self.conn = conn
        self.cursor = cursor
        
        self.cursor.execute(f"USE sirius")
        
        try:
            self.cursor.execute("SELECT data, ativo, classe FROM alocacoes;")
            Ativos = pd.DataFrame([[i[0], i[1], i[2]] for i in self.cursor.fetchall()], columns=['Data', 'Ativos', 'Tipo']).drop_duplicates('Ativos')
        except cnn.Error as err:
            print(f"Erro ao excluir tabela: {err}")
        
        self.start = Ativos['Data'].iloc[0]
        self.end = dt.today().date()
        
        self.ativos = Ativos[['Ativos', 'Tipo']]

    def get_data(self):
        Ativos_YF = self.ativos.set_index('Tipo').loc[['Ações Listadas na B3', 'Ações Americanas']]
        Ativos_YF.loc['Ações Listadas na B3'] = Ativos_YF.loc['Ações Listadas na B3'].apply(lambda x: x+".SA")
        Ativos_YF = list(Ativos_YF['Ativos'].values)
        YF = Prices(ativos=Ativos_YF, start=self.start, end=self.end).get_tickers_yf()
        YF.columns = pd.Series(YF.columns).str.replace('.SA', '')

        Ativos_FUT = self.ativos.set_index('Tipo').loc[['Futuros']]
        Ativos_FUT = list(Ativos_FUT['Ativos'].values)
        FUT = Prices(Ativos_FUT, start=self.start, end=self.end).get_fut_data()
        
        CDI = Prices(['CDI'], start=self.start, end=self.end).get_cdi()
        
        return pd.concat([YF.loc[CDI.index], CDI.loc[CDI.index]], axis=1)
                
if __name__ == "__main__":
    from Preços import Prices
    import mysql.connector as cnn
    from os import listdir, getcwd
    
    config = {'user':'root', 'password':'Pipoca123', 'host':'localhost', 'raise_on_warnings':True}
    conn = cnn.connect(**config)
    cursor = conn.cursor()
    
    def Fechar(conn=conn, cursor=cursor):
        cursor.close()
        conn.close()

    Add_ativos(conn, cursor).get_data()
    
    Fechar()

else:
    from Prices.Preços import Prices