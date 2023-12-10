import mysql.connector as cnn
import datetime as dt
import pandas as pd
from Orders import Controle_Ativos
from Google_Base import Puxar_controle
from Prices.Save_prices import Add_ativos
import os
import numpy as np

# Alterar sempre que a planilha de controle atualizar
id_arquivo = '1quIRoVsA3ySxtYkdKTr6eNFrlFIbKgR0'

# Path para as credenciais do Google API
path_credentials = r"C:\Users\lucas\Downloads\token.json"

# Informações do servidor MySQL
# Mudar somente se estiver em rede local
config = {'user':'root', 'password':'Pipoca123', 'host':'localhost', 'raise_on_warnings':True}

class Integration:
    def __init__(self, config, path_credentials):
        '''
        Definir as conexões com o servidor e com Google API
        Define database do servidor (sirius)
        Adiciona os dados atualizados da aba de alocações no servidor
        '''
        
        # Conectar ao servidor
        self.conn = cnn.connect(**config)
        self.cursor = self.conn.cursor()
        
        # Crendenciais dadas para conectar ao Google API
        self.path_credentials = path_credentials
        
        # Usar o banco de dados
        self.cursor.execute(f"USE sirius")
        
        # Se não tiver baixado a planilha de controle, baixe
        if not r"download_controle.xlsx" in os.listdir(os.getcwd()):
            Puxar_controle(rf'{id_arquivo}', path_credentials)
        
        # Da planilha de controle, inserir as alocações no servidor
        Controle_Ativos(self.cursor).Puxar_alocacoes()
        
        # Da planilha de controle, inserir o cadastro dos ativos no servidor
        self.cadastro = Controle_Ativos(self.cursor).Puxar_cadastro()
        
    def Fechar(self):
        '''
        Fechamento da conexão com o servidor
        '''
        self.cursor.close()
        self.conn.close()

    def update_hist(self):
        '''
        Atualiza os preços e/ou variações(para futuros) no histórico
        '''
        Tables = ['curva_fut', 'historico']
        
        # Baixa os ativos
        Curva_FUT, Data = Add_ativos(self.conn, self.cursor).get_data()
        
        # Deleta as TABLEs do histórico e curva_fut
        for i in Tables:
            try:
                self.cursor.execute(f"DROP TABLE IF EXISTS {i};")
            except cnn.Error as err:
                print(f"Erro ao excluir tabela: {err}")

        # Cria TABLE do histórico
        try:
            self.cursor.execute("""
                CREATE TABLE historico (
                id INT AUTO_INCREMENT,
                data DATE,
                ativo VARCHAR(25),
                precovar FLOAT,
                PRIMARY KEY (id));
            """)
        except cnn.Error as err:
            print(f"Erro ao criar tabela: {err}")
            
        # Cria TABLE das Curvas de Futuros
        try:
            self.cursor.execute("""
                CREATE TABLE curva_fut (
                id INT AUTO_INCREMENT,
                data DATE,
                mercadoria VARCHAR(10),
                vertice VARCHAR(10),
                vencimento DATE,
                ajuste FLOAT,
                taxa FLOAT,
                variacao FLOAT,
                variacao_contrato FLOAT,
                PRIMARY KEY (id));
            """)
        except cnn.Error as err:
            print(f"Erro ao criar tabela: {err}")
        
        # Reseta a ID
        for i in Tables:
            try:
                self.cursor.execute(f"ALTER TABLE {i} AUTO_INCREMENT = 1;")
            except cnn.Error as err:
                print(f'Erro ao resetar o ID: {err}')
        
        # Para cada ativo, adicionar informações na TABLE curva_fut
        comandos = []
        for index, line in Curva_FUT.iterrows():
            # if line['Mercadoria'] == 'DI1' or line['Mercadoria'] == 'DAP':
            #     line['Ticker'] = line['Mercadoria'] + '-' + line['Vct']
            #     line['Taxa'] = (100000/line['Preço de Ajuste Atual'])**(252/DU) - 1
            #     pd.bdate_range(index, )
            comandos += ((index, line['Mercadoria'], line['Vct'], None,
                          line['Preço de Ajuste Atual'], None, line['Variação'], line['Valor do Ajuste por Contrato (R$)']),)
        print(comandos[0])
        self.cursor.executemany("""
            INSERT INTO curva_fut (data, mercadoria, vertice, vencimento, ajuste, taxa, variacao, variacao_contrato)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
        """, comandos)
                
        # Para cada ativo, adicionar informações na TABLE histórico
        comandos = []
        for ativo in Data.columns:
            if pd.isnull(ativo): # Se ativo == nan
                print(1)
            # Para cada data (index) no DataFrame
            for index in Data[ativo].index:
                if pd.isnull(index): # Se index == nan
                    print(2)
                if pd.isnull(Data.loc[index, ativo]): # Se dados do ativo (x) e index (y) == nan
                    continue # Procedimento padrão, não puxou o ativo pois ele venceu (por exemplo). Em caso de falha, verificar outros meios, aqui não é o problema
                # Adiciona no servidor
                comandos += ((index, ativo, Data.loc[index, ativo]),)

        self.cursor.executemany("""
            INSERT INTO historico (data, ativo, precovar)
            VALUES (%s, %s, %s);
        """, comandos)
        
        # Executa comandos no servidor 
        self.conn.commit()
        
        # Fecha a conexão
        self.Fechar()

# Função
Integration(config, path_credentials).update_hist()

# Fazer table posições (ativo book Q)
# Refazer a coloção dos tipos na classificação dos ativos
# Ver se os dias que tivemos posições, tivemos preços (check)
# Calcular cota (caixa + movimentado + ajst fut - tx admin + (dividendos))