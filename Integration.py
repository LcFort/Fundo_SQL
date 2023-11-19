import mysql.connector as cnn
import yfinance as yf
import pandas as pd
from Alocações import Puxar_alocacoes as aloc
from Google_Base import Puxar_controle as pc
import os

id_arquivo = '1quIRoVsA3ySxtYkdKTr6eNFrlFIbKgR0'

config = {'user':'root', 'password':'Pipoca123', 'host':'localhost', 'raise_on_warnings':True}

conn = cnn.connect(**config)
cursor = conn.cursor()

def Fechar(cursor=cursor):
    cursor.close()
    conn.close()

def Add_Ativos(tickers = ['NVDA'], cursor = cursor):

    # Criar o banco de dados se não existir
    # cursor.execute("CREATE DATABASE IF NOT EXISTS Sirius;")
    
    # Usar o banco de dados
    cursor.execute(f"USE sirius")

    try:
        cursor.execute("DROP TABLE IF EXISTS TabelaAtivos;")
    except cnn.Error as err:
        print(f"Erro ao excluir tabela: {err}")

    try:
        cursor.execute("""
            CREATE TABLE TabelaAtivos (
                id INT AUTO_INCREMENT,
                ativo VARCHAR(10),
                data DATE,
                abertura FLOAT,
                max FLOAT,
                min FLOAT,
                fechamento FLOAT,
                volume INT,
                PRIMARY KEY (id)
            );
        """)
    except cnn.Error as err:
        print(f"Erro ao criar tabela: {err}")
    
    # cursor.execute(f"ALTER TABLE TabelaAtivos AUTO_INCREMENT = 1;")
    
    df = pd.DataFrame()
    for i in tickers:
        # Excluir todos os registros existentes para o ativo
        cursor.execute(f"DELETE FROM TabelaAtivos WHERE ativo = %s", (i,))
        d = yf.download(i, start = '2023-09-01').bfill()
        for index, row in d.iterrows():
            cursor.execute("""
                INSERT INTO TabelaAtivos (ativo, data, abertura, min, max, fechamento, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s);
            """, (i, index, row['Open'], row['High'], row['Low'], row['Close'], row['Volume']))

if not "download_controle.xlsx" in os.listdir(os.getcwd()):
    pc(rf'{id_arquivo}')

# cursor.execute('SHOW DATABASES;')
aloc(conn = conn, cursor = cursor)
# resultados = cursor.fetchall()

# print([i for i in resultados])

# Add_Ativos(['NVDA', 'XLF', 'XLK', 'PRIO3.SA', 'PETR4.SA'])
conn.commit()
# cursor.execute('SELECT * FROM TabelaAtivos;')
# print(pd.DataFrame(cursor.fetchall()).sort_values(2))

Fechar()