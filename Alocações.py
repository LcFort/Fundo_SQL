import pandas as pd

# pc.main(r'1quIRoVsA3ySxtYkdKTr6eNFrlFIbKgR0')

if __name__ == '__main__':
    import mysql.connector as cnn
    config = {'user':'root', 'password':'Pipoca123', 'host':'localhost', 'raise_on_warnings':True}

    conn = cnn.connect(**config)
    cursor = conn.cursor()
    
    def Fechar(cursor):
        cursor.close()
        conn.close()

def Puxar_alocacoes(conn, cursor):
    cursor.execute(f"USE sirius")
    
    try:
        cursor.execute("DROP TABLE IF EXISTS Alocacoes;")
    except cnn.Error as err:
        print(f"Erro ao excluir tabela: {err}")

    try:
        cursor.execute("""
            CREATE TABLE Alocacoes (
                id INT AUTO_INCREMENT,
                data DATE,
                ativo VARCHAR(20),
                classe VARCHAR(40),
                direcao VARCHAR(10),
                book VARCHAR(10),
                preco FLOAT,
                dolareuro FLOAT,
                vencimento VARCHAR(20),
                tamanho INT,
                PRIMARY KEY (id)
            );
        """)
    except cnn.Error as err:
        print(f"Erro ao criar tabela: {err}")
    
    # cursor.execute(f"ALTER TABLE TabelaAtivos AUTO_INCREMENT = 1;")
    
    df = pd.read_excel('download_controle.xlsx', sheet_name='Alocação',  header = 1, index_col='Data').drop(columns="Unnamed: 0")
    df = df[['Ativo', 'Classe', 'Direção', 'Book', 'Preço', 'Dólar/Euro', 'Vencimento', 'Tamanho']]

    for index, row in df.iterrows():
        for c in range(len(row)):
            if pd.isna(row[c]):
                row[c] = None
            else:
                pass
        cursor.execute("""
            INSERT INTO Alocacoes (data, ativo, classe, direcao, book, preco, dolareuro, vencimento, tamanho)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
        """, (index, row['Ativo'], row['Classe'], row['Direção'], row['Book'], row['Preço'], row['Dólar/Euro'], row['Vencimento'], row['Tamanho']))

if __name__ == "__main__":
    Alocações(conn, cursor)
    conn.commit()
    Fechar(cursor)