import pandas as pd

def Puxar_alocacoes(cursor):
    '''
    Procura resetar a tabela de alocações sempre
    Falta incrementar os tamanhos de cada posição
    (Futuros, ações, títulos, ...)
    A princípio, será utilizada a forma atual da planilha,
    podendo ser reestilizada no futuro.
    '''
    
    # Reseta a TABLE de alocações
    try:
        cursor.execute("DROP TABLE IF EXISTS Alocacoes;")
    except cnn.Error as err:
        print(f"Erro ao excluir tabela: {err}")

    # Cria a TABLE de alocações
    # Isso é temporário, ainda ocorrerão mudanças
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
    
    # Resetar ID
    try:
        cursor.execute(f"ALTER TABLE TabelaAtivos AUTO_INCREMENT = 1;")
    except cnn.Error as err:
        print(f"Erro ao resetar o ID: {err}")
    
    # Lê o Excel do controle
    df = pd.read_excel('download_controle.xlsx', sheet_name='Alocação',  header = 1, index_col='Data').drop(columns="Unnamed: 0")
    df = df[['Ativo', 'Classe', 'Direção', 'Book', 'Preço', 'Dólar/Euro', 'Vencimento', 'Tamanho']]

    # Adiciona a tabela de alocações no servidor
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

# Se for executado diretamente
if __name__ == "__main__":
    import mysql.connector as cnn
    from os import listdir, getcwd
    from Google_Base import Puxar_controle
    
    # Configurações para conexão no servidor
    config = {'user':'root', 'password':'Pipoca123', 'host':'localhost', 'raise_on_warnings':True}
    conn = cnn.connect(**config)
    cursor = conn.cursor()
    
    # Credenciais únicas de cada um do Google API (não é o token que aparece)
    # Recomendável adicionar um PATH exclusivo para tal no environment do computador (evita de vazar)
    path_credentials = r'C:\Users\lucas\Downloads\token.json'
    
    # Fechar conexão do servidor
    def Fechar(conn, cursor):
        cursor.close()
        conn.close()
    
    # Baixa a planilha de controle, caso já não tenha sido baixada
    if not "download_controle.xlsx" in listdir(getcwd()):
        Puxar_controle(r'1quIRoVsA3ySxtYkdKTr6eNFrlFIbKgR0', path_credentials) # Atualizar ID do controle Sirius
    
    # Função
    Puxar_alocacoes(cursor)
    conn.commit()
    
    # Fecha a conexão
    Fechar(conn, cursor)