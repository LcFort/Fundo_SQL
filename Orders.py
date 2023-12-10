import pandas as pd

class Controle_Ativos:
    def __init__(self, cursor):
        self.cursor = cursor
        # Fazer pd.bdate_range()
    def Puxar_alocacoes(self):
        '''
        Procura resetar a tabela de alocações sempre
        Falta incrementar os tamanhos de cada posição
        (Futuros, ações, títulos, ...)
        A princípio, será utilizada a forma atual da planilha,
        podendo ser reestilizada no futuro.
        
        Falta adicionar a classe a partir da tabela de classificação*
        '''
        
        # Reseta a TABLE de alocações
        try:
            self.cursor.execute("DROP TABLE IF EXISTS Alocacoes;")
        except cnn.Error as err:
            print(f"Erro ao excluir tabela: {err}")

        # Cria a TABLE de alocações
        # Isso é temporário, ainda ocorrerão mudanças
        try:
            self.cursor.execute("""
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
            self.cursor.execute(f"ALTER TABLE Alocacoes AUTO_INCREMENT = 1;")
        except cnn.Error as err:
            print(f"Erro ao resetar o ID: {err}")
        
        # Lê o Excel do controle
        df = pd.read_excel('download_controle.xlsx', sheet_name='Alocação',  header = 1, index_col='Data').drop(columns="Unnamed: 0")
        df = df[['Ativo', 'Classe', 'Direção', 'Book', 'Preço', 'Dólar/Euro', 'Vencimento', 'Tamanho']]
        
        # Puxa a classe da TABLE de cadastro
        self.Puxar_cadastro()
        try:
            self.cursor.execute(f"SELECT ticker, classe, vencimento FROM cadastro;")
            cadastro = pd.DataFrame([[i[0], i[1], i[2]] for i in self.cursor.fetchall()]+[['ASAI3', 'Ações Listadas na B3', '']], columns=['ticker', 'classe', 'vencimento']).drop_duplicates('ticker')
            for i in cadastro['ticker']:
                df.loc[df['Ativo'] == i, ['Classe', 'Vencimento']] = cadastro.loc[cadastro['ticker'] == i, ['classe', 'vencimento']].values

        except cnn.Error as err:
            print(f"Erro ao resetar o ID: {err}")

        # Adiciona a tabela de alocações no servidor
        for index, row in df.iterrows():
            for c in range(len(row)):
                if pd.isna(row[c]):
                    row[c] = None
                else:
                    pass
            self.cursor.execute("""
                INSERT INTO Alocacoes (data, ativo, classe, direcao, book, preco, dolareuro, vencimento, tamanho)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
            """, (index, row['Ativo'], row['Classe'], row['Direção'], row['Book'], row['Preço'], row['Dólar/Euro'], row['Vencimento'], row['Tamanho']))
            
    ###################################################################################################################################################        
    ###################################################################################################################################################

    def Puxar_cadastro(self):
        '''
        Procura resetar a TABLE cadastro sempre que executado
        Possivelmente, irei mudar a forma como faço atualmente
        Tanto esta função, quanto a de cima
        
        Esta terá papel de ajudar nos cálculos de tamanho e exposição
        que serão dados na TABLE alocacoes
        '''
        # Reseta a TABLE de cadastro de ativos
        try:
            self.cursor.execute("DROP TABLE IF EXISTS cadastro;")
        except cnn.Error as err:
            print(f"Erro ao excluir tabela: {err}")
            
        # Cria a TABLE de cadastro de ativos
        # Isso é temporário, ainda ocorrerão mudanças
        try:
            self.cursor.execute("""
                CREATE TABLE cadastro (
                    id INT AUTO_INCREMENT,
                    ticker VARCHAR(20),
                    ativo VARCHAR(255),
                    classe VARCHAR(40),
                    vencimento DATE,
                    PRIMARY KEY (id)
                );
            """)
        except cnn.Error as err:
            print(f"Erro ao criar tabela: {err}")
            
        # Resetar ID
        try:
            self.cursor.execute(f"ALTER TABLE cadastro AUTO_INCREMENT = 1;")
        except cnn.Error as err:
            print(f"Erro ao resetar o ID: {err}")
        
        # Lê o Excel do controle
        df = pd.read_excel('download_controle.xlsx', sheet_name='Cadastro dos Ativos', index_col='*')
        
        # Adiciona a tabela de cadastro no servidor
        for index, row in df.iterrows():
            # Sem index, break
            if pd.isna(index):
                break
            else:
                pass
            
            # Limpa os NaN e NaT para Null, aceitado pelo SQL
            for c in range(len(row)):
                if pd.notna(row[c]):
                    if row.items() == 'Vencimento':
                        row[c] = pd.to_datetime(row[c])
                    else:
                        pass
                else:   
                    row[c] = None

            # Adiciona na TABLE cadastro
            self.cursor.execute("""
                INSERT INTO cadastro (ticker, ativo, classe, vencimento)
                VALUES (%s, %s, %s, %s);
            """, (index, row['Nome'], row['Classe'], row['Vencimento']))
        
        return df
    
###################################################################################################################################################        
###################################################################################################################################################

# Se for executado diretamente
if __name__ == "__main__":
    import mysql.connector as cnn
    from os import listdir, getcwd
    from Google_Base import Puxar_controle
    
    # Configurações para conexão no servidor
    config = {'user':'root', 'password':'Pipoca123', 'host':'localhost', 'raise_on_warnings':True, 'database':'sirius'}
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
    #Controle_Ativos(cursor).Puxar_cadastro()
    Controle_Ativos(cursor).Puxar_alocacoes()
    conn.commit()
    
    # Fecha a conexão
    Fechar(conn, cursor)