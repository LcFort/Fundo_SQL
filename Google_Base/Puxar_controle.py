if __name__ == '__main__':
    import Google as Google
else:
    import Google_Base.Google as Google

# Lista = gg.list_files(r'1qT9BmHuXbFnPu8U89WlmM9ho9I4jXBRu')

def main(id_arq, path_credentials, nome = 'controle'):
    '''
    Função que puxa do Google.py o download do arquivo
    Possivelmente otimizável
    '''
    
    id_arq = rf"{id_arq}"
    Google.download_file(id_arq, path_credentials, nome)