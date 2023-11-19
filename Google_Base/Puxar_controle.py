if __name__ == '__main__':
    import Google as gg
    import pandas as pd
else:
    import Google_Base.Google as gg

# Lista = gg.list_files(r'1qT9BmHuXbFnPu8U89WlmM9ho9I4jXBRu')
def main(id_arq):
    id_arq = rf"{id_arq}"
    gg.download_file(id_arq, 'controle')
