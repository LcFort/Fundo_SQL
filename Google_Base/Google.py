import io
import os.path
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload

SCOPES = ["https://www.googleapis.com/auth/drive"]

def get_credentials():
    creds = None
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(r"C:\Users\lucas\Downloads\token.json", SCOPES)
            creds = flow.run_local_server(port=0)
        with open("token.json", "w") as token:
            token.write(creds.to_json())
    return creds

# Para arquivos padrão google (sheets, docs, ...)
# def download_file(file_id):
#     creds = get_credentials()
#     service = build("drive", "v3", credentials=creds)
#     request = service.files().export_media(fileId=file_id, mimeType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

#     with io.FileIO(f'download_{file_id}.xlsx', 'wb') as fh:
#         downloader = io.BytesIO(request.execute())
#         fh.write(downloader.read())

def download_file(file_id, name):
    creds = get_credentials()
    service = build("drive", "v3", credentials=creds)
    
    request = service.files().get_media(fileId=file_id)

    with io.FileIO(f'download_{name}.xlsx', 'wb') as fh:
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            _, done = downloader.next_chunk()


def upload_file(file_path, folder_id):
    creds = get_credentials()
    service = build("drive", "v3", credentials=creds)

    file_metadata = {
        'name': os.path.basename(file_path),
        'parents': [folder_id],
    }
    media = io.FileIO(file_path, mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    service.files().create(body=file_metadata, media_body=media, fields='id').execute()
    

def list_files(folder_id):
    # Obtém as credenciais
    creds = get_credentials()

    # Cria um serviço do Google Drive
    service = build("drive", "v3", credentials=creds)

    # Consulta para listar arquivos na pasta específica
    results = (
        service.files()
        .list(q=f"'{folder_id}' in parents", pageSize=10, fields="nextPageToken, files(id, name)")
        .execute()
    )
    
    # Retorna a lista de arquivos
    return results.get("files", [])

if __name__ == "__main__":
    # Substitua 'ID_DO_ARQUIVO' pelo ID real do seu arquivo no Google Drive
    file_id = r'1Ic0E96a6KkIfEX-lX6XmLfRwu7k56PqO'
    download_file(file_id)

    # Faça o que precisar com o arquivo baixado...

    # Substitua 'ID_DA_PASTA_DESTINO' pelo ID real da pasta no Google Drive
    # folder_id = r'0B2b6sf56b0-nX2l0cF9qLXRFeXc'
    # upload_file('arquivo_modificado.xlsx', folder_id)
    
else:
    pass