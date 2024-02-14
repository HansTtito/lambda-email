import os
import boto3
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

def download_token_from_s3(bucket_name, object_key):
    s3 = boto3.client('s3')
    local_file_path = '/tmp/token.json'  # Puedes ajustar la ruta según tus necesidades

    try:
        s3.download_file(bucket_name, object_key, local_file_path)
        return local_file_path
    except Exception as e:
        print(f"Error al descargar el archivo desde S3: {e}")
        return None


def lambda_handler(event, context):
    # Descargar el archivo de token desde S3
    bucket_name = 'token-access'  # Reemplaza con tu nombre de bucket
    object_key = 'token.json'  # Reemplaza con la ruta de tu archivo en el bucket
    local_token_path = download_token_from_s3(bucket_name, object_key)

    if local_token_path:
        # Obtener las credenciales desde el archivo de token
        creds = Credentials.from_authorized_user_file(local_token_path)

        # Crear el servicio Gmail
        service = build('gmail', 'v1', credentials=creds)

        # Ahora puedes usar 'service' para acceder a los correos electrónicos, por ejemplo:
        user_id = 'me'
        results = service.users().messages().list(userId=user_id, labelIds=['INBOX']).execute()
        
        # Procesar los resultados y realizar acciones según tus requisitos
        for message in results.get('messages', []):
            msg = service.users().messages().get(userId=user_id, id=message['id']).execute()
            # Realizar acciones con el mensaje, por ejemplo, imprimir el asunto
            print(f"Asunto: {msg['subject']}")
    else:
        print("No se pudo descargar el archivo de token desde S3.")
        
        
