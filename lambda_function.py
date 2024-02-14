import os
import boto3
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from botocore.exceptions import ClientError

def get_secret():

    secret_name = "TokenGmailSNP"
    region_name = "us-east-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    secret = get_secret_value_response['SecretString']
    
def lambda_handler(event, context):
    # Obtener las credenciales desde Secrets Manager
    secret_data = get_secret("TokenGmailSNP")
    creds = Credentials.from_authorized_user_info(secret_data)

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