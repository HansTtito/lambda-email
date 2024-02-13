import os
import boto3
import io
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from dateutil import parser
import csv
import logging
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def get_credentials_from_env():
    # Leer las variables de entorno
    env_vars = {
        "type": os.environ.get("GMAIL_TYPE"),
        "project_id": os.environ.get("GMAIL_PROJECT_ID"),
        "private_key_id": os.environ.get("GMAIL_PRIVATE_KEY_ID"),
        "private_key": os.environ.get("GMAIL_PRIVATE_KEY").replace('\\n', '\n'),  # Ajustar formato de la clave privada
        "client_email": os.environ.get("GMAIL_CLIENT_EMAIL"),
        "client_id": os.environ.get("GMAIL_CLIENT_ID"),
        "auth_uri": os.environ.get("GMAIL_AUTH_URI"),
        "token_uri": os.environ.get("GMAIL_TOKEN_URI"),
        "auth_provider_x509_cert_url": os.environ.get("GMAIL_AUTH_PROVIDER_CERT_URL"),
        "client_x509_cert_url": os.environ.get("GMAIL_CLIENT_CERT_URL")
    }

    # Crear instancias de Credentials desde las variables de entorno
    creds = Credentials.from_authorized_user_info(env_vars)
    return creds
    

def lambda_handler(event, context):
    # Obtener las credenciales desde variables de entorno
    creds = get_credentials_from_env()

    service = build('gmail', 'v1', credentials=creds)

    # Definir el remitente y el asunto
    sender_email = 'echocce@austral.com.pe'
    subject_keyword = 'Sitrapesca'

    user_id = 'me'
    query = f"from:{sender_email} subject:{subject_keyword}"

    # Configurar el cliente de S3
    s3 = boto3.client('s3')
    bucket_name = 'snp-demo'

    # Definir el mapeo de remitentes a carpetas específicas
    sender_folder_mapping = {
        'echocce@austral.com.pe': 'austral',
        'otro-sender@example.com': 'otro-sender'
    }

    # Obtener la lista de mensajes
    results = service.users().messages().list(q=query, userId=user_id, labelIds=['INBOX']).execute()

    for message in results.get('messages', []):
        msg = service.users().messages().get(userId=user_id, id=message['id']).execute()

        # Obtener el remitente del mensaje
        sender_email = ''
        for header in msg['payload']['headers']:
            if header['name'] == 'From':
                sender_email = header['value']
                break

        # Ajustar el valor de folder_prefix según el sender
        folder_prefix = sender_folder_mapping.get(sender_email, 'otros/')

        # Crear el archivo CSV en memoria
        csv_buffer = io.StringIO()
        csv_writer = csv.writer(csv_buffer)
        csv_writer.writerow(['Fecha envio', 'Hora envio'])  # Encabezados del CSV

        # Extraer la fecha y hora de envío del mensaje
        date_str = msg['payload']['headers'][1]['value']
        date_obj = parser.parse(date_str.split(';')[-1].strip())
        
        fecha_envio = date_obj.strftime('%Y-%m-%d')
        hora_envio = date_obj.strftime('%H:%M:%S')

        # Escribir en el archivo CSV
        csv_writer.writerow([fecha_envio, hora_envio])
        print(f"Fecha de Envío: {fecha_envio}, Hora de Envío: {hora_envio}")

        # Subir el archivo CSV a S3
        csv_buffer.seek(0)
        s3.put_object(Body=csv_buffer.getvalue(), Bucket=bucket_name, Key=f'{folder_prefix}fechas_envio.csv')
        print("Archivo CSV subido a S3")