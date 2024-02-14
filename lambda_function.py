import os
import boto3
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
import base64
from dateutil import parser
import csv
import json
import io
import pytz

def lambda_handler(event, context):
    
    s3 = boto3.client('s3')
    # Obtener los datos del token desde S3
    bucket_name = 'token-access'  # Reemplaza con tu nombre de bucket
    object_key = 'token.json'  # Reemplaza con la ruta de tu archivo en el bucket
    
    response = s3.get_object(Bucket=bucket_name, Key=object_key)
    token_data_str = response['Body'].read().decode('utf-8')
    token_data = json.loads(token_data_str)
    
    if token_data:
        # Obtener las credenciales desde los datos del token
        creds = Credentials.from_authorized_user_info(token_data)
        # Crear el servicio Gmail
        service = build('gmail', 'v1', credentials=creds)

        # Definir el remitente y el asunto
        subject_keyword = 'Sitrapesca'
        user_id = 'me'
        download_folder_base = 'snp-demo'  # Ruta base en S3

        # Obtener la lista de mensajes
        results = service.users().messages().list(q=f'subject:{subject_keyword}', userId=user_id, labelIds=['INBOX']).execute()
        # Lista de remitentes que deseas procesar
        remitentes_procesar = ['echocce@austral.com.pe', 'lmoreno@tasa.com.pe', 'smontero@diamante.com.pe']

        # Procesar cada mensaje
        for message in results.get('messages', []):
            msg = service.users().messages().get(userId=user_id, id=message['id']).execute()
            # Obtener el remitente del mensaje
            sender_email = ''
            for header in msg['payload']['headers']:
                if header['name'] == 'From':
                    sender_email = header['value']
                    # Extraer solo la dirección de correo electrónico
                    sender_email = sender_email.split('<')[-1].strip('>').strip()
                    break

            # Verificar si el remitente está en la lista de remitentes a procesar
            if sender_email in remitentes_procesar:
                # Ajustar el valor de folder_prefix según el remitente
                folder_prefix = ''
                empresa = ''
                if 'echocce@austral.com.pe' in sender_email:
                    folder_prefix = 'austral/'
                    empresa = 'austral'
                elif 'lmoreno@tasa.com.pe' in sender_email:
                    folder_prefix = 'tasa/'
                    empresa = 'tasa'
                elif 'smontero@diamante.com.pe' in sender_email:
                    folder_prefix = 'diamante/'
                    empresa = 'diamante'
                    
                # Obtener la fecha y hora del correo
                date_str = msg['payload']['headers'][1]['value']
                date_obj = parser.parse(date_str.split(';')[-1].strip())

                # Convertir la fecha y hora a la zona horaria de Perú
                peru_timezone = pytz.timezone('America/Lima')
                date_obj_peru = date_obj.astimezone(peru_timezone)

                # Formatear la fecha y hora en el formato deseado
                fecha_envio = date_obj_peru.strftime('%Y-%m-%d')
                hora_envio = date_obj_peru.strftime('%H:%M:%S')

                # Crear una lista que represente cada fila del CSV
                csv_row = [fecha_envio, hora_envio, empresa]

                # Escribir la lista en un archivo CSV y guardarlo en S3
                csv_buffer = io.StringIO()
                csv_writer = csv.writer(csv_buffer)
                csv_writer.writerow(['Fecha envio', 'Hora envio', 'Empresa'])  # Encabezados del CSV
                csv_writer.writerow(csv_row)
                
                s3_key_csv = f'{folder_prefix}fechas_envio.csv'
                s3.put_object(Body=csv_buffer.getvalue(), Bucket=download_folder_base, Key=s3_key_csv)
                print(f"Archivo CSV creado y guardado en S3: {s3_key_csv}")

                # Descargar archivos adjuntos
                for part in msg['payload']['parts']:
                    filename = part.get('filename')
                    
                    if filename:
                        if 'body' in part and 'attachmentId' in part['body']:
                            attachment_id = part['body']['attachmentId']
                            
                            # Descargar el archivo adjunto
                            request = service.users().messages().attachments().get(userId=user_id, messageId=message['id'], id=attachment_id)
                            attachment = request.execute()
                            file_data = base64.urlsafe_b64decode(attachment['data'])
                            
                            # Guardar el archivo en la carpeta de descarga en S3
                            s3_key = f'{folder_prefix}{filename}'
                            s3.put_object(Body=file_data, Bucket=download_folder_base, Key=s3_key)
                            
                            print(f"Archivo descargado: {filename}, Guardado en S3: {s3_key}")
