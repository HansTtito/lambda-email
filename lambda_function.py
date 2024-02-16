import boto3
import pandas as pd
import io
import json
import logging
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from emailfunctions import *
from general_functions import *

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    
    s3 = boto3.client('s3')
    # Obtener los datos del token desde S3
    bucket_name = 'token-access'  # bucket json
    object_key = 'token_snp.json'  # token access
    correos = read_files_from_bucket(s3, 'snp-demo','extras/emails.csv')   
    correos_lista = pd.read_csv(io.StringIO(correos))
    
    try:

        token_data_str = read_files_from_bucket(s3, bucket_name, object_key)
        token_data = json.loads(token_data_str)
          
        if token_data:
            
            creds = Credentials.from_authorized_user_info(token_data)
            service = build('gmail', 'v1', credentials=creds)
            
            unread_messages = obtener_correos_no_leidos(service, 'Sitrapesca')

            if unread_messages:
                
                for message in unread_messages:
                    
                    sender_email = obtener_remitente(service,  'me',  message['id'])
                    
                    if sender_email in correos_lista['CORREO'].to_list():
                        
                        empresa = correos_lista.loc[correos_lista['CORREO'] == sender_email, 'label_empresa'].astype(str).iloc[0]

                        fecha_envio, hora_envio = obtener_fecha_hora(service, 'me', message['id'])
                        
                        date_time_sr = pd.DataFrame([fecha_envio, hora_envio, empresa], index=['Fecha_envio', 'Hora_envio', 'Empresa']).T
                        date_time_sr_content = date_time_sr.to_csv(index=False)
                        
                        guardar_archivo_en_s3(s3, date_time_sr_content,f'RegionNorteCentro/Temporada_2024_I/DescargaCorreos/{empresa}/fechas_envio.csv','snp-demo')
                        
                        file = download_attachments(service, message['id'])
                        
                        for file_info in file:
                            filename = file_info['filename']
                            file_data = file_info['file_data']
                            
                            if filename and file_data:
                                s3_key = f'RegionNorteCentro/Temporada_2024_I/DescargaCorreos/{empresa}/{filename}'
                                guardar_archivo_en_s3(s3, file_data, s3_key, 'snp-demo')
                                
                    # Marcar el mensaje como leído
                    service.users().messages().modify(userId='me', id=message['id'], body={'removeLabelIds': ['UNREAD']}).execute()

            # Assuming you want to return a success message
        return {
            'statusCode': 200,
            'body': 'Se leyó correctamente el correo".'
        }
    except Exception as e:
        logger.exception(f'Error al leer el correo: {str(e)}')
        return {
            'statusCode': 500,
            'body': f'Error al leer el correo: {str(e)}'
        }
        
