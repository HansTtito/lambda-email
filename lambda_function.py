import boto3
import json
import logging
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from emailfunctions import *
from general_functions import *

logger = logging.getLogger()
logger.setLevel(logging.INFO)

#Get environment variable from SSM
client_ssm = boto3.client('ssm')

def lambda_handler(event, context):
    
    s3 = boto3.client('s3')
    # Obtener los datos del token desde S3
    main_bucket_name = client_ssm.get_parameter(Name='bucket_name')['Parameter']['Value']
    url_api_base = client_ssm.get_parameter(Name='API_url')['Parameter']['Value']
    temporada_endpoint = '/temporadasUbicaciones/lastTemporada'
    new_season = url_api_base + temporada_endpoint
    url_api_key_email = "https://oauth2.googleapis.com/token"
    api_key = client_ssm.get_parameter(Name='lastTemporada_ApiKey')['Parameter']['Value']
    client_id = client_ssm.get_parameter(Name='client_id')['Parameter']['Value']
    client_secret = client_ssm.get_parameter(Name='client_secret')['Parameter']['Value']
    refresh_token = client_ssm.get_parameter(Name='refresh_token')['Parameter']['Value']

    payload = f'client_id={client_id}&client_secret={client_secret}&grant_type=refresh_token&refresh_token={refresh_token}'
    headers_email = {
       'Content-Type': 'application/x-www-form-urlencoded'
    }

    correos = read_files_from_bucket(s3, main_bucket_name,'extras/emails.csv')   
    df = pd.read_csv(io.StringIO(correos))
    correos_lista = df.to_dict(orient='records')
    
    try:

        response = requests.post(url_api_key_email, headers=headers_email, data=payload)
        response_json = response.json()

        token_data = {
            'access_token': response_json['access_token'],
            'expires_in': response_json['expires_in'],
            'scope': response_json['scope'],
            'token_type': "Bearer",
            'client_id': client_id,
            'client_secret': client_secret,
            'refresh_token': refresh_token,
        }
        
        api_temporada = obtener_api_temporada(new_season, api_key)
        temporada = api_temporada['temporada']
          
        if token_data:
            
            creds = Credentials.from_authorized_user_info(token_data)
            service = build('gmail', 'v1', credentials=creds)
            
            mensajes_con_asunto= obtener_correos_no_leidos_2(service,'SITRAPESCA CN')
            
            for mensaje, asunto in mensajes_con_asunto:
                
                if 'SIN REPORTE' in asunto:
                    # Procesar el mensaje como SIN REPORTE
                    sender_email = obtener_remitente(service, 'me', mensaje['id'])
                    
                    for empresa_info in correos_lista:
                        
                        if sender_email == empresa_info['CORREO']:
                            
                            empresa = empresa_info['label_empresa']
                            fecha_envio, hora_envio = obtener_fecha_hora(service, 'me', mensaje['id'])
                            cuerpo_mensaje = extraer_mensaje(service, 'me', mensaje['id'])
                            if cuerpo_mensaje:
                                fechas = extraer_fechas(cuerpo_mensaje)
                                date_time_sr_content = ""

                                if fechas:  # Si hay una sola fecha                                    
                                    rango_fechas = generar_rango_fechas(fechas)
                                    print(rango_fechas)
                                    for fecha in rango_fechas:                                       
                                        # Agregar la fila al contenido
                                        date_time_sr_content += f"{fecha_envio},{fecha},{hora_envio},{empresa},sin_reporte\n"
                                else:
                                    date_time_sr_content += f"{fecha_envio},{fecha_envio},{hora_envio},{empresa},sin_reporte\n"

                            guardar_archivo_en_s3(s3, date_time_sr_content.encode('utf-8'), f'regionNorteCentro/{temporada}/DescargaCorreos/{empresa}/fechas_envio.txt', main_bucket_name)
                    # Marcar el mensaje como leído
                    service.users().messages().modify(userId='me', id=mensaje['id'], body={'removeLabelIds': ['UNREAD']}).execute()
                    
                else:
                    # Procesar el mensaje como con reporte
                    sender_email = obtener_remitente(service, 'me', mensaje['id'])
                    for empresa_info in correos_lista:
                        if sender_email == empresa_info['CORREO']:
                            empresa = empresa_info['label_empresa']
                            fecha_envio, hora_envio = obtener_fecha_hora(service, 'me', mensaje['id'])
                            date_time_sr_content = f"{fecha_envio},{fecha_envio},{hora_envio},{empresa},reporte\n"
                            guardar_archivo_en_s3(s3, date_time_sr_content.encode('utf-8'), f'regionNorteCentro/{temporada}/DescargaCorreos/{empresa}/fechas_envio.txt', main_bucket_name)
                            # Descargar archivos adjuntos
                            files = download_attachments(service, mensaje['id'])
                            for file_info in files:
                                filename = file_info['filename']
                                file_data = file_info['file_data']
                                if filename and file_data:
                                    s3_key = f'regionNorteCentro/{temporada}/DescargaCorreos/{empresa}/{filename}'
                                    guardar_archivo_en_s3(s3, file_data, s3_key, main_bucket_name)
                    # Marcar el mensaje como leído
                    service.users().messages().modify(userId='me', id=mensaje['id'], body={'removeLabelIds': ['UNREAD']}).execute()


        #En el lambda descarga_email: 
        sqs = boto3.client('sqs')
        queue_url = client_ssm.get_parameter(Name='sqs_ToCheckFiles')['Parameter']['Value']
        sqs.send_message(
            QueueUrl=queue_url,
            MessageBody='Message from  descarga_email'
        )
        
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
        
