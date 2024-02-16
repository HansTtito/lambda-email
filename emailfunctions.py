import pytz
import base64
from dateutil import parser

    
def obtener_correos_no_leidos(service, palabra_clave):
    try:
        query = f'subject:{palabra_clave} is:unread'
        results = service.users().messages().list(q=query, userId='me', labelIds=['INBOX']).execute()
        mensajes = results.get('messages', [])
        return mensajes

    except Exception as e:
        print(f"Error al obtener correos no leídos: {e}")
        return []
    

def obtener_remitente(servicio_gmail, user_id, message_id):
    try:
        # Obtener el mensaje completo
        mensaje = servicio_gmail.users().messages().get(userId=user_id, id=message_id).execute()

        # Inicializar el remitente como una cadena vacía
        remitente_email = ''

        # Buscar la información del remitente en los encabezados del mensaje
        for header in mensaje['payload']['headers']:
            if header['name'] == 'From':
                remitente_email = header['value']
                # Extraer solo la dirección de correo electrónico, si está presente
                remitente_email = remitente_email.split('<')[-1].strip('>').strip()
                break

        return remitente_email

    except Exception as e:
        print(f"Error al obtener el remitente: {e}")
        return None    
    
def obtener_fecha_hora(servicio_gmail, user_id, message_id):
    try:
        # Obtener el mensaje completo
        mensaje = servicio_gmail.users().messages().get(userId=user_id, id=message_id).execute()

        for header in mensaje['payload']['headers']:
            if header['name'] == 'Date':
                fecha_hora_str = header['value']
                date_obj = parser.parse(fecha_hora_str)

                peru_timezone = pytz.timezone('America/Lima')
                date_obj_peru = date_obj.astimezone(peru_timezone)

                fecha_envio = date_obj_peru.strftime('%Y-%m-%d')
                hora_envio = date_obj_peru.strftime('%H:%M:%S')

                return fecha_envio, hora_envio

        return None, None

    except Exception as e:
        print(f"Error al obtener la fecha y hora: {e}")
        return None, None
    

def download_attachments(service, message_id):
    
    message = service.users().messages().get(userId='me', id=message_id).execute()
    
    try:
        parts = message['payload']['parts']
        attachments = []

        for part in parts:
            filename = part.get('filename')

            if filename and 'body' in part and 'attachmentId' in part['body']:
                attachment_id = part['body']['attachmentId']

                # Descargar el archivo adjunto
                request = service.users().messages().attachments().get(userId='me', messageId=message_id, id=attachment_id)
                attachment = request.execute()
                file_data = base64.urlsafe_b64decode(attachment['data'])

                attachments.append({'filename': filename, 'file_data': file_data})

        return attachments

    except Exception as e:
        print(f"Error al descargar archivos adjuntos: {e}")
