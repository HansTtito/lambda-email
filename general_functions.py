

def read_files_from_bucket(s3_client, bucket_name, object_key, index_bucket = 'Body'):
    
    response = s3_client.get_object(Bucket = bucket_name, Key = object_key)
    output = response[index_bucket].read().decode('utf-8', errors='replace')
        
    return output

def guardar_archivo_en_s3(s3_client, content, s3_key, s3_bucket_name):
    try:
        s3_client.put_object(Body=content, Bucket=s3_bucket_name, Key=s3_key)        
        print(f"Archivo CSV creado y guardado en S3: {s3_key}")

    except Exception as e:
        print(f"Error al guardar el archivo CSV en S3: {e}")

def eliminar_archivo_en_s3(s3_client, bucket_name, key):
    try:
        s3_client.delete_object(Bucket=bucket_name, Key=key)
        print(f"Archivo eliminado en S3: {key}")
    except Exception as e:
        print(f"Error al eliminar el archivo en S3: {e}")
