import pandas as pd
import numpy as np
from haversine import haversine

def lat_long(lat_lon):
    # Dividir la cadena en grados, minutos y segundos
    lat_lon_split = lat_lon.str.split(" ", n=2, expand=True)

    # Convertir las partes a números y realizar cálculos
    lat_lon_final = lat_lon_split.apply(lambda x: pd.to_numeric(x.str.replace("[' °]*", "", regex=True)), axis=1)
    lat_lon_final[0] = -1 * lat_lon_final[0] - lat_lon_final[1] / 60 - lat_lon_final[2] / 3600

    return lat_lon_final[0]
  
  
def processing_faenas(data_faenas):
    # Seleccionar columnas específicas
    data_faenas = data_faenas.iloc[:, [7, 2, 1, 3, 5, 6]]

    # Renombrar las columnas
    data_faenas.columns = ["codigo_faena", "embarcacion", "armador", "matricula","fecha_inicio_faena","fecha_fin_faena"]

    return data_faenas

def processing_calas(data_calas):
    # Eliminar la primera columna
    data_calas = data_calas.iloc[:, 1:]

    # Renombrar las columnas
    data_calas.columns = ["codigo_faena", "n_cala", "fecha_inicio_cala",
                          "fecha_fin_cala", "latitud_inicio", "longitud_inicio", "latitud_fin",
                          "longitud_fin", "tipo_arte", "descripcion", "catch",
                          "estado", "origen_cala", "fecha_registro"]


    columns_to_replace = ["latitud_inicio", "longitud_inicio", "latitud_fin", "longitud_fin"]

    for column in columns_to_replace:
      data_calas[column] = data_calas[column].astype(str).apply(lambda x: x.replace("?", "°"))
    # Procesar los datos
    data_calas["descripcion"] = data_calas["descripcion"].str.strip()
    data_calas["lat_inicial"] = lat_long(data_calas["latitud_inicio"])
    data_calas["lon_inicial"] = lat_long(data_calas["longitud_inicio"])
    data_calas["lat_final"] = lat_long(data_calas["latitud_fin"])
    data_calas["lon_final"] = lat_long(data_calas["longitud_fin"])

    return data_calas


def processing_tallas(data_tallas):
  
    data_tallas = data_tallas.copy()
    # Eliminar columnas específicas
    data_tallas = data_tallas.iloc[:, [1, 2, 3, 4, 6]]

    # Renombrar las columnas
    data_tallas.columns = ["codigo_faena", "n_cala", "descripcion", "talla", "freq"]

    # Convertir a tipo numérico y procesar
    data_tallas["talla"] = pd.to_numeric(data_tallas["talla"], errors="coerce")
    data_tallas["freq"] = pd.to_numeric(data_tallas["freq"], errors="coerce")
    data_tallas["descripcion"] = data_tallas["descripcion"].str.strip()

    data_tallas = data_tallas.pivot_table(index=["codigo_faena", "n_cala", "descripcion"], columns="talla", values="freq", aggfunc="sum").reset_index()

    return data_tallas


def length_weight(length, a, b):
    length = pd.to_numeric(length)
    w = a * (length**b)
    return w


def ponderacion(data, tallas, captura_column, a, b):
    
    talla = data[tallas]
    catch = data[captura_column]

    peso = length_weight(tallas, a, b) * talla

    fp = catch / (1000 * peso.sum(axis=1, skipna=True))

    resultados = pd.DataFrame((fp.values.reshape(-1, 1) * talla.values),
                              index=fp.index, columns=talla.columns)

    return resultados


def number_to_weight(data, tallas, a, b):
    talla = data[tallas]
    peso = length_weight(tallas, a, b) * talla

    return peso


def porc_juveniles(data, tallas_names=None, juv_lim=12):

    if tallas_names is None:
        raise ValueError("Se requiere especificar los nombres de las columnas de tallas.")
    
    total = data[tallas_names].sum(axis = 1, skipna=True)
    
    tallas_juv = tallas_names[pd.to_numeric(tallas_names, errors='coerce') < juv_lim]
    
    juv = data[tallas_juv].sum(axis = 1, skipna = True)
    
    juv = juv*100/total

    return juv


def min_range(data):
    min_column_names = []
    for index, row in data.iterrows():
        # Encontrar el índice del primer valor no nulo en cada fila
        first_non_null_index = row.first_valid_index()
        
        # Obtener el nombre de la columna mínima
        min_column_name = data.columns[data.columns.get_loc(first_non_null_index)]
        min_column_names.append(min_column_name)
        
    return min_column_names
  
def max_range(data):
    min_column_names = []
    for index, row in data.iterrows():
        # Encontrar el índice del primer valor no nulo en cada fila
        first_non_null_index = row.last_valid_index()
        
        # Obtener el nombre de la columna mínima
        min_column_name = data.columns[data.columns.get_loc(first_non_null_index)]
        min_column_names.append(min_column_name)
        
    return min_column_names


def salto_indices_de_1(index):
    try:
        # Intenta convertir los índices a números
        index = [float(i) for i in index]
    except ValueError:
        return False  # No es posible convertir a números, no es un salto de 1 en 1
    
    return all(index[i] + 1 == index[i + 1] for i in range(len(index) - 1))


def remove_high_row(row, prop=0.9):
    non_null_elements = row.dropna().values
    normalized_values = non_null_elements / np.sum(non_null_elements)

    return any(normalized_values > prop)


def merge_tallasfaenas_calas(data_calas, data_tallasfaenas):
    # Seleccionar columnas con nombres que contienen al menos un dígito del 1 al 9
    tallas_columns = data_tallasfaenas.filter(regex="[1-9]")

    # Obtener nombres de las columnas seleccionadas
    tallas = tallas_columns.columns.tolist()

    # Procesamiento de 'catch_sps'
    data_calas['descripcion'] = data_calas['descripcion'].str.strip()
    catch_sps = data_calas[data_calas['descripcion'].notna() & (data_calas['descripcion'] != "")].copy()
    
    catch_sps['catch'] = pd.to_numeric(catch_sps['catch'])
    catch_sps = catch_sps.groupby(['codigo_faena', 'n_cala', 'descripcion'])['catch'].sum().reset_index()
    catch_sps['descripcion_catch'] = 'catch_' + catch_sps['descripcion'].astype(str)
    catch_sps = catch_sps.pivot_table(index=['codigo_faena', 'n_cala'], columns='descripcion_catch', values='catch', aggfunc='first').reset_index()
  
    data_tallasfaenas['descripcion'] = data_tallasfaenas['descripcion'].str.strip()
    data_ranges = data_tallasfaenas[data_tallasfaenas['descripcion'].notna() & (data_tallasfaenas['descripcion'] != "")].copy()

    data_ranges['min_rango'] = min_range(data_ranges[tallas])
    data_ranges['descripcion_min'] = 'min_' + data_ranges['descripcion'].astype(str)
    pivot_table_min = data_ranges.pivot_table(index=['codigo_faena', 'n_cala'], columns='descripcion_min', values='min_rango', aggfunc='first').reset_index()
  
    data_ranges['max_rango'] = max_range(data_ranges[tallas])
    data_ranges['descripcion_max'] = 'max_' + data_ranges['descripcion'].astype(str)
    pivot_table_max = data_ranges.pivot_table(index=['codigo_faena', 'n_cala'], columns='descripcion_max', values='max_rango', aggfunc='first').reset_index()


    # Combinar 'min_sps' y 'max_sps'
    min_max_sps = pd.merge(pivot_table_min, pivot_table_max, on=['codigo_faena', 'n_cala'], how='outer')

    # Combinar 'data_tallasfaenas' y 'min_max_sps'
    tallas_total = pd.merge(catch_sps, min_max_sps, on=['codigo_faena', 'n_cala'], how='outer')

    tallas_total = pd.merge(data_tallasfaenas, tallas_total, on=['codigo_faena', 'n_cala'], how='outer')

    # Combinar 'catch_sps' y 'tallas_total'
    total_data = pd.merge(tallas_total, data_calas.drop('catch', axis = 1), on=['codigo_faena', 'n_cala', 'descripcion'], how='outer')

    # Eliminar duplicados
    final_data = total_data[~total_data.duplicated(['codigo_faena', 'n_cala', 'descripcion'])]

    return final_data


def distancia_costa(lat, lon, costa):
    lat_lon = np.column_stack((lat, lon))
    
    def calcular_distancia(punto):
        distancias_km = np.array([haversine(punto, (lat, lon)) for lat, lon in costa.values])
        distancia_minima_km = np.min(distancias_km)
        distancia_minima_mn = distancia_minima_km * 0.539957  # Conversión a millas náuticas
        return distancia_minima_mn
    
    distancias_minimas_mn = np.apply_along_axis(calcular_distancia, 1, lat_lon)
    
    return distancias_minimas_mn
  

def puntos_tierra(x, y, shoreline):
    x = pd.to_numeric(x, errors='coerce').round(3)
    y = pd.to_numeric(y, errors='coerce').round(3)
    resultados = []

    for i in range(len(y)):
        if pd.notna(x.iloc[i]) and pd.notna(y.iloc[i]):
            base_corr1 = shoreline[np.isclose(shoreline['Lat'].round(3), y.iloc[i])]
            base_corr2 = base_corr1.iloc[0, :].values if not base_corr1.empty else [np.nan, np.nan]

            base_corr00 = pd.DataFrame([base_corr2], columns=['LonL', 'LatL'])
            base_corr00['LonP'] = x.iloc[i]
            distancia = (base_corr00['LonP'].astype(float) - base_corr00['LonL'].astype(float)) * -1

            if distancia.iloc[0] < 0:
                resultados.append('tierra')
            else:
                resultados.append('ok')
        else:
            resultados.append(np.nan)

    return resultados

