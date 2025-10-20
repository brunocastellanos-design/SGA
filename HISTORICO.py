import pandas as pd
from unidecode import unidecode

# Función para normalizar columnas
def normalizar_columnas(df):
    df.columns = [unidecode(str(c).strip().lower()) for c in df.columns]
    return df

# Función para leer y procesar Excel desde URL
def leer_excel(url, sheet_name="Data", fila_header=3, pais="tanzania"):
    # Leemos el Excel
    df = pd.read_excel(url, sheet_name=sheet_name, header=fila_header)
    
    # Guardamos nombres originales
    column_headers = df.columns.tolist()[1:]
    
    # Transponemos
    df = df.T
    df.columns = df.iloc[0]
    df = df[1:]
    
    # Creamos columna con encabezados originales
    df['Encabezado'] = column_headers
    cols = ['Encabezado'] + [c for c in df.columns if c != 'Encabezado']
    df = df[cols].reset_index(drop=True)
    
    # Eliminamos primeras 3 filas (corrección)
    df = df.iloc[3:].reset_index(drop=True)
    
    # Normalizamos nombres de columnas
    df = normalizar_columnas(df)
    
    # Normalizamos nombre del país
    pais_norm = unidecode(pais.strip().lower())
    
    # Verificamos que exista la columna del país
    if pais_norm not in df.columns:
        raise ValueError(f"No se encontró ninguna columna para '{pais}' en el dataset.")
    
    # Seleccionamos solo "año" y el país
    df = df[["año", pais_norm]]
    
    return df

# URLs de los datasets
urls = {
    "Poblacion_Destino": "https://api.worldbank.org/v2/en/indicator/SP.POP.TOTL?downloadformat=excel",
    "Crecimiento_Poblacional": "https://api.worldbank.org/v2/en/indicator/SP.POP.GROW?downloadformat=excel",
    "Pobreza_Poblacion_Porcentual": "https://api.worldbank.org/v2/en/indicator/SI.POV.DDAY?downloadformat=excel",
    "Porcentaje_Edad_Laboral": "https://api.worldbank.org/v2/en/indicator/SP.POP.1564.TO.ZS?downloadformat=excel",
    "Cantidad_Turistas_Año": "https://api.worldbank.org/v2/en/indicator/ST.INT.ARVL?downloadformat=excel",
    "Estabilidad_Politica": "https://api.worldbank.org/v2/en/indicator/PV.PER.RNK?downloadformat=excel",
    "Control_Corrupcion": "https://api.worldbank.org/v2/es/indicator/CC.EST?downloadformat=excel"
}

# Leemos todos los datasets y normalizamos columnas
datasets = {}
for nombre, url in urls.items():
    datasets[nombre] = leer_excel(url, pais="tanzania")
    # Renombramos la columna del país con el nombre del dataset
    datasets[nombre].rename(columns={"tanzania": nombre}, inplace=True)

# Unimos todos los datasets por "año"
Datos_Fecha = datasets["Poblacion_Destino"]
for key in list(datasets.keys())[1:]:
    Datos_Fecha = pd.merge(Datos_Fecha, datasets[key], on="año", how="left")

# Creamos columna adicional: ratio turistas / residentes
Datos_Fecha["ratio_turistas_residentes"] = (
    Datos_Fecha["Cantidad_Turistas_Año"] / Datos_Fecha["Poblacion_Destino"] * 100
)

# Mostramos dimensiones y primeras filas
print(Datos_Fecha.shape)
print(Datos_Fecha.head())

# Guardar resultado final en CSV (opcional)
Datos_Fecha.to_csv("Datos_Fecha_Tanzania.csv", index=False)
