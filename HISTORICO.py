import pandas as pd

# Función para leer y procesar Excel desde URL
def leer_excel(url, sheet_name="Data", fila_header=3, pais="Tanzania"):
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
    
    # Eliminamos primeras 3 filas si necesario
    df = df.iloc[3:].reset_index(drop=True)
    
    # Limpiamos nombres de columnas
    df.columns = df.columns.str.strip()
    
    # Renombramos la primera columna a "Año"
    df.rename(columns={df.columns[0]: "Año"}, inplace=True)
    
    # Nos quedamos solo con el país deseado
    if pais in df.columns:
        df = df[["Año", pais]]
    else:
        raise ValueError(f"La columna '{pais}' no existe en el dataset.")
    
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

# Leemos todos los datasets
datasets = {}
for nombre, url in urls.items():
    pais = "Tanzania"  # Establecemos país por defecto
    datasets[nombre] = leer_excel(url, pais=pais)
    # Renombramos columna del país al nombre del dataset
    datasets[nombre].rename(columns={pais: nombre}, inplace=True)

# Unimos todos los datasets por "Año"
Datos_Fecha = datasets["Poblacion_Destino"]
for key in list(datasets.keys())[1:]:
    Datos_Fecha = pd.merge(Datos_Fecha, datasets[key], on="Año", how="left")

# Creamos columna adicional
Datos_Fecha["Ratio turistas / residentes"] = (
    Datos_Fecha["Cantidad_Turistas_Año"] / Datos_Fecha["Poblacion_Destino"] * 100
)

# Mostramos las primeras filas
print(Datos_Fecha.shape)
print(Datos_Fecha.head())
