import pandas as pd
from unidecode import unidecode

# Normaliza columnas (minúsculas, sin tildes, sin espacios)
def normalizar_columnas(df):
    df.columns = [unidecode(str(c).strip().lower()) for c in df.columns]
    return df

# Detecta la columna del país
def detectar_columna_pais(df, pais="tanzania"):
    pais_norm = unidecode(pais.strip().lower())
    for col in df.columns:
        if pais_norm in unidecode(str(col).strip().lower()):
            return col
    raise ValueError(f"No se encontró ninguna columna para '{pais}' en el dataset.")

# Función para leer Excel y preparar DataFrame
def leer_excel(url, sheet_name="Data", fila_header=3, pais="tanzania"):
    # Leer Excel
    df = pd.read_excel(url, sheet_name=sheet_name, header=fila_header)
    
    # Normalizamos columnas
    df = normalizar_columnas(df)
    
    # Suponemos que la primera columna es la de años
    col_año = df.columns[0]
    
    # Detectamos columna del país
    col_pais = detectar_columna_pais(df, pais)
    
    # Seleccionamos solo año y país
    df = df[[col_año, col_pais]]
    
    # Renombramos columnas de forma consistente
    df.rename(columns={col_año: "año", col_pais: "tanzania"}, inplace=True)
    
    # Reindexamos y convertimos años a int si es posible
    df = df.reset_index(drop=True)
    try:
        df["año"] = df["año"].astype(int)
    except:
        pass
    
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

# Leer y procesar datasets
datasets = {}
for nombre, url in urls.items():
    datasets[nombre] = leer_excel(url, pais="tanzania")
    datasets[nombre].rename(columns={"tanzania": nombre}, inplace=True)

# Unir datasets por año
Datos_Fecha = datasets["Poblacion_Destino"]
for key in list(datasets.keys())[1:]:
    Datos_Fecha = pd.merge(Datos_Fecha, datasets[key], on="año", how="left")

# Calcular ratio turistas / residentes
Datos_Fecha["ratio_turistas_residentes"] = (
    Datos_Fecha["Cantidad_Turistas_Año"] / Datos_Fecha["Poblacion_Destino"] * 100
)

# Mostrar resultado
print(Datos_Fecha.shape)
print(Datos_Fecha.head())

# Guardar CSV final
Datos_Fecha.to_csv("Datos_Fecha_Tanzania.csv", index=False)
