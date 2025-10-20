import pandas as pd
from unidecode import unidecode

# Normaliza nombres de columnas
def normalizar_columnas(df):
    df.columns = [unidecode(str(c).strip().lower()) for c in df.columns]
    return df

# Detecta columna del país tras la transposición
def detectar_columna_pais(df, pais="tanzania"):
    pais_norm = unidecode(pais.strip().lower())
    for col in df.columns:
        col_norm = unidecode(str(col).strip().lower().replace(" ", ""))
        if pais_norm.replace(" ", "") in col_norm:
            return col
    raise ValueError(f"No se encontró ninguna columna para '{pais}' en el dataset.")

# Función para leer y procesar Excel
def leer_excel(url, sheet_name="Data", header=3, pais="tanzania"):
    df = pd.read_excel(url, sheet_name=sheet_name, header=header)
    
    # Guardar nombres originales de columnas (excepto la primera)
    column_headers = df.columns.tolist()[1:]
    
    # Transponer
    df = df.T
    df.columns = df.iloc[0]      # Primera fila como header
    df = df[1:]                  # Eliminar primera fila que ya es header
    
    # Crear columna de encabezado original
    df['encabezado'] = column_headers
    cols = ['encabezado'] + [c for c in df.columns if c != 'encabezado']
    df = df[cols].reset_index(drop=True)
    
    # Normalizar columnas
    df = normalizar_columnas(df)
    
    # Columna de años es la primera después de transponer
    col_año = df.columns[0]
    
    # Detectar columna del país
    col_pais = detectar_columna_pais(df, pais)
    
    # Seleccionar solo año y país
    df = df[[col_año, col_pais]]
    
    # Renombrar columnas
    df.rename(columns={col_año: "año", col_pais: "tanzania"}, inplace=True)
    
    # Convertir años a int si es posible
    df = df.reset_index(drop=True)
    try:
        df["año"] = df["año"].astype(int)
    except:
        pass
    
    # Convertir columna del país a números
    df["tanzania"] = pd.to_numeric(df["tanzania"].astype(str).str.replace(",", "").str.strip(), errors='coerce')
    
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

# Convertir columnas necesarias a números antes de calcular ratio
Datos_Fecha["Cantidad_Turistas_Año"] = pd.to_numeric(Datos_Fecha["Cantidad_Turistas_Año"], errors='coerce')
Datos_Fecha["Poblacion_Destino"] = pd.to_numeric(Datos_Fecha["Poblacion_Destino"], errors='coerce')

# Calcular ratio turistas / residentes
Datos_Fecha["ratio_turistas_residentes"] = (
    Datos_Fecha["Cantidad_Turistas_Año"] / Datos_Fecha["Poblacion_Destino"] * 100
)

# Mostrar resultado
print(Datos_Fecha.shape)
print(Datos_Fecha.head())

# Guardar CSV final
Datos_Fecha.to_csv("Datos_Fecha_Tanzania.csv", index=False)
