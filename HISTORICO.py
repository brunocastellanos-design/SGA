import pandas as pd
import numpy as np
import json

# --- Leer configuración ---
with open("config.json", "r", encoding="utf-8") as f:
    config = json.load(f)

# Obtener el código de país desde el JSON
if "country_code" not in config:
    raise ValueError("El archivo config.json debe contener la clave 'country_code'.")

country_code_upper = config["country_code"].upper()

# --- URLs de los datasets del World Bank ---
urls = {
    "Poblacion_Destino": "https://api.worldbank.org/v2/en/indicator/SP.POP.TOTL?downloadformat=excel",
    "Crecimiento_Poblacional": "https://api.worldbank.org/v2/en/indicator/SP.POP.GROW?downloadformat=excel",
    "Pobreza_Poblacion_Porcentual": "https://api.worldbank.org/v2/en/indicator/SI.POV.DDAY?downloadformat=excel",
    "Pobreza_Multidimensional_Porcentual": "https://api.worldbank.org/v2/en/indicator/SI.POV.MPWB?downloadformat=excel",
    "Porcentaje_Edad_Laboral": "https://api.worldbank.org/v2/en/indicator/SP.POP.1564.TO.ZS?downloadformat=excel",
    "Porcentaje_Edad_Laboral_Estudios": "https://api.worldbank.org/v2/en/indicator/SE.SEC.CUAT.UP.ZS?downloadformat=excel",
    "Tasa_Desempleo": "https://api.worldbank.org/v2/en/indicator/SL.UEM.TOTL.NE.ZS?downloadformat=excel",
    "Cantidad_Turistas_Año": "https://api.worldbank.org/v2/en/indicator/ST.INT.ARVL?downloadformat=excel",
    "Tasa_Participacion_Fuerza_Laboral": "https://api.worldbank.org/v2/en/indicator/SL.TLF.CACT.NE.ZS?downloadformat=excel",
    "Inseguridad_Alimentaria": "https://api.worldbank.org/v2/en/indicator/SN.ITK.MSFI.ZS?downloadformat=excel",
    "Estabilidad_Politica": "https://api.worldbank.org/v2/en/indicator/PV.PER.RNK?downloadformat=excel",
    "Efectividad_Gubernamental": "https://api.worldbank.org/v2/en/indicator/GE.EST?downloadformat=excel",
    "Control_Corrupcion": "https://api.worldbank.org/v2/en/indicator/CC.EST?downloadformat=excel",
    "Rendicion_Cuentas": "https://api.worldbank.org/v2/en/indicator/VA.EST?downloadformat=excel",
    "Estado_Derecho": "https://api.worldbank.org/v2/en/indicator/RL.EST?downloadformat=excel",
    "Calidad_Regulatoria": "https://api.worldbank.org/v2/en/indicator/RQ.PER.RNK?downloadformat=excel",
    "Homicidios": "https://api.worldbank.org/v2/en/indicator/VC.IHR.PSRC.P5?downloadformat=excel"
}

# ----------------------------------------------------------------------
# Función de Lectura REAL del World Bank (Adaptada de tu código original)
# ----------------------------------------------------------------------
def leer_excel_codigo(url, codigo=config["country_code"], codigo_upper=country_code_upper):
    """
    Lee un Excel del World Bank, filtra por Country Code y transforma los años a filas.
    """
    try:
        # header=3 porque la cabecera real está en la cuarta fila (índice 3)
        df = pd.read_excel(url, sheet_name="Data", header=3) 
    except Exception as e:
        print(f"Error al leer la URL {url}: {e}")
        return pd.DataFrame()

    # Buscar fila del país
    fila = df[df["Country Code"] == codigo]
    if fila.empty:
        raise ValueError(f"No se encontró el país '{codigo}' en el dataset.")
    
    # Columnas de años
    columnas_años = [c for c in df.columns if str(c).isdigit()]
    
    # Melt (transformación de columnas a filas)
    df_result = fila[["Country Code"] + columnas_años].melt(
        id_vars="Country Code", var_name="año", value_name=codigo_upper
    )
    
    # Conversión de tipos de datos
    df_result[codigo_upper] = pd.to_numeric(df_result[codigo_upper], errors='coerce')
    df_result['año'] = pd.to_numeric(df_result['año'], errors='coerce').astype('Int64')
    
    # Devolver sin la columna 'Country Code'
    return df_result.drop(columns=["Country Code"])

# ----------------------------------------------------------------------
## 1. Carga, Procesamiento y Unión de Datos
# ----------------------------------------------------------------------

datasets = {}
print(f"Iniciando la carga de datos para el país: {country_code_upper}")

for nombre, url in urls.items():
    try:
        # Usamos la función real 'leer_excel_codigo'
        df = leer_excel_codigo(url)
        if not df.empty:
            df.rename(columns={country_code_upper: nombre}, inplace=True)
            datasets[nombre] = df
            print(f"✅ {nombre} cargado.")
    except Exception as e:
        print(f"❌ Error al procesar {nombre}: {e}")

# Unir datasets
if not datasets:
    print("No se cargó ningún dataset. Finalizando el proceso.")
else:
    # Inicio con el primer dataset (Poblacion_Destino)
    Datos_Fecha = datasets["Poblacion_Destino"]
    
    # Fusión con los datasets restantes usando how="outer" para mantener todos los años
    for key in list(datasets.keys())[1:]:
        Datos_Fecha = pd.merge(Datos_Fecha, datasets[key], on="año", how="outer")

    # Limpieza final: eliminar filas con año nulo y asegurar el tipo de dato
    Datos_Fecha = Datos_Fecha[pd.notna(Datos_Fecha['año'])].reset_index(drop=True)
    Datos_Fecha['año'] = Datos_Fecha['año'].astype(int)

# ----------------------------------------------------------------------
## 2. Limpieza y Conversión Numérica
# ----------------------------------------------------------------------

numericas = [
    "Cantidad_Turistas_Año", "Poblacion_Destino",
    "Control_Corrupcion", "Estado_Derecho", "Efectividad_Gubernamental", "Rendicion_Cuentas",
    "Estabilidad_Politica", "Calidad_Regulatoria", "Homicidios"
]

# Convertir a numérico y limpiar cualquier formato de cadena (aunque el World Bank ya es limpio)
for col in numericas:
    # Usamos .astype(str) antes de .str.replace para evitar errores si la columna tiene NaN/None
    Datos_Fecha[col] = pd.to_numeric(
        Datos_Fecha[col].astype(str).str.replace(",", "").str.strip(), 
        errors='coerce'
    )

# Calcular ratio turistas / residentes
Datos_Fecha["ratio_turistas_residentes"] = (
    Datos_Fecha["Cantidad_Turistas_Año"] / Datos_Fecha["Poblacion_Destino"] * 100
)

# ----------------------------------------------------------------------
## 3. Aplicación de Condiciones de Riesgo (Categorización)
# ----------------------------------------------------------------------

# A. Indicadores de Gobernanza (Rango de -2.5 a 2.5)
for col in ["Control_Corrupcion", "Estado_Derecho", "Efectividad_Gubernamental", "Rendicion_Cuentas"]:
    Datos_Fecha[col] = Datos_Fecha[col].apply(
        lambda x: (
            "Riesgo bajo" if pd.notna(x) and x >= 1.0 else
            "Riesgo medio" if pd.notna(x) and 0.0 <= x <= 0.99 else
            "Riesgo alto" if pd.notna(x) and -1.0 <= x <= -0.01 else
            "Riesgo muy alto" if pd.notna(x) and x < -1.0 else np.nan
        )
    )

# B. Percentiles de Ranking (Rango de 0 a 100)
for col in ["Estabilidad_Politica", "Calidad_Regulatoria"]:
    Datos_Fecha[col] = Datos_Fecha[col].apply(
        lambda x: (
            "Bajo" if pd.notna(x) and x >= 75 else # Bajo riesgo
            "Medio" if pd.notna(x) and 50 <= x <= 74 else
            "Alto" if pd.notna(x) and 25 <= x <= 49 else
            "Muy alto" if pd.notna(x) and x < 25 else np.nan # Muy alto riesgo
        )
    )

# C. Tasa de Homicidios (Tasa por 100,000)
Datos_Fecha["Homicidios"] = Datos_Fecha["Homicidios"].apply(
    lambda x: (
        "Riesgo bajo — niveles bajos de violencia" if pd.notna(x) and x < 5 else
        "Riesgo medio — violencia moderada o localizada" if pd.notna(x) and 5 <= x <= 15 else
        "Riesgo alto — violencia generalizada o crimen estructural" if pd.notna(x) and 15 < x <= 30 else
        "Riesgo muy alto — violencia extrema o debilidad institucional severa" if pd.notna(x) and x > 30 else np.nan
    )
)

# ----------------------------------------------------------------------
## 4. Guardar Resultado
# ----------------------------------------------------------------------

Datos_Fecha.to_excel("Historico.xlsx", index=False)

print("\n--- Proceso Finalizado ---")
print(f"✅ DataFrame guardado en 'Historico.xlsx'. Dimensiones: {Datos_Fecha.shape}")
print("\n--- Vista Preliminar (Categorizada) ---")
print(Datos_Fecha.sort_values(by='año', ascending=False).head())