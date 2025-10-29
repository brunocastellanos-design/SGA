import pandas as pd
import numpy as np
import json

# ----------------------------------------------------------------------
# 1️⃣ Leer configuración del JSON
# ----------------------------------------------------------------------
with open("config.json", "r", encoding="utf-8") as f:
    config = json.load(f)
nombre_pais_json = config.get("País")
if not nombre_pais_json:
    raise ValueError("El archivo config.json debe contener la clave 'País'.")
print(f"Nombre de país en JSON: {nombre_pais_json}")

# ----------------------------------------------------------------------
# 2️⃣ URLs de los datasets del World Bank
# ----------------------------------------------------------------------
urls = {
    "Pobreza_Multidimennsional_Porcentual": "https://api.worldbank.org/v2/en/indicator/SI.POV.MDIM?downloadformat=excel",
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
    "Homicidios": "https://api.worldbank.org/v2/en/indicator/VC.IHR.PSRC.P5?downloadformat=excel",
    "IPC": "https://api.worldbank.org/v2/en/indicator/FP.CPI.TOTL.ZG?downloadformat=excel"
}

# ----------------------------------------------------------------------
# 3️⃣ Función para buscar el código de país
# ----------------------------------------------------------------------
def buscar_country_code(df, nombre_pais):
    fila = df[df['Country Name'].str.lower().str.strip() == nombre_pais.lower().strip()]
    if not fila.empty:
        return fila['Country Code'].values[0]
    return None

# ----------------------------------------------------------------------
# 4️⃣ Buscar country code en los datasets
# ----------------------------------------------------------------------
country_code_obtenido = None

for nombre, url in urls.items():
    try:
        df = pd.read_excel(url, sheet_name="Data", header=3)
        code = buscar_country_code(df, nombre_pais_json)
        if code:
            country_code_obtenido = code.upper()
            print(f"✅ Código de país encontrado: {country_code_obtenido} (en {nombre})")
            break
    except Exception as e:
        print(f"⚠️ No se pudo leer {nombre} para buscar código de país: {e}")

if not country_code_obtenido:
    raise ValueError(f"No se encontró el país '{nombre_pais_json}' en ninguno de los datasets.")

# ----------------------------------------------------------------------
# 5️⃣ Función para leer y transformar Excel del World Bank
# ----------------------------------------------------------------------
def leer_excel_codigo(url, nombre_indicador, codigo=country_code_obtenido, codigo_upper=None):
    if codigo_upper is None:
        codigo_upper = codigo
    try:
        df = pd.read_excel(url, sheet_name="Data", header=3)
    except Exception as e:
        raise RuntimeError(f"Fallo en la lectura del Excel para {nombre_indicador}: {e}")

    if codigo not in df["Country Code"].unique():
        raise ValueError(f"No se encontró el país '{codigo}' en el dataset de {nombre_indicador}.")

    fila = df[df["Country Code"] == codigo]
    columnas_años = [c for c in df.columns if str(c).isdigit()]

    df_result = fila[["Country Code"] + columnas_años].melt(
        id_vars="Country Code", var_name="año", value_name=codigo_upper
    )
    df_result[codigo_upper] = pd.to_numeric(df_result[codigo_upper], errors='coerce')
    df_result['año'] = pd.to_numeric(df_result['año'], errors='coerce').astype('Int64')
    return df_result.drop(columns=["Country Code"])

# ----------------------------------------------------------------------
# 6️⃣ Carga, procesamiento y unión de datos
# ----------------------------------------------------------------------
datasets = {}
print(f"\nIniciando la carga de datos para el país: {country_code_obtenido}")

for nombre, url in urls.items():
    try:
        df = leer_excel_codigo(url, nombre)
        if not df.empty:
            df.rename(columns={country_code_obtenido: nombre}, inplace=True)
            datasets[nombre] = df
            print(f"✅ {nombre} cargado.")
        else:
            print(f"⚠️ {nombre} no tiene datos para el país.")
    except Exception as e:
        print(f"❌ Error al procesar {nombre}: {e}")

# Unión de datasets
Datos_Fecha = None
initial_key = "Poblacion_Destino"

if datasets and initial_key in datasets:
    Datos_Fecha = datasets[initial_key].copy()
    for key in [k for k in datasets.keys() if k != initial_key]:
        Datos_Fecha = pd.merge(Datos_Fecha, datasets[key], on="año", how="outer")
    Datos_Fecha = Datos_Fecha[pd.notna(Datos_Fecha['año'])].reset_index(drop=True)
    Datos_Fecha['año'] = Datos_Fecha['año'].astype(int)
else:
    raise RuntimeError("No se pudo cargar el dataset base 'Poblacion_Destino'.")

# ----------------------------------------------------------------------
# 7️⃣ Limpieza, conversión y cálculos
# ----------------------------------------------------------------------
numericas = [
    "Cantidad_Turistas_Año", "Poblacion_Destino",
    "Control_Corrupcion", "Estado_Derecho", "Efectividad_Gubernamental", "Rendicion_Cuentas",
    "Estabilidad_Politica", "Calidad_Regulatoria", "Homicidios"
]

for col in numericas:
    if col in Datos_Fecha.columns:
        Datos_Fecha[col] = pd.to_numeric(Datos_Fecha[col].astype(str).str.replace(",", "").str.strip(), errors='coerce')

if "Cantidad_Turistas_Año" in Datos_Fecha.columns and "Poblacion_Destino" in Datos_Fecha.columns:
    Datos_Fecha["ratio_turistas_residentes"] = (
        Datos_Fecha["Cantidad_Turistas_Año"] / Datos_Fecha["Poblacion_Destino"] * 100
    )

# ----------------------------------------------------------------------
# 8️⃣ Categorización de riesgo (sin reemplazar las columnas originales)
# ----------------------------------------------------------------------

# Gobernanza
for col in ["Control_Corrupcion", "Estado_Derecho", "Efectividad_Gubernamental", "Rendicion_Cuentas"]:
    if col in Datos_Fecha.columns:
        Datos_Fecha[f"{col}_cat"] = Datos_Fecha[col].apply(
            lambda x: (
                "BAJO ⭣" if pd.notna(x) and x >= 0.99 else
                "MEDIO ⭤" if pd.notna(x) and 0 <= x < 0.99 else
                "ALTO ⭡" if pd.notna(x) and -1 <= x < 0 else
                "MUY ALTO ⚠" if pd.notna(x) and x < -1 else np.nan
            )
        )

# Percentiles de ranking
for col in ["Estabilidad_Politica", "Calidad_Regulatoria"]:
    if col in Datos_Fecha.columns:
        Datos_Fecha[f"{col}_cat"] = Datos_Fecha[col].apply(
            lambda x: (
                "BAJO ⭣" if pd.notna(x) and x > 74 else
                "MEDIO ⭤" if pd.notna(x) and 50 <= x <= 74 else
                "ALTO ⭡" if pd.notna(x) and 25 <= x <= 49 else
                "MUY ALTO ⚠" if pd.notna(x) and x < 25 else np.nan
            )
        )

# Homicidios
if "Homicidios" in Datos_Fecha.columns:
    Datos_Fecha["Homicidios_cat"] = Datos_Fecha["Homicidios"].apply(
        lambda x: (
            "BAJO ⭣" if pd.notna(x) and x < 5 else
            "MEDIO ⭤" if pd.notna(x) and 5 <= x <= 15 else
            "ALTO ⭡" if pd.notna(x) and 15 < x <= 30 else
            "MUY ALTO ⚠" if pd.notna(x) and x > 30 else np.nan
        )
    )


# ----------------------------------------------------------------------
# 9️⃣ Resultado final
# ----------------------------------------------------------------------
Datos_Fecha = Datos_Fecha.iloc[-10:]  # Últimos 10 años
print("\nVista preliminar de Datos_Fecha:")
print(Datos_Fecha.head())


Datos_Fecha.to_excel("Historico.xlsx", index=False)


print("\n✅ Datos guardados en 'Historico.xlsx'.")

