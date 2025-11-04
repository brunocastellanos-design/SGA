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
    "IPC": "https://api.worldbank.org/v2/en/indicator/FP.CPI.TOTL.ZG?downloadformat=excel",
    "Acceso_Agua_Potable": "https://api.worldbank.org/v2/en/indicator/SH.H2O.BASW.ZS?downloadformat=excel",
    "Acceso_Saneamiento": "https://api.worldbank.org/v2/es/indicator/SH.STA.BASS.ZS?downloadformat=excel"
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
# 7️⃣ Limpieza y cálculo de ratio turistas/población
# ----------------------------------------------------------------------
numericas = [
    "Cantidad_Turistas_Año", "Poblacion_Destino",
    "Control_Corrupcion", "Estado_Derecho", "Efectividad_Gubernamental", "Rendicion_Cuentas",
    "Estabilidad_Politica", "Calidad_Regulatoria", "Homicidios",
    "Crecimiento_Poblacional", "IPC", "Pobreza_Poblacion_Porcentual",
    "Pobreza_Multidimennsional_Porcentual", "Porcentaje_Edad_Laboral",
    "Porcentaje_Edad_Laboral_Estudios", "Tasa_Desempleo"
]

for col in numericas:
    if col in Datos_Fecha.columns:
        Datos_Fecha[col] = pd.to_numeric(Datos_Fecha[col].astype(str).str.replace(",", "").str.strip(), errors='coerce')

if "Cantidad_Turistas_Año" in Datos_Fecha.columns and "Poblacion_Destino" in Datos_Fecha.columns:
    Datos_Fecha["ratio_turistas_residentes"] = Datos_Fecha["Cantidad_Turistas_Año"] / Datos_Fecha["Poblacion_Destino"] * 100


# ----------------------------------------------------------------------
# 8️⃣ Categorización de riesgo vectorizada con tipo object
# ----------------------------------------------------------------------
def categorizar_npselect(df, col, condiciones, valores):
    df[f"{col}_cat"] = np.select(condiciones, valores, default=None).astype(object)

# Gobernanza
gob_cols = ["Control_Corrupcion", "Estado_Derecho", "Efectividad_Gubernamental", "Rendicion_Cuentas"]
for col in gob_cols:
    if col in Datos_Fecha.columns:
        condiciones = [
            Datos_Fecha[col] >= 1.0,
            (Datos_Fecha[col] >= 0.0) & (Datos_Fecha[col] < 1.0),
            (Datos_Fecha[col] >= -1.0) & (Datos_Fecha[col] <= -0.01),
            Datos_Fecha[col] < -1.0
        ]
        valores = ["BAJO ⭣", "MEDIO ⭤", "ALTO ⭡", "MUY ALTO ⚠"]
        categorizar_npselect(Datos_Fecha, col, condiciones, valores)

# Percentiles de ranking
rank_cols = ["Estabilidad_Politica", "Calidad_Regulatoria"]
for col in rank_cols:
    if col in Datos_Fecha.columns:
        condiciones = [
            Datos_Fecha[col] >= 75,
            (Datos_Fecha[col] >= 50) & (Datos_Fecha[col] < 75),
            (Datos_Fecha[col] >= 25) & (Datos_Fecha[col] <= 49),
            Datos_Fecha[col] < 25
        ]
        valores = ["BAJO ⭣", "MEDIO ⭤", "ALTO ⭡", "MUY ALTO ⚠"]
        categorizar_npselect(Datos_Fecha, col, condiciones, valores)

# Homicidios
if "Homicidios" in Datos_Fecha.columns:
    condiciones = [
        Datos_Fecha["Homicidios"] < 5,
        (Datos_Fecha["Homicidios"] >= 5) & (Datos_Fecha["Homicidios"] <= 15),
        (Datos_Fecha["Homicidios"] > 15) & (Datos_Fecha["Homicidios"] <= 30),
        Datos_Fecha["Homicidios"] > 30
    ]
    valores = ["BAJO ⭣", "MEDIO ⭤", "ALTO ⭡", "MUY ALTO ⚠"]
    categorizar_npselect(Datos_Fecha, "Homicidios", condiciones, valores)

# Crecimiento Poblacional
if "Crecimiento_Poblacional" in Datos_Fecha.columns:
    condiciones = [
        Datos_Fecha["Crecimiento_Poblacional"] < 0,
        (Datos_Fecha["Crecimiento_Poblacional"] >= 0) & (Datos_Fecha["Crecimiento_Poblacional"] <= 1),
        (Datos_Fecha["Crecimiento_Poblacional"] > 1) & (Datos_Fecha["Crecimiento_Poblacional"] <= 2),
        Datos_Fecha["Crecimiento_Poblacional"] > 2
    ]
    valores = ["ALTO ⭡", "MEDIO ⭤", "BAJO ⭣", "ALTO ⭡"]
    categorizar_npselect(Datos_Fecha, "Crecimiento_Poblacional", condiciones, valores)

# IPC
if "IPC" in Datos_Fecha.columns:
    bins = [-np.inf, 3, 15, np.inf]
    labels = ["BAJO ⭣", "MEDIO ⭤", "ALTO ⭡"]
    Datos_Fecha["IPC_cat"] = pd.cut(Datos_Fecha["IPC"], bins=bins, labels=labels, right=False).astype(object)

# Pobreza_Poblacion_Porcentual
if "Pobreza_Poblacion_Porcentual" in Datos_Fecha.columns:
    condiciones = [
        Datos_Fecha["Pobreza_Poblacion_Porcentual"] < 10,
        (Datos_Fecha["Pobreza_Poblacion_Porcentual"] >= 10) & (Datos_Fecha["Pobreza_Poblacion_Porcentual"] <= 50),
        Datos_Fecha["Pobreza_Poblacion_Porcentual"] > 50
    ]
    valores = ["BAJO ⭣", "MEDIO ⭤", "ALTO ⭡"]
    categorizar_npselect(Datos_Fecha, "Pobreza_Poblacion_Porcentual", condiciones, valores)

# Pobreza_Multidimennsional_Porcentual
if "Pobreza_Multidimennsional_Porcentual" in Datos_Fecha.columns:
    condiciones = [
        Datos_Fecha["Pobreza_Multidimennsional_Porcentual"] < 10,
        (Datos_Fecha["Pobreza_Multidimennsional_Porcentual"] >= 10) & (Datos_Fecha["Pobreza_Multidimennsional_Porcentual"] <= 30),
        Datos_Fecha["Pobreza_Multidimennsional_Porcentual"] > 30
    ]
    valores = ["BAJO ⭣", "MEDIO ⭤", "ALTO ⭡"]
    categorizar_npselect(Datos_Fecha, "Pobreza_Multidimennsional_Porcentual", condiciones, valores)

# Porcentaje_Edad_Laboral
if "Porcentaje_Edad_Laboral" in Datos_Fecha.columns:
    condiciones = [
        Datos_Fecha["Porcentaje_Edad_Laboral"] < 55,
        (Datos_Fecha["Porcentaje_Edad_Laboral"] >= 55) & (Datos_Fecha["Porcentaje_Edad_Laboral"] <= 70),
        Datos_Fecha["Porcentaje_Edad_Laboral"] > 70
    ]
    valores = ["ALTO ⭡", "MEDIO ⭤", "ALTO ⭡"]
    categorizar_npselect(Datos_Fecha, "Porcentaje_Edad_Laboral", condiciones, valores)

# Porcentaje_Edad_Laboral_Estudios
if "Porcentaje_Edad_Laboral_Estudios" in Datos_Fecha.columns:
    condiciones = [
        Datos_Fecha["Porcentaje_Edad_Laboral_Estudios"] >= 60,
        (Datos_Fecha["Porcentaje_Edad_Laboral_Estudios"] >= 30) & (Datos_Fecha["Porcentaje_Edad_Laboral_Estudios"] <= 59),
        Datos_Fecha["Porcentaje_Edad_Laboral_Estudios"] < 30
    ]
    valores = ["BAJO ⭣", "MEDIO ⭤", "ALTO ⭡"]
    categorizar_npselect(Datos_Fecha, "Porcentaje_Edad_Laboral_Estudios", condiciones, valores)

# Tasa_Desempleo
if "Tasa_Desempleo" in Datos_Fecha.columns:
    condiciones = [
        Datos_Fecha["Tasa_Desempleo"] < 6,
        (Datos_Fecha["Tasa_Desempleo"] >= 6) & (Datos_Fecha["Tasa_Desempleo"] <= 10),
        Datos_Fecha["Tasa_Desempleo"] > 10
    ]
    valores = ["BAJO ⭣", "MEDIO ⭤", "ALTO ⭡"]
    categorizar_npselect(Datos_Fecha, "Tasa_Desempleo", condiciones, valores)

# ratio_turistas_residentes
if "ratio_turistas_residentes" in Datos_Fecha.columns:
    condiciones = [
        Datos_Fecha["ratio_turistas_residentes"] < 100,
        (Datos_Fecha["ratio_turistas_residentes"] >= 100) & (Datos_Fecha["ratio_turistas_residentes"] <= 300),
        Datos_Fecha["ratio_turistas_residentes"] > 300
    ]
    valores = ["BAJO ⭣", "MEDIO ⭤", "ALTO ⭡"]
    categorizar_npselect(Datos_Fecha, "ratio_turistas_residentes", condiciones, valores)

# ----------------------------------------------------------------------
# 9️⃣ Resultado final
# ----------------------------------------------------------------------
Datos_Fecha = Datos_Fecha.iloc[-10:]  # Últimos 10 años
print("\nVista preliminar de Datos_Fecha:")
print(Datos_Fecha.head())

Datos_Fecha.to_excel("Historico.xlsx", index=False)

print("\n✅ Datos guardados en 'Historico.xlsx'.")

