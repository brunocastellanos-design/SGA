import pandas as pd
import numpy as np
from unidecode import unidecode

# --- Funciones auxiliares ---
def normalizar_columnas(df):
    df.columns = [unidecode(str(c).strip().lower()) for c in df.columns]
    return df

def detectar_columna_pais(df, pais="tanzania"):
    pais_norm = unidecode(pais.strip().lower())
    for col in df.columns:
        col_norm = unidecode(str(col).strip().lower().replace(" ", ""))
        if pais_norm.replace(" ", "") in col_norm:
            return col
    raise ValueError(f"No se encontró ninguna columna para '{pais}' en el dataset.")

def leer_excel(url, sheet_name="Data", header=3, pais="tanzania"):
    df = pd.read_excel(url, sheet_name=sheet_name, header=header)
    column_headers = df.columns.tolist()[1:]
    df = df.T
    df.columns = df.iloc[0]
    df = df[1:]
    df['encabezado'] = column_headers
    cols = ['encabezado'] + [c for c in df.columns if c != 'encabezado']
    df = df[cols].reset_index(drop=True)
    df = normalizar_columnas(df)
    col_año = df.columns[0]
    col_pais = detectar_columna_pais(df, pais)
    df = df[[col_año, col_pais]]
    df.rename(columns={col_año: "año", col_pais: "tanzania"}, inplace=True)
    df["tanzania"] = pd.to_numeric(df["tanzania"].astype(str).str.replace(",", "").str.strip(), errors='coerce')
    return df

# --- URLs ---
urls = {
    "Poblacion_Destino": "https://api.worldbank.org/v2/en/indicator/SP.POP.TOTL?downloadformat=excel",
    "Crecimiento_Poblacional": "https://api.worldbank.org/v2/en/indicator/SP.POP.GROW?downloadformat=excel",
    "Pobreza_Poblacion_Porcentual": "https://api.worldbank.org/v2/en/indicator/SI.POV.DDAY?downloadformat=excel",
    "Pobreza_Multidimennsional_Porcentual": "https://api.worldbank.org/v2/en/indicator/SI.POV.MPWB?downloadformat=excel",
    "Porcentaje_Edad_Laboral": "https://api.worldbank.org/v2/en/indicator/SP.POP.1564.TO.ZS?downloadformat=excel",
    "Porcentaje_Edad_Laboral_Estudios": "https://api.worldbank.org/v2/en/indicator/SE.SEC.CUAT.UP.ZS?downloadformat=excel",
    "Tasa_Desempeleo": "https://api.worldbank.org/v2/es/indicator/SL.UEM.TOTL.NE.ZS?downloadformat=excel",
    "Cantidad_Turistas_Año": "https://api.worldbank.org/v2/en/indicator/ST.INT.ARVL?downloadformat=excel",
    "Jornada_Laboral_Promedio": "https://api.worldbank.org/v2/en/indicator/SL.TLF.CACT.NE.ZS?downloadformat=excel",
    "Inseguridad Alimentaria": "https://api.worldbank.org/v2/es/indicator/SN.ITK.MSFI.ZS?downloadformat=excel",
    "Estabilidad_Politica": "https://api.worldbank.org/v2/en/indicator/PV.PER.RNK?downloadformat=excel",
    "Efectividad_Gubernamental": "https://api.worldbank.org/v2/es/indicator/GE.EST?downloadformat=excel",
    "Control_Corrupcion": "https://api.worldbank.org/v2/es/indicator/CC.EST?downloadformat=excel",
    "Rendicion_Cuentas": "https://api.worldbank.org/v2/es/indicator/VA.EST?downloadformat=excel",
    "Estado_Derecho": "https://api.worldbank.org/v2/es/indicator/RL.EST?downloadformat=excel",
    "Calidad_Regulatoria": "https://api.worldbank.org/v2/es/indicator/RQ.PER.RNK?downloadformat=excel",
    "Homicidios": "https://api.worldbank.org/v2/es/indicator/VC.IHR.PSRC.P5?downloadformat=excel"
}

# --- Leer y procesar datasets ---
datasets = {}
for nombre, url in urls.items():
    datasets[nombre] = leer_excel(url, pais="tanzania")
    datasets[nombre].rename(columns={"tanzania": nombre}, inplace=True)

# --- Unir datasets ---
Datos_Fecha = datasets["Poblacion_Destino"]
for key in list(datasets.keys())[1:]:
    Datos_Fecha = pd.merge(Datos_Fecha, datasets[key], on="año", how="left")

# Filtrar filas válidas y convertir año a int
Datos_Fecha = Datos_Fecha[Datos_Fecha['año'].apply(lambda x: str(x).isdigit())].reset_index(drop=True)
Datos_Fecha['año'] = Datos_Fecha['año'].astype(int)

# Convertir columnas a numérico
numericas = [
    "Cantidad_Turistas_Año", "Poblacion_Destino",
    "Control_Corrupcion", "Estado_Derecho", "Efectividad_Gubernamental", "Rendicion_Cuentas",
    "Estabilidad_Politica", "Calidad_Regulatoria", "Homicidios"
]

for col in numericas:
    Datos_Fecha[col] = pd.to_numeric(Datos_Fecha[col].astype(str).str.replace(",", "").str.strip(), errors='coerce')

# Calcular ratio turistas / residentes
Datos_Fecha["ratio_turistas_residentes"] = (
    Datos_Fecha["Cantidad_Turistas_Año"] / Datos_Fecha["Poblacion_Destino"] * 100
)

# --- Aplicar condiciones de riesgo directamente en las mismas columnas ---

# Control_Corrupcion, Estado_Derecho, Efectividad_Gubernamental, Rendicion_Cuentas
for col in ["Control_Corrupcion", "Estado_Derecho", "Efectividad_Gubernamental", "Rendicion_Cuentas"]:
    Datos_Fecha[col] = Datos_Fecha[col].apply(
        lambda x: (
            "Riesgo bajo" if pd.notna(x) and x >= 1.0 else
            "Riesgo medio" if pd.notna(x) and 0.0 <= x <= 0.99 else
            "Riesgo alto" if pd.notna(x) and -1.0 <= x <= -0.01 else
            "Riesgo muy alto" if pd.notna(x) and x < -1.0 else np.nan
        )
    )

# Estabilidad_Politica, Calidad_Regulatoria
for col in ["Estabilidad_Politica", "Calidad_Regulatoria"]:
    Datos_Fecha[col] = Datos_Fecha[col].apply(
        lambda x: (
            "Bajo" if pd.notna(x) and x >= 75 else
            "Medio" if pd.notna(x) and 50 <= x <= 74 else
            "Alto" if pd.notna(x) and 25 <= x <= 49 else
            "Muy alto" if pd.notna(x) and x < 25 else np.nan
        )
    )

# Homicidios
Datos_Fecha["Homicidios"] = Datos_Fecha["Homicidios"].apply(
    lambda x: (
        "Riesgo bajo — niveles bajos de violencia" if pd.notna(x) and x < 5 else
        "Riesgo medio — violencia moderada o localizada" if pd.notna(x) and 5 <= x <= 15 else
        "Riesgo alto — violencia generalizada o crimen estructural" if pd.notna(x) and 15 < x <= 30 else
        "Riesgo muy alto — violencia extrema o debilidad institucional severa" if pd.notna(x) and x > 30 else np.nan
    )
)

# --- Guardar Excel final con el mismo nombre ---
Datos_Fecha.to_excel("Historico.xlsx", index=False)

print(Datos_Fecha.shape)
print(Datos_Fecha.head())
