import pandas as pd
import numpy as np
import json
import io
import requests

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
    "Acceso_Saneamiento": "https://api.worldbank.org/v2/en/indicator/SH.STA.BASS.ZS?downloadformat=excel",
    "Poblacion_Urbana": "https://api.worldbank.org/v2/en/indicator/EN.POP.SLUM.UR.ZS?downloadformat=excel"
}

# ----------------------------------------------------------------------
# 3️⃣ Función para buscar el código de país
# ----------------------------------------------------------------------
def buscar_country_code(df, nombre_pais):
    fila = df[df['Country Name'].str.lower().str.strip() == nombre_pais.lower().strip()]
    return fila['Country Code'].values[0] if not fila.empty else None

# ----------------------------------------------------------------------
# 4️⃣ Buscar country code en los datasets
# ----------------------------------------------------------------------
country_code_obtenido = None
for nombre, url in urls.items():
    try:
        response = requests.get(url, timeout=60)
        xls = pd.ExcelFile(io.BytesIO(response.content))
        sheet = "Data" if "Data" in xls.sheet_names else xls.sheet_names[0]
        df = xls.parse(sheet, header=3)
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
# 5️⃣ Función robusta para leer Excel del World Bank
# ----------------------------------------------------------------------
def leer_excel_codigo(url, nombre_indicador, codigo=country_code_obtenido, codigo_upper=None):
    if codigo_upper is None:
        codigo_upper = codigo

    try:
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        contenido = io.BytesIO(response.content)

        try:
            xls = pd.ExcelFile(contenido)
        except Exception:
            raise RuntimeError("El contenido descargado no parece un archivo Excel válido.")

        hoja_valida = None
        for hoja in xls.sheet_names:
            try:
                df_test = pd.read_excel(xls, sheet_name=hoja, nrows=5)
                if any("Country" in str(c) for c in df_test.columns):
                    hoja_valida = hoja
                    break
            except Exception:
                continue

        if hoja_valida is None:
            raise ValueError(f"No se encontró una hoja válida en el archivo {url}")

        df = pd.read_excel(xls, sheet_name=hoja_valida, header=3)

    except Exception as e:
        raise RuntimeError(f"Fallo en la lectura del Excel para {nombre_indicador}: {e}")

    if codigo not in df["Country Code"].unique():
        raise ValueError(f"No se encontró el país '{codigo}' en {nombre_indicador}.")

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
            print(f"✅ {nombre} cargado correctamente.")
        else:
            print(f"⚠️ {nombre} vacío.")
    except Exception as e:
        print(f"❌ Error al procesar {nombre}: {e}")

if "Poblacion_Destino" not in datasets:
    raise RuntimeError("No se pudo cargar 'Poblacion_Destino' como base para la unión.")

Datos_Fecha = datasets["Poblacion_Destino"].copy()
for key, df in datasets.items():
    if key != "Poblacion_Destino":
        Datos_Fecha = pd.merge(Datos_Fecha, df, on="año", how="outer")

Datos_Fecha = Datos_Fecha[pd.notna(Datos_Fecha["año"])].reset_index(drop=True)
Datos_Fecha["año"] = Datos_Fecha["año"].fillna(0).astype(int)

# ----------------------------------------------------------------------
# 7️⃣ Limpieza y cálculo de ratios
# ----------------------------------------------------------------------
numericas = [
    "Cantidad_Turistas_Año", "Poblacion_Destino", "Control_Corrupcion", "Estado_Derecho",
    "Efectividad_Gubernamental", "Rendicion_Cuentas", "Estabilidad_Politica", "Calidad_Regulatoria",
    "Homicidios", "Crecimiento_Poblacional", "IPC", "Pobreza_Poblacion_Porcentual",
    "Pobreza_Multidimennsional_Porcentual", "Porcentaje_Edad_Laboral",
    "Porcentaje_Edad_Laboral_Estudios", "Tasa_Desempleo"
]

for col in numericas:
    if col in Datos_Fecha.columns:
        Datos_Fecha[col] = pd.to_numeric(Datos_Fecha[col].astype(str).str.replace(",", "").str.strip(), errors='coerce')

if all(col in Datos_Fecha.columns for col in ["Cantidad_Turistas_Año", "Poblacion_Destino"]):
    Datos_Fecha["ratio_turistas_residentes"] = Datos_Fecha["Cantidad_Turistas_Año"] / Datos_Fecha["Poblacion_Destino"] * 100

# ----------------------------------------------------------------------
# 8️⃣ Categorización
# ----------------------------------------------------------------------
def categorizar_npselect(df, col, condiciones, valores):
    df[f"{col}_cat"] = np.select(condiciones, valores, default=None).astype(object)

gob_cols = ["Control_Corrupcion", "Estado_Derecho", "Efectividad_Gubernamental", "Rendicion_Cuentas"]
for col in gob_cols:
    if col in Datos_Fecha.columns:
        condiciones = [
            Datos_Fecha[col] >= 1.0,
            (Datos_Fecha[col] >= 0.0) & (Datos_Fecha[col] < 1.0),
            (Datos_Fecha[col] >= -1.0) & (Datos_Fecha[col] < 0.0),
            Datos_Fecha[col] < -1.0
        ]
        valores = ["BAJO ⭣", "MEDIO ⭤", "ALTO ⭡", "MUY ALTO ⚠"]
        categorizar_npselect(Datos_Fecha, col, condiciones, valores)

if "Inseguridad_Alimentaria" in Datos_Fecha.columns:
    condiciones = [
        Datos_Fecha["Inseguridad_Alimentaria"] <= 10,
        (Datos_Fecha["Inseguridad_Alimentaria"] > 10) & (Datos_Fecha["Inseguridad_Alimentaria"] <= 30),
        Datos_Fecha["Inseguridad_Alimentaria"] > 30
    ]
    valores = ["BAJO ⭣", "MEDIO ⭤", "ALTO ⭡"]
    categorizar_npselect(Datos_Fecha, "Inseguridad_Alimentaria", condiciones, valores)

if "Poblacion_Urbana" in Datos_Fecha.columns:
    condiciones = [
        Datos_Fecha["Poblacion_Urbana"] <= 10,
        (Datos_Fecha["Poblacion_Urbana"] > 10) & (Datos_Fecha["Poblacion_Urbana"] <= 30),
        Datos_Fecha["Poblacion_Urbana"] > 30
    ]
    valores = ["BAJO ⭣", "MEDIO ⭤", "ALTO ⭡"]
    categorizar_npselect(Datos_Fecha, "Poblacion_Urbana", condiciones, valores)

# ----------------------------------------------------------------------
# 9️⃣ Guardar resultado
# ----------------------------------------------------------------------
Datos_Fecha = Datos_Fecha.iloc[-10:]  # Últimos 10 años
print("\nVista preliminar de Datos_Fecha:")
print(Datos_Fecha.head())

try:
    Datos_Fecha.to_excel("Historico.xlsx", index=False)
    print("\n✅ Datos guardados correctamente en 'Historico.xlsx'.")
except Exception as e:
    print(f"❌ Error al guardar el archivo: {e}")







