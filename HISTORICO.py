import pandas as pd
import numpy as np
import json

# --- Leer configuración (Sin manejo de errores de archivo) ---
with open("config.json", "r", encoding="utf-8") as f:
    config = json.load(f)

# Obtener el código de país desde el JSON
if "country_code" not in config:
    raise ValueError("El archivo config.json debe contener la clave 'country_code'.")

country_code_upper = config["country_code"].upper()
print(f"Código de País detectado: {country_code_upper}")


# --- URLs de los datasets del World Bank ---
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
    "Inseguridad_Alimentaria": "https://api.worldbank.org/v2/es/indicator/SN.ITK.MSFI.ZS?downloadformat=excel"
}

# ----------------------------------------------------------------------
# Función de Lectura REAL del World Bank (Revertida al uso directo de URL)
# ----------------------------------------------------------------------
def leer_excel_codigo(url, nombre_indicador, codigo=config["country_code"], codigo_upper=country_code_upper):
    """
    Lee un Excel del World Bank usando la URL, filtra por Country Code y transforma los años a filas.
    """
    try:
        # Lectura directa de la URL (puede causar BadZipFile)
        df = pd.read_excel(url, sheet_name="Data", header=3) 
    except Exception as e:
        raise RuntimeError(f"Fallo en la lectura del Excel para {nombre_indicador}: {e}")

    # --- PASO DE DEPURACIÓN (DEBUG) para el código de país ---
    if codigo not in df["Country Code"].unique():
        print(f"\n--- DEBUG: Carga de {nombre_indicador} ---")
        print(f"Buscando código: '{codigo}'")
        
        # Búsqueda insensible a mayúsculas para dar pistas
        paises_similares = df[df["Country Code"].astype(str).str.contains(codigo, na=False, case=False)].head()
        
        if not paises_similares.empty:
             print(f"⚠️ Se encontraron códigos similares (revise mayúsculas/minúsculas o el código):")
             print(paises_similares[['Country Name', 'Country Code']].to_string(index=False))
        else:
             print(f"❌ ADVERTENCIA: El código '{codigo}' no se encontró. Los primeros 5 códigos disponibles son:\n{df['Country Code'].head().tolist()}")
        print("---------------------------------------")
        
        # Lanza error para detener la carga de este indicador
        raise ValueError(f"No se encontró el país '{codigo}' en el dataset.")

    # Buscar fila del país
    fila = df[df["Country Code"] == codigo]
    
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
print(f"\nIniciando la carga de datos para el país: {country_code_upper}")

# --- Fase de Carga ---
for nombre, url in urls.items():
    try:
        df = leer_excel_codigo(url, nombre) # Se pasa el nombre del indicador para debugging
        if not df.empty:
            df.rename(columns={country_code_upper: nombre}, inplace=True)
            datasets[nombre] = df
            print(f"✅ {nombre} cargado.")
        else:
            print(f"⚠️ {nombre} no tiene datos para el país o la carga resultó en un DataFrame vacío.")
    except Exception as e:
        print(f"❌ Error al procesar {nombre}: {e}")

# --- Fase de Unión (Robustez contra KeyError) ---
Datos_Fecha = None 
initial_key = "Poblacion_Destino"

if not datasets:
    print("\nNo se cargó ningún dataset. Finalizando el proceso.")
elif initial_key not in datasets:
    # Esto manejará el KeyError original si falla la carga de Poblacion_Destino
    print(f"\n❌ ERROR CRÍTICO: El dataset base ('{initial_key}') no se cargó correctamente.")
    print("El proceso se detiene. Revise el 'country_code' y los errores de carga.")
else:
    # Inicia con el dataset base si existe
    Datos_Fecha = datasets[initial_key].copy()
    
    # Fusión con los datasets restantes
    keys_to_merge = [key for key in datasets.keys() if key != initial_key]
    
    for key in keys_to_merge:
        Datos_Fecha = pd.merge(Datos_Fecha, datasets[key], on="año", how="outer")

    # Limpieza final: eliminar filas con año nulo y asegurar el tipo de dato
    Datos_Fecha = Datos_Fecha[pd.notna(Datos_Fecha['año'])].reset_index(drop=True)
    Datos_Fecha['año'] = Datos_Fecha['año'].astype(int)


# ----------------------------------------------------------------------
# El resto del código solo se ejecuta si Datos_Fecha pudo ser creado
# ----------------------------------------------------------------------
if Datos_Fecha is not None:
    
    ## 2. Limpieza y Conversión Numérica
    # ----------------------------------------------------------------------
    
    numericas = [
        "Cantidad_Turistas_Año", "Poblacion_Destino",
        "Control_Corrupcion", "Estado_Derecho", "Efectividad_Gubernamental", "Rendicion_Cuentas",
        "Estabilidad_Politica", "Calidad_Regulatoria", "Homicidios"
    ]
    
    # Convertir a numérico
    for col in numericas:
        if col in Datos_Fecha.columns:
            Datos_Fecha[col] = pd.to_numeric(
                Datos_Fecha[col].astype(str).str.replace(",", "").str.strip(), 
                errors='coerce'
            )
    
    # Calcular ratio turistas / residentes
    if "Cantidad_Turistas_Año" in Datos_Fecha.columns and "Poblacion_Destino" in Datos_Fecha.columns:
        Datos_Fecha["ratio_turistas_residentes"] = (
            Datos_Fecha["Cantidad_Turistas_Año"] / Datos_Fecha["Poblacion_Destino"] * 100
        )
    
    # ----------------------------------------------------------------------
    ## 3. Aplicación de Condiciones de Riesgo (Categorización)
    # ----------------------------------------------------------------------
    
    # A. Indicadores de Gobernanza (Rango de -2.5 a 2.5)
    for col in ["Control_Corrupcion", "Estado_Derecho", "Efectividad_Gubernamental", "Rendicion_Cuentas"]:
        if col in Datos_Fecha.columns:
            Datos_Fecha[col] = Datos_Fecha[col].apply(
                lambda x: (
                    "Bajo" if pd.notna(x) and x >= 1.0 else
                    "Medio" if pd.notna(x) and 0.0 <= x <= 0.99 else
                    "Alto" if pd.notna(x) and -1.0 <= x <= -0.01 else
                    "Muy alto" if pd.notna(x) and x < -1.0 else np.nan
                )
            )
    
    # B. Percentiles de Ranking (Rango de 0 a 100)
    for col in ["Estabilidad_Politica", "Calidad_Regulatoria"]:
        if col in Datos_Fecha.columns:
            Datos_Fecha[col] = Datos_Fecha[col].apply(
                lambda x: (
                    "Bajo" if pd.notna(x) and x >= 75 else # Bajo riesgo
                    "Medio" if pd.notna(x) and 50 <= x <= 74 else
                    "Alto" if pd.notna(x) and 25 <= x <= 49 else
                    "Muy alto" if pd.notna(x) and x < 25 else np.nan # Muy alto riesgo
                )
            )
    
    # C. Tasa de Homicidios (Tasa por 100,000)
    if "Homicidios" in Datos_Fecha.columns:
        Datos_Fecha["Homicidios"] = Datos_Fecha["Homicidios"].apply(
            lambda x: (
                "Bajo" if pd.notna(x) and x < 5 else
                "Medio" if pd.notna(x) and 5 <= x <= 15 else
                "Alto" if pd.notna(x) and 15 < x <= 30 else
                "Muy alto" if pd.notna(x) and x > 30 else np.nan
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

else:
    print("\n--- Proceso Detenido ---")
    print("El DataFrame principal 'Datos_Fecha' no pudo ser inicializado. Revise los errores de carga de datos anteriores.")