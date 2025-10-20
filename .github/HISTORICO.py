import pandas as pd  # Importamos pandas

# 👉 Ruta directa al archivo Excel en la web
url = "https://api.worldbank.org/v2/en/indicator/SP.POP.TOTL?downloadformat=excel"

# Leemos el Excel directamente desde la URL
Poblacion_Destino = pd.read_excel(url, sheet_name="Data", header=3)

# Guardamos los nombres de las columnas originales
column_headers = Poblacion_Destino.columns.tolist()[1:]

# Transponemos el DataFrame (filas ⇄ columnas)
Poblacion_Destino = Poblacion_Destino.T

# Tomamos la primera fila transpuesta como columnas
Poblacion_Destino.columns = Poblacion_Destino.iloc[0]      # df.iloc[0] es la primera fila del transpuesto → la usamos como header

# Eliminamos la fila que acabamos de convertir en encabezado
Poblacion_Destino = Poblacion_Destino[1:]                  # Nos quedamos con todas las filas salvo la que ya usamos como header

# Creamos una nueva columna "Encabezado" con los nombres originales
Poblacion_Destino['Encabezado'] = column_headers

# Reordenamos columnas para que "Encabezado" sea la primera
cols = ['Encabezado'] + [c for c in Poblacion_Destino.columns if c != 'Encabezado']
Poblacion_Destino = Poblacion_Destino[cols]

# Reindexamos el DataFrame para que el índice sea limpio (0,1,2,...)
Poblacion_Destino = Poblacion_Destino.reset_index(drop=True)

# Eliminamos las primeras 3 filas por posición (corrección)
Poblacion_Destino = Poblacion_Destino.iloc[3:].reset_index(drop=True)

# Poblacion_Destino.iloc[:, 1:] = Poblacion_Destino.iloc[:, 1:] * 1000

column_headers = Poblacion_Destino.columns.tolist()

column_headers[0] = "Año"

# 👉 Ruta directa al archivo Excel en la web
url = "https://api.worldbank.org/v2/en/indicator/SP.POP.GROW?downloadformat=excel"

# Leemos el Excel directamente desde la URL
Crecimiento_Poblacional = pd.read_excel(url, sheet_name="Data", header=3)

# Guardamos los nombres de las columnas originales
column_headers = Crecimiento_Poblacional.columns.tolist()[1:]

# Transponemos el DataFrame (filas ⇄ columnas)
Crecimiento_Poblacional = Crecimiento_Poblacional.T

# Tomamos la primera fila transpuesta como columnas
Crecimiento_Poblacional.columns = Crecimiento_Poblacional.iloc[0]      # df.iloc[0] es la primera fila del transpuesto → la usamos como header

# Eliminamos la fila que acabamos de convertir en encabezado
Crecimiento_Poblacional = Crecimiento_Poblacional[1:]                  # Nos quedamos con todas las filas salvo la que ya usamos como header

# Creamos una nueva columna "Encabezado" con los nombres originales
Crecimiento_Poblacional['Encabezado'] = column_headers

# Reordenamos columnas para que "Encabezado" sea la primera
cols = ['Encabezado'] + [c for c in Crecimiento_Poblacional.columns if c != 'Encabezado']
Crecimiento_Poblacional = Crecimiento_Poblacional[cols]

# Reindexamos el DataFrame para que el índice sea limpio (0,1,2,...)
Crecimiento_Poblacional = Crecimiento_Poblacional.reset_index(drop=True)

# Eliminamos las primeras 3 filas por posición (corrección)
Crecimiento_Poblacional = Crecimiento_Poblacional.iloc[3:].reset_index(drop=True)

column_headers = Crecimiento_Poblacional.columns.tolist()

column_headers[0] = "Año"

Crecimiento_Poblacional.columns = column_headers

# Indicamos las dimensiones
print(Crecimiento_Poblacional.shape)

# Mostramos las primeras 5 filas del DataFrame para verificar que cargó bien
Crecimiento_Poblacional.head()

# Nos quedamos con Tanzania para ambos dataset
Poblacion_Destino = Poblacion_Destino[["Año", "Tanzania"]]
Crecimiento_Poblacional = Crecimiento_Poblacional[["Año", "Tanzania"]]

# Renombramos las columnas
Poblacion_Destino = Poblacion_Destino.rename(columns={"Tanzania": "Poblacion_Destino"})
Crecimiento_Poblacional = Crecimiento_Poblacional.rename(columns={"Tanzania": "Crecimiento_Poblacional"})

# La unimos en una unica tabla
Datos_Fecha = pd.merge(Poblacion_Destino, Crecimiento_Poblacional, on="Año")

# Dimensiones
Datos_Fecha.shape

# 5 filas
Datos_Fecha.head()

# 👉 Ruta directa al archivo Excel en la web
url = "https://api.worldbank.org/v2/en/indicator/SI.POV.DDAY?downloadformat=excel"

# Leemos el Excel directamente desde la URL
Pobreza_Poblacion_Porcentual = pd.read_excel(url, sheet_name="Data", header=3)

# Guardamos los nombres de las columnas originales
column_headers = Pobreza_Poblacion_Porcentual.columns.tolist()[1:]

# Transponemos el DataFrame (filas ⇄ columnas)
Pobreza_Poblacion_Porcentual = Pobreza_Poblacion_Porcentual.T

# Tomamos la primera fila transpuesta como columnas
Pobreza_Poblacion_Porcentual.columns = Pobreza_Poblacion_Porcentual.iloc[0]      # df.iloc[0] es la primera fila del transpuesto → la usamos como header

# Eliminamos la fila que acabamos de convertir en encabezado
Pobreza_Poblacion_Porcentual = Pobreza_Poblacion_Porcentual[1:]                  # Nos quedamos con todas las filas salvo la que ya usamos como header

# Creamos una nueva columna "Encabezado" con los nombres originales
Pobreza_Poblacion_Porcentual['Encabezado'] = column_headers

# Reordenamos columnas para que "Encabezado" sea la primera
cols = ['Encabezado'] + [c for c in Pobreza_Poblacion_Porcentual.columns if c != 'Encabezado']
Pobreza_Poblacion_Porcentual = Pobreza_Poblacion_Porcentual[cols]

# Reindexamos el DataFrame para que el índice sea limpio (0,1,2,...)
Pobreza_Poblacion_Porcentual = Pobreza_Poblacion_Porcentual.reset_index(drop=True)

# Eliminamos las primeras 3 filas por posición (corrección)
Pobreza_Poblacion_Porcentual = Pobreza_Poblacion_Porcentual.iloc[3:].reset_index(drop=True)

column_headers = Pobreza_Poblacion_Porcentual.columns.tolist()

column_headers[0] = "Año"

Pobreza_Poblacion_Porcentual.columns = column_headers

# Indicamos las dimensiones
print(Pobreza_Poblacion_Porcentual.shape)

# Mostramos las primeras 5 filas del DataFrame para verificar que cargó bien
Pobreza_Poblacion_Porcentual.head()

# Nos quedamos con Tanzania para ambos dataset
Pobreza_Poblacion_Porcentual = Pobreza_Poblacion_Porcentual[["Año", "Tanzania"]]

# Renombramos las columnas
Pobreza_Poblacion_Porcentual = Pobreza_Poblacion_Porcentual.rename(columns={"Tanzania": "Pobreza_Poblacion_Porcentual"})

# La unimos en una unica tabla
Datos_Fecha = pd.merge(Datos_Fecha, Pobreza_Poblacion_Porcentual, on="Año")

# Dimensiones
Datos_Fecha.shape

# 5 filas
Datos_Fecha.head()

# 👉 Ruta directa al archivo Excel en la web
url = "https://api.worldbank.org/v2/en/indicator/SP.POP.1564.TO.ZS?downloadformat=excel"

# Leemos el Excel directamente desde la URL
Porcentaje_Edad_Laboral = pd.read_excel(url, sheet_name="Data", header=3)

# Guardamos los nombres de las columnas originales
column_headers = Porcentaje_Edad_Laboral.columns.tolist()[1:]

# Transponemos el DataFrame (filas ⇄ columnas)
Porcentaje_Edad_Laboral = Porcentaje_Edad_Laboral.T

# Tomamos la primera fila transpuesta como columnas
Porcentaje_Edad_Laboral.columns = Porcentaje_Edad_Laboral.iloc[0]      # df.iloc[0] es la primera fila del transpuesto → la usamos como header

# Eliminamos la fila que acabamos de convertir en encabezado
Porcentaje_Edad_Laboral = Porcentaje_Edad_Laboral[1:]                  # Nos quedamos con todas las filas salvo la que ya usamos como header

# Creamos una nueva columna "Encabezado" con los nombres originales
Porcentaje_Edad_Laboral['Encabezado'] = column_headers

# Reordenamos columnas para que "Encabezado" sea la primera
cols = ['Encabezado'] + [c for c in Porcentaje_Edad_Laboral.columns if c != 'Encabezado']
Porcentaje_Edad_Laboral = Porcentaje_Edad_Laboral[cols]

# Reindexamos el DataFrame para que el índice sea limpio (0,1,2,...)
Porcentaje_Edad_Laboral = Porcentaje_Edad_Laboral.reset_index(drop=True)

# Eliminamos las primeras 3 filas por posición (corrección)
Porcentaje_Edad_Laboral = Porcentaje_Edad_Laboral.iloc[3:].reset_index(drop=True)

column_headers = Porcentaje_Edad_Laboral.columns.tolist()

column_headers[0] = "Año"

Porcentaje_Edad_Laboral.columns = column_headers

# Indicamos las dimensiones
print(Porcentaje_Edad_Laboral.shape)

# Mostramos las primeras 5 filas del DataFrame para verificar que cargó bien
Porcentaje_Edad_Laboral.head()

# Nos quedamos con Tanzania para ambos dataset
Porcentaje_Edad_Laboral = Porcentaje_Edad_Laboral[["Año", "Tanzania"]]

# Renombramos las columnas
Porcentaje_Edad_Laboral = Porcentaje_Edad_Laboral.rename(columns={"Tanzania": "Porcentaje_Edad_Laboral"})

# La unimos en una unica tabla
Datos_Fecha = pd.merge(Datos_Fecha, Porcentaje_Edad_Laboral, on="Año")

# Dimensiones
Datos_Fecha.shape

# 5 filas
Datos_Fecha.head()

# 👉 Ruta directa al archivo Excel en la web
url = "https://api.worldbank.org/v2/en/indicator/ST.INT.ARVL?downloadformat=excel"

# Leemos el Excel directamente desde la URL
Cantidad_Turistas_Año = pd.read_excel(url, sheet_name="Data", header=3)

# Guardamos los nombres de las columnas originales
column_headers = Cantidad_Turistas_Año.columns.tolist()[1:]

# Transponemos el DataFrame (filas ⇄ columnas)
Cantidad_Turistas_Año = Cantidad_Turistas_Año.T

# Tomamos la primera fila transpuesta como columnas
Cantidad_Turistas_Año.columns = Cantidad_Turistas_Año.iloc[0]      # df.iloc[0] es la primera fila del transpuesto → la usamos como header

# Eliminamos la fila que acabamos de convertir en encabezado
Cantidad_Turistas_Año = Cantidad_Turistas_Año[1:]                  # Nos quedamos con todas las filas salvo la que ya usamos como header

# Creamos una nueva columna "Encabezado" con los nombres originales
Cantidad_Turistas_Año['Encabezado'] = column_headers

# Reordenamos columnas para que "Encabezado" sea la primera
cols = ['Encabezado'] + [c for c in Cantidad_Turistas_Año.columns if c != 'Encabezado']
Cantidad_Turistas_Año = Cantidad_Turistas_Año[cols]

# Reindexamos el DataFrame para que el índice sea limpio (0,1,2,...)
Cantidad_Turistas_Año = Cantidad_Turistas_Año.reset_index(drop=True)

# Eliminamos las primeras 3 filas por posición (corrección)
Cantidad_Turistas_Año = Cantidad_Turistas_Año.iloc[3:].reset_index(drop=True)

column_headers = Cantidad_Turistas_Año.columns.tolist()

column_headers[0] = "Año"

Cantidad_Turistas_Año.columns = column_headers

# Indicamos las dimensiones
print(Cantidad_Turistas_Año.shape)

# Mostramos las primeras 5 filas del DataFrame para verificar que cargó bien
Cantidad_Turistas_Año.head()

# Nos quedamos con Tanzania para ambos dataset
Cantidad_Turistas_Año = Cantidad_Turistas_Año[["Año", "Tanzania"]]

# Renombramos las columnas
Cantidad_Turistas_Año = Cantidad_Turistas_Año.rename(columns={"Tanzania": "Cantidad_Turistas_Año"})

# La unimos en una unica tabla
Datos_Fecha = pd.merge(Datos_Fecha, Cantidad_Turistas_Año, on="Año")

# Dimensiones
Datos_Fecha.shape

# Divsion
Datos_Fecha["Ratio turistas / residentes"] = Datos_Fecha["Cantidad_Turistas_Año"] / Datos_Fecha["Poblacion_Destino"]*100

# Dimensiones
Datos_Fecha.shape

# 5 filas
Datos_Fecha.tail(20)

# 👉 Ruta directa al archivo Excel en la web
url = "https://api.worldbank.org/v2/en/indicator/PV.PER.RNK?downloadformat=excel"

# Leemos el Excel directamente desde la URL
Estabilidad_Politica = pd.read_excel(url, sheet_name="Data", header=3)

# Guardamos los nombres de las columnas originales
column_headers = Estabilidad_Politica.columns.tolist()[1:]

# Transponemos el DataFrame (filas ⇄ columnas)
Estabilidad_Politica = Estabilidad_Politica.T

# Tomamos la primera fila transpuesta como columnas
Estabilidad_Politica.columns = Estabilidad_Politica.iloc[0]      # df.iloc[0] es la primera fila del transpuesto → la usamos como header

# Eliminamos la fila que acabamos de convertir en encabezado
Estabilidad_Politica = Estabilidad_Politica[1:]                  # Nos quedamos con todas las filas salvo la que ya usamos como header

# Creamos una nueva columna "Encabezado" con los nombres originales
Estabilidad_Politica['Encabezado'] = column_headers

# Reordenamos columnas para que "Encabezado" sea la primera
cols = ['Encabezado'] + [c for c in Estabilidad_Politica.columns if c != 'Encabezado']
Estabilidad_Politica = Estabilidad_Politica[cols]

# Reindexamos el DataFrame para que el índice sea limpio (0,1,2,...)
Estabilidad_Politica = Estabilidad_Politica.reset_index(drop=True)

# Eliminamos las primeras 3 filas por posición (corrección)
Estabilidad_Politica = Estabilidad_Politica.iloc[3:].reset_index(drop=True)

column_headers = Estabilidad_Politica.columns.tolist()

column_headers[0] = "Año"

Estabilidad_Politica.columns = column_headers

# Indicamos las dimensiones
print(Estabilidad_Politica.shape)

# Mostramos las primeras 5 filas del DataFrame para verificar que cargó bien
Estabilidad_Politica.tail()

# Nos quedamos con Tanzania para ambos dataset
Estabilidad_Politica = Estabilidad_Politica[["Año", "Tanzania"]]

# Renombramos las columnas
Estabilidad_Politica = Estabilidad_Politica.rename(columns={"Tanzania": "Estabilidad_Politica"})

# La unimos en una unica tabla
Datos_Fecha = pd.merge(Datos_Fecha, Estabilidad_Politica, on="Año")

# Dimensiones
Datos_Fecha.shape

# 5 filas
Datos_Fecha.head()

# 👉 Ruta directa al archivo Excel en la web
url = "https://api.worldbank.org/v2/es/indicator/CC.EST?downloadformat=excel"

# Leemos el Excel directamente desde la URL
Control_Corrupcion = pd.read_excel(url, sheet_name="Data", header=3)

# Guardamos los nombres de las columnas originales
column_headers = Control_Corrupcion.columns.tolist()[1:]

# Transponemos el DataFrame (filas ⇄ columnas)
Control_Corrupcion = Control_Corrupcion.T

# Tomamos la primera fila transpuesta como columnas
Control_Corrupcion.columns = Control_Corrupcion.iloc[0]      # df.iloc[0] es la primera fila del transpuesto → la usamos como header

# Eliminamos la fila que acabamos de convertir en encabezado
Control_Corrupcion = Control_Corrupcion[1:]                  # Nos quedamos con todas las filas salvo la que ya usamos como header

# Creamos una nueva columna "Encabezado" con los nombres originales
Control_Corrupcion['Encabezado'] = column_headers

# Reordenamos columnas para que "Encabezado" sea la primera
cols = ['Encabezado'] + [c for c in Control_Corrupcion.columns if c != 'Encabezado']
Control_Corrupcion = Control_Corrupcion[cols]

# Reindexamos el DataFrame para que el índice sea limpio (0,1,2,...)
Control_Corrupcion = Control_Corrupcion.reset_index(drop=True)

# Eliminamos las primeras 3 filas por posición (corrección)
Control_Corrupcion = Control_Corrupcion.iloc[3:].reset_index(drop=True)

column_headers = Control_Corrupcion.columns.tolist()

column_headers[0] = "Año"

Control_Corrupcion.columns = column_headers

# Indicamos las dimensiones
print(Control_Corrupcion.shape)

# Mostramos las primeras 5 filas del DataFrame para verificar que cargó bien
Control_Corrupcion.tail()

# Nos quedamos con Tanzania para ambos dataset
Control_Corrupcion = Control_Corrupcion[["Año", "Tanzanía"]]

# Renombramos las columnas
Control_Corrupcion = Control_Corrupcion.rename(columns={"Tanzania": "Control_Corrupcion"})

# La unimos en una unica tabla
Datos_Fecha = pd.merge(Datos_Fecha, Control_Corrupcion, on="Año")

# Dimensiones
Datos_Fecha.shape

# 5 filas
Datos_Fecha.head()