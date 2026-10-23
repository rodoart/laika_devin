# 01 — Extract (`extract`)

**Módulo:** `libs/extract.py` — clase `Extract` · `libs/extract_special_treatments/`

Es la primera etapa del pipeline: descarga las tablas fuente desde **Hive** hacia
**Parquet** en `working_path/year_id/source/extract/`, recortadas al rango temporal que
necesita el datapull (`n_mis` meses hacia atrás desde las vintages del pivot).

## Flujo general

```text
pivot ──► vintages ──► rangos de fechas ──► queries SQL por mes
                                        └─► join pivot ↔ fuente (por mes)
                                        └─► tfrom final (mensual / semanal / diario)
                                        └─► write directo a <table>_historic/vintage_date=*/tfrom=*
                                        └─► controles HTML (conteos, nulos) ──► email
```

Ya **no** existen los parquets intermedios `<table>_YYYYMM`,
`<table>_vintage_YYYYMM` ni `<table>_full`: cada mes transformado se escribe
directamente en las particiones `vintage_date/tfrom` del `_historic` final
con `partitionOverwriteMode=dynamic` (un write solo reemplaza las
particiones que toca, así el proceso es idempotente mes a mes).

## Piezas clave

### `_get_vintages_dates(pivot, name_vintage_date, table_key, sqlContext)`

Obtiene del **pivot** la lista de fechas de cosecha (`vintage_date`) para las que hay que
extraer historia. El pivot puede filtrarse por `table_key` cuando la fuente solo cubre un
subconjunto de clientes.

### `_range_dates` / `_range_vintages`

A partir de las vintages generan la lista de periodos a descargar: `[vintage, retraso,
n_mis]` — cada vintage menos `retraso` meses, abarcando `n_mis` meses hacia atrás.

### `_create_queries(list_dates, table_name, database, type_date, name_mis, select_variables)`

Genera las queries SQL por tabla: un `SELECT` sobre `database.table_name` filtrando
`type_date` en el rango de fechas, proyeccionando `select_variables` y calculando la
columna de mes relativo (`name_mis` → el `tfrom` en meses). Devuelve `(queries, tables)`.

### `_run_month_query` / `_join_month_pivot` / `_month_base`

Por cada mes: ejecuta la query, aplica el **tratamiento especial** de la fuente
(`_special_treatment`), hace join left del pivot con los datos del mes y filtra cada fila
a la ventana temporal de su propio vintage (`tfrom_date` dentro de
`[tfrom_min, tfrom_ini]` por fila).

### `_apply_tfrom(base, name_vintage_pivote, retraso, wk, bounds)`

Calcula el `tfrom`/`tfrom_date` final:

- **Sin `wk`** (fuente sin bloque `weeks`): tfrom mensual
  (`months_between(fin_rango_vintage, tfrom_date)`).
- **Con `wk`**: el reetiquetado del antiguo paso `weeks`, integrado aquí —
  métodos `force_4_weeks`/`standard`/`payroll` (`weeks_difference` anclado al
  `last_day(vintage)` por fila) o **`daily`** (intervalos de un día:
  `tfrom = datediff(last_day(vintage), tfrom_date)`). La ventana global del
  antiguo `filtered` se aplica igual, computada con
  `tfrom_window_bounds(max_tfrom_date, method, type, n_mis)` — los bounds se
  obtienen con un pre-escaneo descendente de los meses hasta el primero con
  datos (es el máximo `tfrom_date` de la tabla).

### Spill de particiones semanales

Una semana a caballo de dos meses agrupa ficheros de **dos** buckets en la
misma partición `(vintage, tfrom)`. Regla de propiedad: el bucket con fecha
lit en el mes M escribe todas las particiones cuyo **domingo**
(`tfrom_date + 6`) cae en M — ese bucket tiene garantizados los ficheros del
tramo final; el tramo inicial llega como *spill* del bucket anterior. El
método `daily` y el tfrom mensual no tienen spill (cada fichero cae en un
único bucket).

### `_special_treatment(input_df, table_name, date, query)`

Dispatcher que devuelve una instancia de `ExtractSpecialTreatment` según la fuente y
ejecuta su método `run()`:

- `extract_special_treatments/default.py` → tratamiento genérico (renombrado de columnas,
  casts, deduplicación por llave).
- `extract_special_treatments/payroll_streaks.py` → misma plantilla de clase con el
  comportamiento específico de la fuente payroll_streaks.
- `extract_special_treatments/adobe.py` → resolución de identidad
  `numcliente` vía tabla hash con regla de no-fuga del snapshot futuro.

### Controles de calidad

Antes de cerrar la extracción se generan tablas HTML de control
(`_controls_output`, `_controls_notnull_output`, `create_html_controls`) que el
`DataPull` adjunta al correo *"Input controls for source {X}"*: número de registros por
periodo y conteo de nulos en la llave pivote — sirven para detectar extracciones
incompletas antes de seguir. Los controles de entrada de cada mes se
persisten como sidecars `_meta/<table>/<YYYYMM>.ctrl` para reconstruirlos
en una reanudación sin releer Hive.

### `collect_history()`

Punto de entrada que ejecuta todo lo anterior de extremo a extremo (es lo que invoca
`DataPull._perform_extract`).

### Reanudación (`is_dynamic`)

Con `is_dynamic=True` (lo que pasa `ExtractStep` desde `main_dynamic`), el
estado por tabla vive en `extract/_meta/<table>/`:

- `<YYYYMM>.done` — ese mes ya escribió sus particiones en el `_historic`:
  no se reejecuta su query (solo se recomputa si el siguiente bucket
  necesita su *spill* semanal).
- `<YYYYMM>.ctrl` — controles de entrada persistidos del mes.
- `_DONE` — la tabla está completa: `_check_extract` la da por hecha y la
  reanudación solo recalcula los controles de salida.

Los writes mensuales con `partitionOverwriteMode=dynamic` no dejan
`_SUCCESS` en la raíz (Spark 3.3) — se toca manualmente al cerrar la tabla.
Un kill a mitad de tabla solo reejecuta las queries de los meses sin
`.done`. Con `is_dynamic=False` se wipea el `_historic` y se recomputa todo
(comportamiento del pipeline legado).

## Salida

```
{working_path}/{year_id}/{source}/extract/{table}_historic/   (parquet,
                                        vintage_date=*/tfrom=*)
{working_path}/{year_id}/{source}/extract/_meta/{table}/      (marcadores)
```

Columnas típicas tras la extracción: `numcliente`, `vintage_date`, `tfrom`,
`tfrom_date`, `process_date` más las variables propias de cada fuente
(`importe`, `tef`, `fecha`, saldos, flags…). Para fuentes con `weeks`,
`tfrom` ya sale en la granularidad final (semanas o días) — la etapa
`weeks` de `steps_list` es un no-op de compatibilidad.
