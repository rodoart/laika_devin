# 01 — Extract (`extract`)

**Módulo:** `libs/extract.py` — clase `Extract` · `libs/extract_special_treatments/`

Es la primera etapa del pipeline: descarga las tablas fuente desde **Hive** hacia
**Parquet** en `working_path/year_id/source/extract/`, recortadas al rango temporal que
necesita el datapull (`n_mis` meses hacia atrás desde las vintages del pivot).

## Flujo general

```text
pivot ──► vintages ──► rangos de fechas ──► queries SQL ──► threads ──► parquet por tabla
                                        └─► cohortes mensuales (join pivot ↔ fuente)
                                        └─► controles HTML (conteos, nulos) ──► email
```

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
columna de mes relativo (`name_mis` → el `tfrom` en meses). Devuelve `(queries, paths)`
para ejecutar en paralelo con `number_threads` (`config_custom["n_threads"]`).

### `_create_cohorts` / `_create_month_cohort` / `_join_full_cohorts`

Construye las **cohortes**: el cruce cliente × periodo. `_create_month_cohort` hace join
del pivot con la tabla extraída de cada mes y **aplica el tratamiento especial** de la
fuente (`_special_treatment`), devolviendo el DataFrame ya normalizado. Los parquets por
mes se unen después en el `*_historic` final (`_join_full_cohorts`).

### `_special_treatment(input_df, table_name, date, query)`

Dispatcher que devuelve una instancia de `ExtractSpecialTreatment` según la fuente y
ejecuta su método `run()`:

- `extract_special_treatments/default.py` → tratamiento genérico (renombrado de columnas,
  casts, deduplicación por llave).
- `extract_special_treatments/payroll_streaks.py` → misma plantilla de clase con el
  comportamiento específico de la fuente payroll_streaks.

### Controles de calidad

Antes de cerrar la extracción se generan tablas HTML de control
(`_controls_output`, `_controls_notnull_output`, `create_html_controls`) que el
`DataPull` adjunta al correo *"Input controls for source {X}"*: número de registros por
periodo y conteo de nulos en la llave pivote — sirven para detectar extracciones
incompletas antes de seguir.

### `collect_history()`

Punto de entrada que ejecuta todo lo anterior de extremo a extremo (es lo que invoca
`DataPull._perform_extract`).

### Reanudación (`is_dynamic`)

Con `is_dynamic=True` (lo que pasa `ExtractStep` desde `main_dynamic`), cada
artefacto se recarga si su directorio tiene `_SUCCESS`:

- `<table>_YYYYMM` — parquet mensual (particionado por la columna de
  vintage del pivot): no se reejecuta la query a Hive, los controles del
  correo se reconstruyen desde el parquet. Sirven además como fuente de
  `_create_cohorts` — el antiguo `<table>_full` intermedio ya no existe:
  los cohortes leen los mensuales directamente con poda por vintage.
- `<table>_vintage_YYYYMM` — cohorte por vintage (prefijo `_vintage_`
  para no colisionar con los mensuales).
- `<table>_historic` — salida final: si existe, la tabla entera se salta.

Un kill a mitad de tabla solo reejecuta las queries de los meses que
falten. Con `is_dynamic=False` (pipeline legado) todo se recomputa.

## Salida

```
{working_path}/{year_id}/{source}/extract/{table}_historic/   (parquet)
```

Columnas típicas tras la extracción: `numcliente`, `vintage_date`, `tfrom`,
`tfrom_date`, `tfrom_month`, `process_date` más las variables propias de cada fuente
(`importe`, `tef`, saldos, flags…).
