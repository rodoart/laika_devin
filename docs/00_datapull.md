# 00 — ¿Qué es el Datapull?

El **datapull** es el proceso ETL del equipo COE Advance Analytics. Su responsabilidad es
tomar fuentes de datos transaccionales del banco (nómina, pasivos, bureau, PNL, canales,
corporativo, etc.) y convertirlas en **tablas de variables listas para modelado**, todas
indexadas por cliente (`numcliente`), fecha de cosecha (`vintage_date`) y ventana temporal
relativa (`tfrom`).

Todo el proceso corre sobre **PySpark** (clúster Cloudera/CDH-7.1.9 con soporte Hive) y
persiste resultados intermedios en **Parquet** en HDFS.

## 1. Entry point

`main.py` hace tres cosas:

1. Carga los dos JSON de configuración:
   - `config/configuration.json` → `config_rigid`: configuración **rígida**, definida por el
     equipo de ingeniería de datos. Contiene, por cada *source*, la lista de pasos
     (`steps_list`), las tablas a extraer, diccionarios de agregación, transformaciones de
     FE, etc.
   - `config/config_custom.json` → `config_custom`: configuración **personalizada** del
     usuario final (sources a correr, rutas, meses, hilos, correos).
2. Construye el `SparkConf` (executors 15–30, 3 cores, 12G + 24G overhead, driver 100G,
   cola YARN `root.datalabs`, UI en puerto 9812) y define `spark_session_builder`, un
   lambda que crea la `SparkSession` con `enableHiveSupport()`.
3. Instancia `DataPull(config_rigid, config_custom, spark_session_builder)` y llama `run()`.

## 2. La clase `DataPull` (datapull.py)

Es el **orquestador**. Piezas clave:

### Propiedades perezosas

- `sqlContext`: crea la `SparkSession` bajo demanda (lazy) y registra la UDF
  `quita_acentos` en cada (re)creación.
- `pivot`: lee el parquet de pivot si existe (`_path_new_pivot`); si no, lo construye con
  `_pivot_preparation(sources)` (cruce de clientes × vintages). El pivot es la espina
  dorsal del proceso: define **quiénes son los clientes y en qué fechas** se observan.

### `run()`

Itera `self.sources` (de `config_custom["sources"]`, acepta str o lista) y ejecuta
`_pipeline(source)` por cada una, **aislando errores por fuente**: si una fuente falla,
captura el traceback, envía correo de error y continúa con la siguiente.

### `_pipeline(source)`

Lee `source_dict = config_rigid[source]` y su `steps_list`, y por cada paso invoca el
método `_perform_*` correspondiente. El `previous_path` va encadenando la salida de un
paso como entrada del siguiente. Al final, arma una tabla HTML con los tiempos por paso y
la envía por correo (`EmailSender`).

```
extract → weeks → mt → groupby → pivoting → ratios → fe → mobs → va → cols2vec
```

| Paso | Método | Salida (`working_path/year_id/source/...`) | Módulo |
|---|---|---|---|
| `extract` | `_perform_extract` | `extract/` | `libs/extract.py` |
| `weeks` | `_perform_weeks` | `weekly_extract/` | `libs/weeks.py` |
| `mt` | `_perform_mt` | `missing_treatment/` | `libs/missing_treatment.py` |
| `groupby` | `_perform_groupby` | `groupby/` | `libs/group_by.py` + `aggregattes/` |
| `pivoting` | `_perform_pivoting` | `pivot/` | `libs/group_by.py` (`GeneratePivot`) |
| `ratios` | (inline) `GenerateRatios` | `ratios/` (o `features/` en corporate) | `libs/generate_ratios.py` |
| `fe` | `_perform_fe` | `features/` | `libs/feature_engineering.py` |
| `mobs` | `_perform_mobs` | `mobs/imputate_mobs/` | `libs/mobs/mobs.py` (`Mobs.run_mobs`) |
| `va` | `_perform_va` | `features` ensambladas | `libs/vector_assembler.py` |
| `cols2vec` | `_perform_cols2vec` | salida final vectorizada | `libs/colums_2_vector.py` |

### Ajustes especiales de `n_mis`

- `payroll_income`: `n_mis += 2` antes de `extract`, se restaura tras `groupby`.
- Cualquier fuente con paso `weeks`: `n_mis += 1` antes de `extract`, `-1` después —
  el reetiquetado semanal necesita un mes extra de historia.

### Idempotencia

Cada paso verifica primero si su salida ya existe (`_check_extract`, `_check_missing`,
`_check_groupby`, `_check_pivoting`, `_fe_performed`, `_check_mobs`, `_evaluate_path`,
`_check_files`): si el parquet ya está, el paso se salta. Esto permite re-ejecutar el
pipeline sin recomputar etapas completadas.

### Notificaciones

`EmailSender` (`libs/email_sender.py`) envía:
- Correo de **error** por fuente (con traceback).
- Correo de **controles de input** (tablas HTML generadas por `Extract` y
  `_create_html_notification_times`).
- Correo de **fin de fuente** con los tiempos de ejecución por paso.

## 3. Claves de `config_custom.json`

| Clave | Significado |
|---|---|
| `sources` | fuente(s) a procesar (`pnl_new`, `payroll_income`, `bureau_pull`, …) |
| `working_path` | raíz HDFS de trabajo |
| `year_id` | vintage/ejecución (ej. `202601`) |
| `n_mis` | número de meses de historia hacia atrás |
| `period` | periodicidad en meses para ventanas móviles |
| `pivot_path` | parquet del pivot de clientes |
| `lista_trans` | transformaciones de FE a aplicar |
| `n_threads` | hilos para extracción/imputación paralela |
| `write_mode` | modo de escritura (`overwrite`) |
| `email_sender` / `email_receivers` | remitente y destinatarios de alertas |

## 4. Convenciones transversales

- **`tfrom`** ("time from"): índice de periodo relativo al `vintage_date`. `tfrom=0` es el
  periodo más reciente; aumenta hacia el pasado. Con paso `weeks` se expresa en semanas;
  sin él, en meses (`tfrom_month`).
- **`*_historic`**: las tablas extraídas se guardan con sufijo `_historic`.
- **`cached_property` / `dynamic`** (definidos en `aggregattes/payroll_income.py`):
  decoradores que cachean propiedades en memoria o las materializan en disco
  (`@dynamic(path_key=...)` guarda/lee parquet según `is_dynamic`).
- **`numcliente`, `vintage_date`**: claves que sobreviven todo el pipeline; los agregadores
  las excluyen de renombrados (`_columns_not_to_be_renamed`).
