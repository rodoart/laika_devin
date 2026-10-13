# DataPull

Pipeline Spark/Hive que extrae históricos mensuales por cliente
(`numcliente` × `vintage_date` × `tfrom`), los re-etiqueta a semanas,
aplica tratamiento de nulos, calcula agregados por fuente, genera
features de feature engineering y ensambla el vector final para los
modelos.

Cada **source** (p. ej. `payroll_streaks`, `bureau_pull`, `pnl_new`…)
tiene su propia cadena de etapas declarada en
`configuration.json -> <source>.steps_list`.

## Layout del repositorio

```
main.py              Entry point clásico (pipeline lineal DataPull.run)
main_dynamic.py      Entry point con framework Step (resumable, dinámico)
supervisor.py        Watchdog: reinicia el proceso si muere/zombiea
datapull.py          Orquestador DataPull (un _perform_* por etapa)
run_fs.sh            spark3-submit en el clúster (CDH 7.1.9, Spark 3.3.2)
config/
  configuration.json Config rígida por source (extract/weeks/mt/group_by/...)
  config_custom.json Config editable por el usuario (sources, paths, n_mis...)
  paths.json         Config interna: rutas relativas de cada etapa
libs/
  steps.py           Framework Step + decoradores dynamic_parquet*
  hive_path.py       HivePath: FS dual HDFS/local
  datapull_steps.py  DatapullStageStep: una clase Step por etapa
  session.py         SparkSessionBuilder (YARN o local)
  paths.py           Loader de config/paths.json
  extract.py         Extracción Hive -> parquet por vintages
  weeks.py           Retaggeado semanal (WeekOfMonth*, WeeklyTFrom*)
  missing_treatment.py  MissingTreatment: relleno de nulos (etapa mt)
  group_by.py        Dispatcher source -> *Aggregate
  aggregattes/       21 agregadores (payroll_streaks, bureau_pull, ...)
  generate_pivot.py  Pivoteo long -> wide
  generate_ratios.py Ratios entre variables
  feature_engineering.py  Transformaciones FE (standard/MT/ONZ)
  mobs/              Mobs + PreviousFixer*: cálculo e imputación de MOBs (etapa mobs)
  vector_assembler.py, colums_2_vector.py
  extract_special_treatments/  Cohortes especiales de extracción
  email_sender.py
docs/                Documentación por etapa (ver docs/README.md)
tests/local/         Harness local (env conda, datos tiny, pytest)
```

## Uso

Guía detallada en **[docs/08_execution.md](docs/08_execution.md)**.
Resumen:

### En el clúster (producción)

```bash
sh run_fs.sh          # spark3-submit de main.py en YARN (clásico)

# o el pipeline dinámico/resumable:
spark3-submit main_dynamic.py                      # todas las sources del config
spark3-submit main_dynamic.py --sources payroll_streaks pnl_new
spark3-submit main_dynamic.py --steps extract groupby fe   # solo algunas etapas
spark3-submit main_dynamic.py --no-dynamic         # recomputar todo
spark3-submit main_dynamic.py --cleanup            # borrar intermedios al final
spark3-submit main_dynamic.py --no-email
```

Cada etapa es un `Step` que **recarga su parquet si ya existe**
(`is_dynamic`), así que una corrida interrumpida reanuda desde la última
etapa completada. `supervisor.py` puede vigilar el proceso y relanzarlo;
los logs `Executing step:` / `Step finished:` son compatibles.

### Local (sin clúster)

```bash
# setup (una vez): Python 3.10 + PySpark 3.3.2
conda create -n laika_datapull_py310 python=3.10 -y
conda run -n laika_datapull_py310 pip install \
    pyspark==3.3.2 "pandas<2" "numpy<2" unidecode python-dateutil \
    pyarrow==10.0.1 cytoolz typing_extensions pytest

# datos tiny + pipeline + tests
conda run -n laika_datapull_py310 python tests/local/make_tiny_data.py
LAIKA_CONFIG_DIR=tests/local/config_dyn LAIKA_DISABLE_EMAIL=1 \
conda run -n laika_datapull_py310 python main_dynamic.py --local --no-email
conda run -n laika_datapull_py310 python -m pytest tests/local/unit_tests -q
```

En Windows hacen falta `winutils.exe`/`hadoop.dll` en
`tests/local/hadoop/bin` (ya incluidos). Ver
`tests/local/README.md` para las variables del harness.

## Configuración

| Archivo | Lo toca el usuario | Contenido |
|---|---|---|
| `config/config_custom.json` | sí | `sources`, `working_path`, `pivot_path`, `year_id`, `n_mis`, `period`, `lista_trans`, `email_*`, `n_threads`, `write_mode` |
| `config/configuration.json` | solo para añadir sources | bloque por source: `extract`, `weeks`, `missing_treatment`, `group_by`, `pivoting`, `fe`, `steps_list` |
| `config/paths.json` | no (interno) | ruta relativa de salida + `keep_or_delete` por etapa |

## Cómo añadir un nuevo source

Un source `mi_source` necesita, como mínimo, un bloque en
`configuration.json` y una entrada en `config_custom.json -> sources`.
Según qué etapas incluya su `steps_list`, hace falta lo siguiente:

1. **`extract`** — declarar tablas Hive de origen:
   ```json
   "mi_source": {
     "extract": {
       "libname": ["db1", "db1"],
       "table_name": ["tabla_a", "tabla_b"],
       "select_variables": ["col1, col2, cast(x as double) as x", "..."],
       "join_keys": [...], "date_column": "process_date"
     }
   }
   ```
   El extracto produce parquets `extract/<table>_historic` con columnas
   `numcliente`, `vintage_date`, `tfrom`, `tfrom_date` + las variables.

2. **`weeks`** (opcional, si hay que re-etiquetar a semanas):
   ```json
   "weeks": {
     "tfrom_method": "payroll",           // standard | force_4_weeks | payroll
     "date_column_for_weekly_tfrom": ["process_date", "fecha"],
     "type_of_date_column": ["monthly", "daily"],
     "sql_date_format": ["yyyy-MM-dd", "yyyy-MM-dd"],
     "drop_columns_at_the_end": [[], ["fecha"]]
   }
   ```

3. **`mt` / `missing_treatment`** (opcional): `fill_variables` +
   `fill_methods` (método por grupo de columnas) + `mobs` si la fuente
   genera MOBs propios.

4. **`groupby`** — agregado por `(numcliente, vintage_date, tfrom)`:
   - Crear `libs/aggregattes/mi_source.py` con una clase
     `MiSourceAggregate(BaseAggregate)` — la base ya aporta
     `__init__`, `_save_parquet`, `_table_path`/`_read_table`,
     `_sibling_path` y el `_create_groupby_query` estándar; solo hace
     falta implementar `aggregate()` (ver `pnl.py` como ejemplo mínimo
     o `payroll_income.py` si necesita `n_mis`/`period`/`@dynamic`).
   - Registrarla en `libs/group_by.py`: una entrada en
     `AGGREGATE_CLASSES` (+ otra en `AGGREGATE_EXTRA_ARGS` si el
     constructor acepta argumentos extra).
   - Declarar `group_by` en el JSON (reglas / `query_dict` según el
     estilo del agregador).

5. **`pivoting`**: `"pivoting": {"source": "mi_source",
   "pivot_key": ["numcliente", "vintage_date", "tfrom", "tfrom_date"]}`.
   Produce la tabla wide `pivot/mi_source_pivot` (una columna por
   variable × tfrom).

6. **`fe`** (opcional): `"fe": {...}` con el `fe_dict`
   (`ignore_this_value_when_calculating_mt_features`, `weights_dict`,
   etc.). Las transformaciones a correr van en
   `config_custom.json -> lista_trans` (ver `docs/07_features.md`).

7. **`mobs`, `va`, `cols2vec`** (opcionales): MOBs por fuente y
   ensamblado final del vector.

8. **`steps_list`**: orden de ejecución, p. ej.
   `["extract", "weeks", "mt", "groupby", "pivoting", "fe", "va"]`.
   `main_dynamic.py` encadena las etapas en ese orden; `previous_path`
   se propaga automáticamente.

9. **`config/paths.json`** (solo si se añade una etapa nueva o un layout
   distinto): añadir `"mi_source": {"table_or_hdfs": "...",
   "keep_or_delete": "keep|delete"}` por etapa. Las entradas `delete`
   se borran con `--cleanup`.

10. **Pivot de salida**: si la fuente participa en el pivot global,
    revisar `lag_ecm_pivot` / `lag_liabilities_pivot` /
    `mis_col_liabilities_pivot` en `configuration.json`.

Notas:

- `need_crd_acct_nbr` / `need_contratos` listan las sources que
  requieren `crd_acct_nbr` / `contratos` en el extract.
- `list_no_mobs` excluye features del cálculo de MOBs.
- Los nombres de etapa en `steps_list` son los que matchea
  `DatapullStageStep` (`extract|weeks|mt|groupby|pivoting|ratios|fe|
  mobs|va|cols2vec`).

## Documentación

`docs/README.md` indexa los notebooks por etapa:

- `00_datapull.md` — visión general del pipeline
- `01_extract.md`, `02_weeks_missing_treatment.md`,
  `03_groupby_aggregates.md`, `04_feature_engineering.md`,
  `05_mobs_vector_assembly.md`, `06_step_framework.md`
- `07_features.md` — catálogo de features/transformaciones
