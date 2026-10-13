# 06 — Step framework: ejecución dinámica (persistencia + reanudación)

Capa añadida sobre el datapull que adopta la lógica del `framework/`
(pasos encadenados con `previous_step`, salidas persistidas en parquet y
recargadas, supervisor de sesiones).

## Piezas

| Archivo | Rol |
|---|---|
| `libs/hive_path.py` | `HivePath` (port de `data_engineering_toolbox/path`): objeto `PurePosixPath` con `exists`/`is_dir`/`iterdir`/`is_successful`/`rmdir`/`mkdir`/`mv`, backend dual HDFS (`hdfs dfs`) / FS local. Toda la verificación y borrado de parquets pasa por aquí (`is_successful` = dir + `_SUCCESS`). |
| `libs/steps.py` | Port del framework: `Step`, `cached_property`, decoradores `dynamic_parquet*` (reload-o-recompute), `collect_step_output`, `inherit_parent_step_attributes`, `delete_tmp_paths` (vía `HivePath.rmdir`). |
| `libs/datapull_steps.py` | Un `DatapullStageStep` por etapa de `_pipeline` (`extract`, `weeks`, `mt`, `groupby`, `pivoting`, `ratios`, `fe`, `mobs`, `va`, `cols2vec`). `build_source_chain` construye la cadena por fuente. |
| `libs/session.py` | `SparkSessionBuilder`: sesión YARN (env `PYSPARK_*`) o `local[*]` con reintentos. |
| `libs/paths.py` + `config/paths.json` | Config **interna** (no la toca el usuario) con los paths relativos de cada etapa bajo `<working_path>/<year_id>/<source>/`; cada entrada lleva `keep_or_delete`. |
| `main_dynamic.py` | Entry point: carga los JSON del usuario sin tocarlos, instancia `DataPull`, construye las cadenas por fuente y ejecuta el último step. Flags: `--local`, `--sources`, `--no-dynamic`, `--steps`, `--cleanup`, `--no-email`. |
| `supervisor.py` | Watchdog: reinicia el proceso si muere o se queda zombie (detecta progreso vía stdout `Executing step:`/`Step finished:` + Spark UI). Al reiniciar, las etapas ya escritas se recargan. |

## Lógica dinámica

- `is_dynamic=True` (default): `step_action` verifica si la salida de la
  etapa está completa (reutiliza los `_check_extract`/`_check_missing`/
  `_check_groupby`/`_evaluate_path`/`_fe_performed` originales); si lo está
  → `data reloaded from <path>` y no recomputa.
- `previous_path` viaja por `output_parameters[step_name]["previous_path"]`
  igual que en `_pipeline` (mobs → `mobs/imputate_mobs`, ratios de
  corporate → `features`, `va`/`cols2vec` pass-through).
- Los `output_hive[stage]` se cargan desde `config/paths.json`; los
  `keep_or_delete="delete"` (`weekly_extract`, `missing_treatment`,
  `pivot`) se borran con `--cleanup`/`Step.delete_tmp_paths()`.
- `sqlContext` se resuelve perezosamente desde `datapull` porque
  `reset_spark_session` recrea la sesión entre etapas.
- Los `hdfs dfs` shell-outs de las clases internas se enrutan a
  `libs.hive_path`/`libs.steps` (HDFS en el clúster, FS local en `--local`).
- Las etapas de salida única (`groupby`, `pivoting`, `va`) verifican el
  parquet con `HivePath.is_successful()` (dir + `_SUCCESS`): si una
  escritura quedó a medias se recomputa.
- `fe` reanuda a mitad: `_fe_performed` lista `features/` y solo corre las
  transformaciones que falten (comportamiento idéntico al `_pipeline`
  original).

## Uso

```bash
# clúster (mismos JSON de siempre)
python main_dynamic.py                 # o: python supervisor.py

# local con datos pequeños
LAIKA_CONFIG_DIR=tests/local/config_dyn LAIKA_DISABLE_EMAIL=1 \
    python main_dynamic.py --local --no-email
```

## Variables de entorno

| Var | Efecto |
|---|---|
| `LAIKA_CONFIG_DIR` | Directorio con `configuration.json`/`config_custom.json` alternativos (default `config/`). |
| `LAIKA_PATHS_CONFIG` | Ruta alternativa de `config/paths.json`. |
| `LAIKA_FS_MODE` | `hdfs`/`local`: fuerza el modo FS de los helpers. |
| `LAIKA_SPARK_MASTER` | Override del master Spark (`yarn`, `local[*]`...). |
| `LAIKA_DISABLE_EMAIL` | No-op del `EmailSender`. |
| `SUPERVISOR_*`, `PYSPARK_PORT` | Timeouts/puerto del supervisor. |
