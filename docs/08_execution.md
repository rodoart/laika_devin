# 08 — Ejecución: remota (clúster) y local

Guía paso a paso para correr el datapull en el clúster CDH y en una
máquina local (Windows/Linux) con el ambiente de pruebas.

---

## 1. Ejecución remota (clúster)

### Prerrequisitos

- Clúster CDH 7.1.9 con la parcela **Spark 3.3.2**
  (`SPARK3-3.3.2.3.3.7191000.12-1-1.p0.70675458`) y Python
  `citiconda39-9.9.46` — son las rutas que exporta `run_fs.sh`.
- Acceso a la cola YARN `root.datalabs` y a las librerías Hive
  (`gcgd1kmxpsd_prd_db`, …) declaradas en `configuration.json`.
- `hdfs dfs` disponible en el `edgenode` donde se lanza el submit.

### 1.1 Pipeline clásico (`main.py`)

```bash
cd <repo>
sh run_fs.sh
```

`run_fs.sh` exporta `SPARK_HOME`, `PYSPARK_PYTHON`,
`HADOOP_CONF_DIR`, `HIVE_CONF_DIR` y lanza:

```bash
nohup /usr/bin/spark3-submit \
    --conf spark.ui.port=12888 \
    --queue root.datalabs \
    main.py > logs/DataPull_err_<ts>.out 2> logs/DataPull_std_<ts>.out &
```

Logs en `./logs/DataPull_{err,std}_<timestamp>.out`. El submit queda en
background (`nohup`) — monitorear con `yarn logs -applicationId` o el
Spark UI en el puerto `12888` / `9812`.

### 1.2 Pipeline dinámico / resumable (`main_dynamic.py`)

Mismo submit, cambiando el script:

```bash
export SPARK_HOME=/opt/cloudera/parcels/SPARK3-3.3.2.../lib/spark3
export PYSPARK_PYTHON=/opt/cloudera/parcels/citiconda39-9.9.46/bin/python
export PYSPARK_DRIVER_PYTHON=$PYSPARK_PYTHON
export HADOOP_CONF_DIR=/opt/cloudera/parcels/CDH-7.1.9.../lib/hadoop/etc/hadoop
export HIVE_CONF_DIR=/opt/cloudera/parcels/CDH-7.1.9.../lib/hive/conf

/usr/bin/spark3-submit --queue root.datalabs main_dynamic.py
```

Flags útiles:

```bash
main_dynamic.py --sources payroll_streaks pnl_new   # subset de sources
main_dynamic.py --steps extract groupby fe          # subset de etapas
main_dynamic.py --no-dynamic                        # recomputar todo
main_dynamic.py --cleanup                           # borrar intermedios al final
main_dynamic.py --cleanup-only                      # solo limpieza
main_dynamic.py --paths-config /ruta/otra_paths.json
main_dynamic.py --no-email
```

Comportamiento clave: cada etapa escribe
`working_path/<stage_dir>` y **se recarga si el parquet ya existe**
(directorio + `_SUCCESS`), así que un proceso matado reanuda desde la
última etapa completada. Dentro de `fe`, cada transformación es un
parquet independiente → reanudación a mitad de etapa.

### 1.3 Con supervisor (recomendado en corridas largas)

```bash
nohup python supervisor.py -- main_dynamic.py --sources payroll_streaks \
    > logs/supervisor.out 2>&1 &
```

`supervisor.py` parsea las líneas `Executing step:` /
`Step finished:` del log; si el proceso muere o se queda zombie lo
relanza y las etapas ya escritas se recargan.

### 1.4 Checklist previo al submit

1. `config/config_custom.json`: `sources`, `working_path`,
   `pivot_path`, `year_id`, `n_mis`, `period`, `lista_trans`,
   `email_receivers`, `write_mode`.
2. `config/configuration.json`: bloques completos para cada source de
   `sources` (extract, weeks/mt/group_by/pivoting/fe según `steps_list`).
3. Espacio en HDFS para `working_path` + `pivot_path`.
4. En la primera corrida de un source nuevo, probar con
   `--steps extract groupby` antes de la cadena completa.

---

## 2. Ejecución local

Sin clúster: Spark `local[*]` sobre el FS local, con tablas tiny
sintéticas. No toca `config/*.json` ni rutas de datos reales.

### 2.1 Crear el ambiente (una vez)

```bash
conda create -n laika_datapull_py310 python=3.10 -y
conda run -n laika_datapull_py310 pip install \
    pyspark==3.3.2 "pandas<2" "numpy<2" unidecode python-dateutil \
    pyarrow==10.0.1 cytoolz typing_extensions pytest
```

- **Python 3.10** (el código usa anotaciones `X | Y` / `type[X]`).
- **PySpark 3.3.2** = versión de producción (`run_fs.sh`).

En **Windows** hacen falta binarios Hadoop para el FS local:
`tests/local/hadoop/bin/winutils.exe` + `hadoop.dll`
(ya descargados en el repo; `bootstrap.py` los pone en `PATH` vía
`HADOOP_HOME`). En Linux/Mac no hace falta nada extra.

### 2.2 Datos tiny

```bash
conda run -n laika_datapull_py310 python tests/local/make_tiny_data.py
```

Genera `pivot`, `asigna_historic`, `nomina_historic` (~56 filas) bajo
`$LAIKA_TEST_WORKDIR` (default `tests/local/workdir`).

### 2.3 Correr el pipeline

```bash
# cadena completa de payroll_streaks sobre la config de prueba
LAIKA_CONFIG_DIR=tests/local/config_dyn LAIKA_DISABLE_EMAIL=1 \
conda run -n laika_datapull_py310 python main_dynamic.py --local --no-email

# subset de etapas / recompute / limpieza
conda run -n laika_datapull_py310 python main_dynamic.py --local --steps extract groupby
conda run -n laika_datapull_py310 python main_dynamic.py --local --no-dynamic
conda run -n laika_datapull_py310 python main_dynamic.py --local --cleanup-only
```

- `--local` → SparkSession `local[*]` + FS local (los helpers
  `path_exists`/`path_delete` evitan `hdfs dfs`).
- `LAIKA_CONFIG_DIR` apunta a `tests/local/config_dyn/` (override sin
  tocar `config/*.json`); `LAIKA_DISABLE_EMAIL=1` evita el envío de
  correos.
- Segunda ejecución → todas las etapas se **recargan** (dinámico).

### 2.4 Verificación

```bash
# imports de los 44 módulos
conda run -n laika_datapull_py310 python tests/local/run_imports.py

# etapa groupby aislada
conda run -n laika_datapull_py310 python tests/local/run_groupby_test.py

# suite pytest completa (210 tests)
conda run -n laika_datapull_py310 python -m pytest tests/local/unit_tests -q
```

### 2.5 Variables de entorno del harness

| Variable | Default | Significado |
|---|---|---|
| `LAIKA_TEST_WORKDIR` | `tests/local/workdir` | raíz de inputs/outputs de prueba |
| `LAIKA_CONFIG_DIR` | `config/` | directorio de los JSON de configuración |
| `HADOOP_HOME` | `tests/local/hadoop` | dir con `bin/winutils.exe` (Windows) |
| `LAIKA_DISABLE_EMAIL` | — | si vale `1`, no se envían correos |
| `PYSPARK_PYTHON` | `sys.executable` | bootstrap lo fija al intérprete del env |

Borrar el workdir resetea la ejecución (todo se recomputa).

---

## 3. Resolución de problemas

| Síntoma | Causa / arreglo |
|---|---|
| `NullPointerException`/`IOException` en FS local Windows | Falta `hadoop.dll`/`winutils.exe` → revisar `HADOOP_HOME`. |
| `CreateProcess error=216` | `winutils.exe` descargado como HTML (LFS) — bajar el binario real. |
| `ImportError: aggregates` | El paquete real es `libs/aggregattes` (typo histórico); `bootstrap.install_aggregates_alias()` lo soluciona en local. |
| `PosixPath not supported` | Windows: `bootstrap` parchea `PosixPath → PurePosixPath`. |
| `Table or view not found: df` en FE | Las transformaciones SQL usan la vista `df`; se registra en `FeatureEngineering.run`. |
| La etapa `fe` no se recarga | Normal: solo se recargan las transformaciones cuyo parquet `<trans>_features` existe; las pendientes se recomputan. |
| Workers con otro Python | `PYSPARK_PYTHON`/`PYSPARK_DRIVER_PYTHON` deben apuntar al intérprete del env. |
