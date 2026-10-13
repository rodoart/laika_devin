# Local test harness

Smoke tests for the datapull code on tiny synthetic tables, run entirely
outside the cluster. Everything is parameterized — nothing touches the
real data paths or the host environment.

## Environment

Production (`run_fs.sh`) uses Spark 3.3.2. The code uses `X | Y` and
`type[X]` annotations, so Python >= 3.10 is required.

```bash
conda create -n laika_datapull_py310 python=3.10 -y
conda run -n laika_datapull_py310 pip install \
    pyspark==3.3.2 "pandas<2" "numpy<2" unidecode python-dateutil \
    pyarrow==10.0.1 cytoolz typing_extensions
```

On Windows, `tests/local/hadoop/bin/` must contain `winutils.exe` +
`hadoop.dll` (already downloaded). The env copies in
`$CONDA_PREFIX/Library/bin` are also used via PATH.

## Parameters

| Variable | Default | Meaning |
|---|---|---|
| `LAIKA_TEST_WORKDIR` | `tests/local/workdir` | root for all test inputs/outputs |
| `HADOOP_HOME` | `tests/local/hadoop` | dir containing `bin/winutils.exe` (Windows) |

No config file edits needed: the harness reads the real
`config/configuration.json` + `config/config-custom_payroll_streaks_*.json`.

## Usage

```bash
# 1. Import smoke test for every module in the repo
conda run -n laika_datapull_py310 python tests/local/run_imports.py

# 2. Generate tiny synthetic tables (pivot, asigna, nomina ~56 rows)
conda run -n laika_datapull_py310 python tests/local/make_tiny_data.py

# 3. Run the payroll_streaks `groupby` step end-to-end
conda run -n laika_datapull_py310 python tests/local/run_groupby_test.py
```

Results land in `$LAIKA_TEST_WORKDIR/output/` +
`summary.txt`. Deleting the workdir resets everything.

## Dynamic pipeline (step framework)

The whole datapull can also run as a `previous_step` chain of framework
`Step`s (see `libs/steps.py`, `libs/datapull_steps.py`, `main_dynamic.py`).
Each stage reloads its persisted parquet instead of recomputing it when
`is_dynamic` is on — the run is resumable.

```bash
# local run over tests/local/config_dyn/ (does NOT touch config/*.json)
LAIKA_CONFIG_DIR=tests/local/config_dyn LAIKA_DISABLE_EMAIL=1 \
conda run -n laika_datapull_py310 python main_dynamic.py --local --no-email

# subset of stages / non-dynamic / cleanup intermediates
python main_dynamic.py --local --steps extract groupby
python main_dynamic.py --local --no-dynamic
python main_dynamic.py --local --cleanup-only
```

`tests/local/config_dyn/config_custom.json` points `working_path` and
`pivot_path` at `tests/local/workdir_dyn/` + `tests/local/workdir/pivot`.
`config/paths.json` (internal file) maps each stage to its relative output
dir; the supervisor (`supervisor.py`) watches `Executing step:` /
`Step finished:` lines and restarts the process, resuming from disk.

## Unit tests

`tests/local/unit_tests/` contains an extensive pytest suite (210 tests,
~2 min) covering:

| File | Covers |
|---|---|
| `test_hive_path.py` | `HivePath`: exists/iterdir/mkdir/touch/rm/mv/rename, `is_successful`, `listparquets`, `glob`, `libs.steps` wrappers |
| `test_steps.py` | `Step` base class, `cached_property`, `dynamic_parquet*` decorators (reload vs recompute), `tmp_paths`/`delete_tmp_paths`, `output_parameters` chaining |
| `test_weeks.py` | `YearWeek` helpers, `WeekOfMonth*` taggers (abstract/FourWeeksForced/Payroll), `WeeklyTFromParquetReTagger` (standard/force_4_weeks/payroll tfroms) |
| `test_missing_treatment.py` | `MissingTreatment` fill strategies |
| `test_generate_ratios.py` | ratio queries / group-by-ratio generation |
| `test_vector_assembler.py` | `FeaturesVectorAssembler` |
| `test_colums_2_vector.py` | `ColumnsToVector` |
| `test_feature_engineering.py` | standard / `*MT` / ONZ transformations |
| `test_generate_pivot.py` | `GeneratePivot` |
| `test_group_by.py` | `GroupBy` dispatcher |
| `test_extract.py` | `Extract` date-range/query helpers |
| `test_mobs.py` | MOB imputation (`PreviousFixer*`) |
| `test_payroll_streaks_rules.py` | all `*_pattern_rule` variants, `get_when_otherwise_nested`, `reverse_pattern_rule`, `fill_and_select_latest` |
| `test_datapull_steps.py` | `DatapullStageStep` chain on a stub `DataPull` (paths, `previous_path` propagation, tmp cleanup) |

```bash
conda run -n laika_datapull_py310 python -m pytest tests/local/unit_tests -q
```

The suite found (and the code was fixed for): `hive_path.rmdir`
not deleting files without `-r`, `steps.delete_tmp_paths` referencing an
undefined `skip_trash`, and confirmed the real `tfrom` semantics
(weeks from the last day of the vintage month; one month = 4 payroll
tfroms).

## Shims applied by `bootstrap.py`

Only needed to run the transcribed code locally; none change behavior:

- `PurePosixPath` for `PosixPath` (POSIX paths can't be built on Windows).
- `libs.aggregates` → `libs.aggregattes` alias (the real dir name has the typo).
- `numpy.source` stub (removed in numpy >= 1.24; present in the cluster's old numpy).
- `typing.LiteralString` from `typing_extensions` (stdlib has it only on >= 3.11).
