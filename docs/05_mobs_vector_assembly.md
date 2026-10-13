# 05 — Mobs, Vector Assembler y salida final

Los últimos pasos del pipeline producen las **observaciones (mobs)** y la tabla final de
features lista para modelado.

---

## `mobs` — cálculo de observaciones

**Módulos:** `MissingTreatment.run_mobs` (`libs/missing_treatment.py`) + `libs/mobs/`

En banca, **MOB** (*months on books*) es la observación de comportamiento del cliente en el
mes N de vida de su cosecha. Este paso toma las `features/` y genera las columnas de MOB
imputadas que el modelo usa como target/auxiliar.

### Flujo en `DataPull._perform_mobs`

1. Si la fuente **no** es `payroll_income` ni `payroll_streaks`, primero llama
   `_create_mobs_extract()` — una extracción auxiliar de las tablas de MOBs
   (`_check_mobs_extract` valida que el parquet `premobs` exista).
2. `_get_mobs_list(path_fe)` arma la lista de features/periodos a imputar a partir de lo
   que hay en `features/`.
3. `mt.run_mobs(tables_path, pathwrite, features_path, feat_list, periodo, sql_context)`
   ejecuta la imputación por fuente.

### Imputación por fuente (`MissingTreatment`)

| Método | Fuente | Detalle |
|---|---|---|
| `liabilities_mobs` | liabilities | `mobs_tables_union` interna — unión de tablas de pasivos |
| `credit_card_mobs` | tarjeta | devuelve `dfs_credit[0]` |
| `bureau_mobs` | bureau | MOBs de buró |
| `payroll_income_mobs` | nómina | delega en `mobs/payroll_income.py` |
| `payroll_streaks_mobs` | rachas nómina | delega en `mobs/payroll_streaks.py` |

### `libs/mobs/payroll_income.py` — `PreviousFixerPayroll`

Clase de imputación paralela (multi-hilo) para payroll:

- `calculated_mobs`, `raw_feature`, `calculated_mobs_with_auxilar_id` — preparan el
  DataFrame con ID auxiliar por feature.
- `imputate_and_save_a_feature(feature)` — imputa una variable y la guarda.
- `multi_thread_imputate_features()` — distribuye features entre `n_threads` workers.
- `payroll_mob_imputation(df, features, lista_meses, reverse)` — la UDF de imputación
  propiamente dicha (forward/backward sobre `lista_meses` de `_get_months(periodo, n_mis)`).
- `save_raw_feature_to_output_path` — persistencia.

Subclases:

- `PreviousFixerPayrollIncome(PreviousFixerPayroll)` — versión `payroll_income` con su
  propio `payroll_mob_imputation` (firma/retorno distinto).
- `PreviousFixerPayrollStreaks(PreviousFixerPayroll)` (`libs/mobs/payroll_streaks.py`) —
  versión para rachas de pago de nómina.

**Salida:** `{working_path}/{year_id}/{source}/mobs/imputate_mobs/`

---

## `va` — Vector Assembler (`libs/vector_assembler.py`)

**Clase:** `FeaturesVectorAssembler` — `run()`

Ensambla las columnas de features + mobs en el **formato vectorial** que espera el
modelo: toma la salida de `mobs/imputate_mobs/`, aplica `VectorAssembler` de Spark (o la
selección configurada) y produce la tabla de variables finales indexada por
`numcliente`/`vintage_date`.

---

## `cols2vec` — Columns To Vector (`libs/colums_2_vector.py`)

**Clase:** `ColumnsToVector` — `run()`

Último paso: convierte un conjunto configurable de columnas (`source_dict["cols2vec"]`)
en **columnas vectoriales** (`VectorUDT`) — el formato denso que consumen los modelos de
Spark ML / el pipeline de scoring. Es la contraparte "oficial" del assembler cuando la
config define explícitamente qué variables entran al vector.

---

## Resumen del producto final

```text
extract       tablas crudas por periodo         {tfrom, vars crudas}
weeks         reetiquetado semanal               {week_of_month, tfrom semanal}
mt            series completas sin huecos        {mismas vars, imputadas}
groupby       features agregadas por fuente      {var, var_cnt, segmentos}
pivoting      tfrom → columnas                   {var_t0, var_t1, ...}
ratios        cocientes entre ventanas           {ratio_*}
fe            transformaciones estadísticas      {mean_*, trend_*, onz_*}
mobs          observaciones imputadas            {mob_*}
va/cols2vec   vector final                       features VectorUDT listas para scoring
```

Todo queda bajo `{working_path}/{year_id}/{source}/` y cada etapa es reanudable gracias a
los métodos `_check_*` del `DataPull`.
