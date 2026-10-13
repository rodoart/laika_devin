# 04 — Feature Engineering (`fe`)

**Módulo:** `libs/feature_engineering.py` (~1979 líneas) — clase `FeatureEngineering`

Es el paso que convierte las columnas agregadas/pivoteadas en **variables derivadas**:
medias, desviaciones, tendencias, móviles, ratios, etc., aplicadas sobre las ventanas
`tfrom` de cada cliente.

## Cómo se ejecuta

`DataPull._perform_fe` instancia `FeatureEngineering(previous_path, write_path, fe_dict,
sqlContext, mode)` y llama `run(transformations)`, donde `transformations` viene de
`config_custom["lista_trans"]` (ej. `["min","max","avgmov","mean","sum"]`) o del diccionario
`fe` de la fuente en `configuration.json`.

## Flujo interno de `FeatureEngineering`

```text
_get_lista_metodo ──► _get_mvars/_list_mvars ──► _create_raw_variables
        │                                              │
        ▼                                              ▼
_obtain_columns ──► _filter_columns_feature ──► _obtain_variables
        │
        ▼
for trans in transformations:
    _run_transformation → _app_transformation → _defineUdfTransformation
                        → _runUdfTransformation (UDF por fila o por vector)
        │
        ▼
_fill_na_zero → _write_parquet (features/)
```

- `_get_lista_metodo` / `_list_mvars`: obtiene la lista de variables base (`mvars`) a
  transformar — el docstring de `_list_mvars` conserva código legado comentado.
- `_obtain_columns` / `_filter_columns_feature` / `_obtain_variables`: resuelven qué
  columnas del DataFrame aplican a cada método (por prefijo/sufijo de patrón).
- `_runUdfTransformation`: aplica la UDF de transformación fila a fila sobre el vector de
  ventanas de cada variable y nombra el resultado `{trans}_{var}`.

## Transformaciones elementales

Implementadas como métodos `_{nombre}Transformation` sobre la clase:

| Transformación | Cálculo |
|---|---|
| `mean` | media de las ventanas |
| `std` | desviación estándar |
| `sum` | suma |
| `min` / `max` | extremos |
| `logmean` | media en escala log |
| `square` | cuadrado |
| `inverse` | inverso |
| `trend` | pendiente temporal |
| `rmeans` | media de ratios entre ventanas consecutivas |
| `incremental` | diferencias sucesivas |
| `vp` | variación porcentual |
| `rt` | ratio tfrom |
| `so` | segundo orden |
| `avgmov` | media móvil |
| `ratiosmod` / `run_ratios` / `run_deltas` | ratios y deltas entre ventanas |
| `rms` | raíz cuadrática media |
| `cmpx` | complejidad |
| `skew` / `curt` | asimetría y curtosis |
| `weighted_mean` | media ponderada (`assign_weights`, `_evaluate_path`) |

## `ExternalTransformations`

Clase base para transformaciones que operan sobre **arrays de meses** en vez de vectores
de tfrom. Reimplementa `_runUdfTransformation` y `_evaluate_path`.

### `StandardMethodsMT(ExternalTransformations)`

Versiones "MT" (monthly/otras tablas) de las mismas transformaciones:
`_trend_mt`, `_rmeans_mt`, `_incremental_mt`, `_vp_mt`, `_rt_mt`, `_so_mt`,
`_avgmov_mt`, `_run_ratios_mt`, `_run_deltas_mt`, `_ratiosmod_mt`, `_rms_mt`,
`_cmpx_mt`, `_skew_mt`, `_curt_mt`, `_weighted_mean_mt`, más el helper
`convert_to_three_digit_string`.

### `OnesAndZerosMethods(ExternalTransformations)`

Métodos para variables **binarias (0/1)** — patrones de pago/impago:

- `_find_pattern_onz` — detecta el patrón binario dominante.
- `_transitions_onz` — cuenta de transiciones 0→1 / 1→0.
- `_autocorrelation_onz` — autocorrelación de la serie binaria.
- `_kolmogorov_complexity_onz` — complejidad de Kolmogorov aproximada del patrón.
- `_pattern_diversity_onz` — diversidad de subpatrones.

### `OnesAndZerosMethodsMT(ExternalTransformations)`

Versiones MT de los métodos binarios (`_kolmogorov_onz_mt`, `_autocorrelation_onz_mt`,
`_pattern_diversity_onz_mt`, `_is_new_onz_mt` — esta última cierra el archivo, línea ~1979).
Son las transformaciones que procesan los patrones `paid_in_week` que generan
`payroll_streaks` (ver [03](03_groupby_aggregates.md)).

## Salida

`{working_path}/{year_id}/{source}/features/` — una fila por
(`numcliente`, `vintage_date`) con todas las variables base más las derivadas
`{trans}_{var}`.

## Comprobación de reanudación

`_fe_performed(path_fe, trans_list)` verifica qué transformaciones ya existen en
`features/` y solo calcula las que falten — permite reanudar sin recomputar.
