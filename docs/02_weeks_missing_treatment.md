# 02 — Weeks y Missing Treatment (`weeks`, `mt`)

Dos pasos opcionales pero centrales para las fuentes con granularidad semanal
(`payroll_income`, `payroll_streaks`) y para cualquier fuente que necesite rellenar
huecos temporales.

---

## Parte A — `weeks` (integrado en el extract; `libs/weeks.py`)

> **Cambio de arquitectura:** `weeks` ya **no** es una etapa separada del
> datapull. El reetiquetado de `tfrom` (semanal o diario) vive dentro del
> extract (`Extract._apply_tfrom`): `extract/<table>_historic` sale
> directamente con el `tfrom` final y el dir `weekly_extract/` dejó de
> existir. `WeeksStep` quedó como no-op en `steps_list` por
> compatibilidad de config — solo propaga `extract/` como
> `previous_path`. Las clases `WeeklyTFrom`/`WeeklyTFromParquetReTagger`
> fueron eliminadas; su lógica es hoy las funciones
> `retag_tfrom_columns` / `tfrom_window_bounds` de este módulo.
>
> Por eso el pipeline sigue haciendo `n_mis += 1` antes de `extract`
> cuando `weeks` está en `steps_list`: se necesita un mes extra para
> cubrir las semanas de borde que la ventana `filtered` luego recorta.

### Funciones auxiliares de calendario

- `YearWeek` — namedtuple `(year, week)` para operar con semanas ISO.
- `date_to_yearweek`, `yearweek_to_date`, `add_weeks_to_yearweek` — conversiones fecha ↔
  año-semana.
- `is_yearweek_complete_in_month` — determina si una semana cae entera dentro de un mes
  (para no partir meses a la mitad).

### Jerarquía `WeekOfMonth`

Clases que, dado un DataFrame con una columna de fecha, añaden las columnas de semana:

| Clase | Regla |
|---|---|
| `WeekOfMonth` | Base: `weeks_and_dates`, semanas naturales del mes; métodos `new_week_of_year`, `new_day_of_week`, `new_week_of_year_day_of_month`, `_new_week_of_month` |
| `WeekOfMonthFourWeeksForced` | **Fuerza exactamente 4 semanas por mes** — días de arrastre de la semana 1/5 se reasignan para que toda semana etiquetada ∈ {1,2,3,4}. Es la convención que usan las reglas de patrón de `payroll_streaks` (`week_of_month` siempre en [1..4]) |
| `WeekOfMonthPayroll` | Variante para nómina: la semana se cuenta por **periodos de pago** (los cortes `day_of_week_start` = 22, 16, 8, 1 que usa `dates_collection` en el agregador) |

### `retag_tfrom_columns` / `tfrom_window_bounds`

- `retag_tfrom_columns(df, date_col, tfrom_method, type_of_date, ...)`
  reetiqueta en memoria lo que el viejo retagger hacía sobre parquets:
  `tfrom_date` pasa a ser el inicio de su semana (o mes) y
  `tfrom = weeks_difference(inicio_semana, last_day(vintage_date))`.
- Métodos: `standard` (52 semanas/año), `force_4_weeks` y `payroll`
  (48/año) — y **`daily`**: `tfrom = datediff(last_day(vintage),
  tfrom_date)` — intervalos de **un día** (p.ej. adobe cuando un mes de
  resolución es demasiado, o streaks diarios).
- `tfrom_window_bounds(max_date, method, type, n_mis)` reproduce la
  ventana `filtered` global (`month_interval` o `week_interval` según
  método/tipo), anclada al **max `tfrom_date` de la tabla**.

**Salida:** la misma `extract/<table>_historic` con `tfrom` ya en la
granularidad final.

---

## Parte B — `mt` / Missing Treatment (`libs/missing_treatment.py`)

**Clase:** `MissingTreatment` — *"Fills Values of a dataframe"*. Rellena valores faltantes
en las columnas temporales de cada tabla `*_historic` para que cada cliente tenga una
serie completa en los `n_mis` periodos.

### `run_mt(path_mt, tables_mt, path_write, sql_context)`

Recorre las tablas extraídas y aplica, según la configuración de cada fuente
(`agg_dict`/`mt` en `configuration.json`), la estrategia de relleno correspondiente.
Escribe en `missing_treatment/`.

### Estrategias de relleno

| Método | Lógica |
|---|---|
| `fill_historic` | Esqueleto maestro: genera todos los pares (cliente, periodo) esperados (`snapshot`), detecta el último valor no nulo (`last_null`), e imputa por **interpolación** (`interpolacion`) o **media móvil** (`moving_average`) según config |
| `fill_value` | Rellena con un valor fijo configurado |
| `fill_stat` | Rellena con un estadístico (media, etc.) |
| `fill_snap` | Rellena con el valor del snapshot más cercano |
| `fill_debito` | Caso especial para columnas de débito |
| `previous_fixer` / `valor_previo4` / `valor_previo4_step` | Rellena con el valor del periodo anterior (forward-fill hasta 4 pasos) |
| `get_last_contiguous_non_null_value` | Último valor no nulo contiguo por ventana |
| `contiguous_mobs` / `discontiguous_mobs` | Utilidades para series contiguas vs. con huecos |

### Soporte a MOBs

La misma clase expone `run_mobs` y `calculate_mobs` (usados en el paso `mobs`) con
implementaciones específicas por fuente: `liabilities_mobs` (con `mobs_tables_union`),
`credit_card_mobs`, `bureau_mobs`, `payroll_income_mobs`, `payroll_streaks_mobs` — cada una
lee las tablas indicadas y prepara el DataFrame sobre el que luego se imputan las
observaciones (ver [05](05_mobs_vector_assembly.md)).

### Utilidades internas

`__vars_select`, `__copy_feature`, `__write_parquet`, `__get_dataframes`,
`_calculate_separated_mobs`, `__list_tables`, `__get_months`, `__evaluate_path`,
`__delete_inmediate`, `get_numeric_columns`, `count_non_null_values`.

**Salida:** `{working_path}/{year_id}/{source}/missing_treatment/` — mismas tablas con
series completas y valores imputados según la regla configurada por columna.

### Reanudación (`is_dynamic`)

Al estar integrado en el extract, el retag se reanuda con los marcadores
`_meta/<table>/<YYYYMM>.done` del propio extract: un kill a mitad de
tabla solo reejecuta las queries de los meses sin marker (ver
[01](01_extract.md#reanudación-is_dynamic)).
