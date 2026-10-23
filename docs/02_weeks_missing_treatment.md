# 02 — Weeks y Missing Treatment (`weeks`, `mt`)

Dos pasos opcionales pero centrales para las fuentes con granularidad semanal
(`payroll_income`, `payroll_streaks`) y para cualquier fuente que necesite rellenar
huecos temporales.

---

## Parte A — `weeks` (`libs/weeks.py`)

Convierte la extracción mensual en **etiquetas semanales**: re-etiqueta cada registro con
la semana del mes, la semana del año y el día de la semana en que ocurrió, y reindexa el
`tfrom` a granularidad semanal. Por eso el pipeline hace `n_mis += 1` antes de `extract`
cuando el paso existe: se necesita un mes extra para cubrir las semanas de borde.

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

### `WeeklyTFrom` y `WeeklyTFromParquetReTagger`

- `WeeklyTFrom`: recalcula `tfrom` en unidades semanales a partir de las fechas
  re-etiquetadas (`run()` produce el parquet `weekly_extract/`).
- `WeeklyTFromParquetReTagger`: re-etiqueta parquets ya escritos — lee el extract,
  aplica el objeto `WeekOfMonth*` configurado y reescribe (`run()`).

**Salida:** `{working_path}/{year_id}/{source}/weekly_extract/` con `tfrom` semanal y
columnas `week_of_month`, `week_of_year`, `day_of_week`.

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

`WeeklyTFrom(is_dynamic=True)` (lo que pasa `WeeksStep` desde `main_dynamic`)
propaga el flag a cada `WeeklyTFromParquetReTagger`: sus propiedades
`filtered` y `filled_columns` están decoradas con `@dynamic`, así que el
intermedio `filtered_<table>` y la salida `weekly_extract/<table>` se
recargan por tabla si tienen `_SUCCESS`, sin releer el extract.
