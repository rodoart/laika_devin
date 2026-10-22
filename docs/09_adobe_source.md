# 09 — Fuentes Adobe Clickstream (`adobe_bancanet` / `adobe_mobile`)

Integración del clickstream de Adobe Analytics (Bancanet y Mobile) como
fuentes de primer nivel del datapull, siguiendo el patrón
`payroll_streaks`: `special_treatment` en el extract, `weeks` para el
tfrom semanal y un agregador propio (`AdobeAggregate`).

> **Origen**: el código previo vive en `adobe_source/` (pipeline
> standalone, ahora de solo referencia). Ver
> `adobe_source/notebook/00_documentacion_adobe_source.ipynb` para la
> documentación del pipeline original.

## Arquitectura

```
extract    hit-level + numcliente (resolución hash)      AdobeExtractSpecialTreatment
weeks      tfrom semanal sobre file_date                 force_4_weeks (igual que payroll_streaks)
groupby    flags + T_* + 4 dominios de tfrom             AdobeAggregate
pivoting   long -> wide (una columna por <var>_<tfrom>)  GeneratePivot (+ drop_null_values)
fe         *_mt sobre series numéricas; *_onz_mt sobre flags
mobs       antigüedad digital -> mobs_adobe
va         ensamblado estándar
```

`steps_list`: `["extract", "weeks", "groupby", "pivoting", "fe", "mobs", "va"]`

> ⚠️ **`period` (config_custom) está en SEMANAS para estas fuentes**
> (y para cualquier fuente con `weeks` semanal). `period=4` ≈ 1 mes de
> ventana en las transformaciones `*_mt`. Con `n_mis=6` el extract trae
> 7 meses (`weeks` añade +1 por bordes de semana) → `tfrom` semanal
> 0..27 con `force_4_weeks`.

## Extract — `AdobeExtractSpecialTreatment`

`libs/extract_special_treatments/adobe.py`. Se usa solo para lo que no
tiene otra opción: el join con el pivot necesita `numcliente`, que la
tabla cruda de Adobe **no trae** (solo `post_evar5`, hash del cliente).

1. **Filtros de página** — `extract.adobe.clickstream_filters`: cada
   grupo es `{key, page_names[]}` → `TRIM(<key>) LIKE '<p>%' OR ...`;
   los grupos se unen con OR (fiel al original).
2. **`session_id`** = `post_visid_high-post_visid_low-visit_num`.
3. **Backfill de `post_evar5`** — `first(ignorenulls)` sobre la ventana
   de sesión ordenada por `date_time`: propaga el hash a hits que lo
   traen vacío (login a mitad de sesión).
4. **Resolución hash → `numcliente`** sin fuga de información: la tabla
   `hash_id_db` se filtra a `proc_dt < <primer día del mes del lote>`
   (cada mes se resuelve con el snapshot previo vigente). Doble join por
   `numcliente_hash` y `numcliente_salti` + `coalesce`, como el original.
5. Salida mínima (`extract.adobe.keep_columns`): `numcliente`,
   `session_id`, `file_date`, `date_time` y las columnas insumo de las
   reglas de flags (`evar25` / `post_evar3`, `post_evar63`, …).

`retraso=0` (la tabla se actualiza diario). `name_mis="file_date"`:
la tabla fuente está **particionada por día** y el extract hace range
queries mensuales sobre ella.

## Weeks

`force_4_weeks` + `type_of_date_column="daily"` +
`date_column_for_weekly_tfrom="file_date"` — la misma rama estable
(`month_interval`) que usa `payroll_streaks`; **`weeks.py` no se
modifica**. Cada hit queda con `tfrom` = semana (snap a lunes) y
`tfrom_date` = lunes de esa semana; `file_date` conserva el día real.

## GroupBy — `AdobeAggregate`

`libs/aggregattes/adobe.py`. El paso más personalizado: calcula
**flags de producto**, **tiempos de sesión** y agrega en **cuatro
dominios de tfrom** dentro del mismo long frame (`GeneratePivot` emite
una columna por par `(variable, tfrom)` → dominios no colisionan porque
los nombres de variable difieren).

| Dominio | tfrom | Variables (`bnt_`/`mob_` + nombre) | Equivale en adobe_source |
|---|---|---|---|
| `weekly` | semana 0..27 | `<flag>` (max 0/1), `<flag>_cnt`, `<tvar>_<stat>`, `hits` | FirstPivot semanal + inputs de views/time metrics |
| `fortnightly` | `floor(semana/2)` | mismas con sufijo `_q` | ventanas ~15d del original |
| `last_days` | día de ventana w | `<flag>_ld`, `<tvar>_ld_<stat>`, `hits_ld` | `views_metrics`/`time_metrics` `*_last_<w>d` |
| `daily_seq` | día 1..14 | `<flag>_d` (0/1) | vector binario diario → rachas reales |

- `last_days`: `datediff(last_day(vintage_date), file_date)+1 ≤ w`;
  ventanas en `domains.last_days.windows` (1..14 y 15..180 paso 15, como
  el original).
- `daily_seq`: últimos `domains.daily_seq.days` días (14 por defecto) —
  el vector binario diario sobre el que `*_onz_mt` calcula **rachas
  diarias**.
- **`T_vista` corregido**: el original hacía
  `when(isNull, T_vista).otherwise(0)` (null en el primer hit y 0 en el
  resto — invertido). Aquí `coalesce(T_vista, 0)`: primer hit = 0,
  resto = delta real.

### Reglas de flags (config-driven, `group_by.flags`)

Cada flag es una lista AND de condiciones `{col, op, value}`;
`op ∈ {eq, contains, startswith, endswith, contains_any, startswith_any, endswith_any}`.
Ejemplos literales del config:

```json
"PIL_view": [
  {"col": "evar25", "op": "startswith",   "value": "MX|BNP|Dashboard|Overlay|"},
  {"col": "evar25", "op": "contains_any", "value": ["Logra", "PERSONAL"]},
  {"col": "evar25", "op": "endswith_any", "value": ["NoPorAhora", "ConoceMas"]}
],
"Flag_interest_LOP": [
  {"col": "post_evar3",  "op": "eq",       "value": "OFERTASPARAMETRICAS"},
  {"col": "post_evar63", "op": "contains", "value": "DIS"}
]
```

Bancanet: `PIL_view/interested`, `LOP_view/interested`, `CLI_view`,
`HIP_view`, `NOM_view` (7 flags sobre `evar25`).
Mobile: `Flag_interest_{LOP,PIL,NOM,CLI,HIP,TDC,BAL}` (7 flags sobre
`post_evar3=='OFERTASPARAMETRICAS'` + `post_evar63`).

## Pivoting

`drop_null_values: true` (opción nueva en `GeneratePivot`): descarta los
pares `(variable, tfrom)` con valor null antes de pivotear — **necesario**
porque los dominios comparten `tfrom` con nombres distintos; sin el
filtro aparecerían columnas `*_q_<semana>`/`_ld_<semana>` all-null que el
fe convertiría en features constantes.

## Fe — binario vs numérico

No hace falta otra fuente: las columnas `_<t>` pivotadas alimentan dos
familias de transformaciones:

- `*_mt` (`sum_mt`, `mean_mt`, `min_mt`, `max_mt`, `std_mt`, `trend_mt`,
  `rmeans_mt`, `avgmov_mt`, …) sobre las **series numéricas**
  (`bnt_t_session_mean_*`, `bnt_hits_*`, `bnt_*_cnt_*`, `bnt_*_ld_*`,
  `bnt_*_q_*`) — producen las ventanas `i4/i8/…` (`period=4` semanas).
- `*_onz_mt` (`longest_1_streak_onz_mt`, `first_1_onz_mt`,
  `transitions_onz_mt`, `first_streaks_onz_mt`,
  `kolmogorov_complexity_onz_mt`, `entropy_onz_mt`, …) sobre las **vars
  binarias** (`bnt_pil_view_*` semanal, `bnt_pil_view_d_*` diario,
  `bnt_pil_view_q_*` quincenal) — **el equivalente nativo de
  `rachas_metrics`**.

Salvedades honestas:

- `*_onz_mt` también corre sobre las vars numéricas (genera columnas
  basura inofensivas; excluibles post-hoc si molestan).
- Sufijos de 3 dígitos (`_ld_105`..`_ld_180` y semanas 100+ — no aplica
  aquí) quedan **fuera del fe** (`_obtain_variables` exige ≤2 dígitos):
  sobreviven en `raw_features` como features ya agregadas.

## Mobs — `mobs_adobe`

`libs/mobs/mobs.py` → dispatch `"adobe"`: lee
`extract/<table>_historic` (tfrom mensual, pre-weeks) **filtrando los
meses vacíos** de la malla pivot (`file_date is not null` — la columna
viene de `extract.name_mis` del config) y define `mobs_adobe =
max(tfrom)` = meses desde la primera actividad digital observada. El
tfrom del extract es inclusivo (1 = mes del vintage, max = mes más
viejo extraído), así que el valor queda **naturalmente capeado** al
borde de la ventana (conservador: actividad en el mes más viejo no
distingue `n_mis` de historia mayor).

La imputación corre por **`PreviousFixerAdobe`**
(`libs/mobs/adobe.py`, registrado en `_FIXER_BY_SOURCE` de
`libs/mobs/mobs.py` — el `previous_fixer` genérico solo entiende
sufijos `_<N>m` y no tocaría las features `_<N>w`): cada columna
`_<N>w` se pone a **-99999** cuando `N > mobs_adobe * 4` semanas
(`force_4_weeks` = 4 semanas/mes). Un cliente sin hits → fuera de
`calculate_mobs` → `mobs` nulo → todas sus ventanas a **-99999**
(según lo acordado: sin actividad = imposible, no cero). Ojo: para los
dominios `_q`/`_d`/`_ld` el índice `N` no son semanas literales
(quincenas/días/buckets); la comparación los trata como semanas, es
decir es **conservadora** (imputa de más, nunca de menos).
`fe.ignore_this_value_when_calculating_mt_features = -99999`.

## Cobertura de clientes — `keep_pivot_clients`

`pivoting.keep_pivot_clients: true` (`libs/generate_pivot.py`):
tras pivotear, el wide hace **left join con el pivot**
(`numcliente`, `vintage`→`vintage_date`) y `fillna(0)` en las columnas
de features. Resultado: **todo cliente del pivot llega a
`vector_assembler`** — los sin actividad aparecen con flags a 0 y sus
ventanas a -99999 vía mobs. Sin esto, `weeks` descarta las filas de la
malla con `file_date` nulo (filtro por `tfrom_date`) y los clientes sin
actividad se perdían entre extract y va.

## Mapeo de variables — adobe_source → datapull

Convención nueva: `bnt_` (bancanet) / `mob_` (mobile) +
`<nombre_lower>` + dominio (`_q` quincenal, `_ld` últimos-N-días,
`_d` secuencia diaria) + `_<tfrom>`.

| adobe_source | datapull |
|---|---|
| `PIL_view` (col en FirstPivot) | `bnt_pil_view_<semana>` (0/1) y `bnt_pil_view_cnt_<semana>` (cuenta) |
| `Flag_interest_LOP` (mobile) | `mob_flag_interest_lop_<semana>` |
| `PIL_view_sum_last_15d` (views_metrics) | `bnt_pil_view_ld_15` |
| `PIL_view_min/max/mean/dev_std_last_Kd` | `bnt_pil_view_cnt_*` + `*_mt` (las stats min/max/std de un 0/1 son triviales); media ≈ `mean_mt` sobre la serie |
| `T_session_sum_last_Kd` (time_metrics) | `bnt_t_session_ld_<K>` |
| `T_session_mean_last_Kd` | `bnt_t_session_ld_mean_<K>` |
| `T_inicio/T_vista_*_last_Kd` | `bnt_t_inicio_ld_<K>` / `bnt_t_vista_ld_<K>` (y stats `*_mt` sobre la serie semanal) |
| `v_max_racha_1` (rachas_metrics) | `longest_1_streak_onz_mt(bnt_pil_view)` semanal, `…(bnt_pil_view_d)` diaria (14d), `…(bnt_pil_view_q)` quincenal |
| `v_first_1` | `first_1_onz_mt` |
| `v_count_10 / v_count_01` | `transitions_onz_mt` |
| `v_first_streak_1` | `first_streaks_onz_mt` |
| `v_count_1s`, `v_last_1`, `v_ratio`, `v_mean_len_*` | `sum_mt`/`mean_mt`/`transitions_onz_mt` sobre la serie binaria; `v_last_1` = `bnt_pil_view_0` |
| `num_hits`/hits por día (count_views) | `bnt_hits_<semana>` (+ `bnt_hits_ld_<w>`) |
| flags por quincena/mes del original (15d) | `bnt_*_q_<quincena>` |

## Config (`configuration.json`)

- `adobe_bancanet` / `adobe_mobile`: bloques de primer nivel con
  `extract` (incl. `extract.adobe` — filtros, `hash_id_db`,
  `keep_columns`), `weeks`, `missing_treatment.mobs` (`mobs_adobe`),
  `group_by` (prefijo, flags, time_vars/stats, dominios),
  `pivoting` (`pivot_key` + `drop_null_values` +
  `keep_pivot_clients`), `fe`, `steps_list`.
- `Extract.special_treatment_sources` += `adobe_bancanet`,
  `adobe_mobile` (`libs/extract.py`).
- `AGGREGATE_CLASSES`/`AGGREGATE_EXTRA_ARGS` (`libs/group_by.py`):
  ambos → `AdobeAggregate` (extra `tables`).
- `MobsStep._run`: los adobe **no** corren `_create_mobs_extract` (como
  las payroll — sus mobs se leen del propio extract).

## Limitaciones / pendientes

- Las ventanas `_ld_105`..`_ld_180` no entran al fe (sufijo 3 dígitos);
  quedan en `raw_features`.
- `weeks` con `force_4_weeks` aplica el snap a lunes — un hit del
  finde anterior puede "viajar" una semana atrás respecto al día real
  (igual que payroll_streaks).
- `T_vista`/`T_session` se calculan por mes de extract — una sesión
  cortada por el borde de mes pierde el contexto previo (el original la
  cortaba por **día**, así que el port es estrictamente mejor).
- El bloque EFT-linking del original (Checkpoints 02–04) está
  desactivado allí y **no** se portó.
