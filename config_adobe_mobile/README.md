# Datapull `adobe_mobile` — variables solicitadas (Tania / Adobe)

Config para generar las variables **mobile** que pidió la compañera de
Adobe (`mb_*`), a partir de su pivote.

## Lo que pidió

- **Pivote**: `/data/gcgamdlmxpysp/work/hive/gcgamdlmxpysp_work/pricing_models/adobe/pivote_test/C4_2023`
- **Vintage**: `2023-06-30` (99,863 clientes)
- **Requisito**: información **diaria** con **12 meses** de historia.
- **Fuente**: únicamente Mobile (`adobe_mobile` →
  `gcgdlkmxpsd_dch_db.adobeanalytics_hit_data_mxmprod_t_d`, particionada por día).

## Archivos

```
config_adobe_mobile/
├── configuration.json     # copia de config/configuration.json + 3 ajustes en adobe_mobile
├── config_custom.json     # sources/pivote/year_id/n_mis/period/lista_trans
└── README.md              # este documento
```

Cambios en `configuration.json` vs la de prod (solo en
`adobe_mobile.group_by.domains.last_days`):

| Cambio | Motivo |
|---|---|
| `windows += [240, 330, 360]` | `T_vista_sum_last_240d`, `T_inicio_sum_last_330d`, `Flag_interest_PIL_sum_last_360d` (prod llegaba solo a 180) |
| `time_stats += ["stddev"]` | `T_session_dev_std_last_150d` |

`config_custom.json`:

| Clave | Valor | Nota |
|---|---|---|
| `sources` | `["adobe_mobile"]` | solo mobile |
| `working_path` | `.../pricing_models/adobe/datapull_out` | **ajustar** al work schema donde quiera la salida |
| `year_id` | `202306` | etiqueta del run (vintage jun-2023) |
| `n_mis` | `12` | 12 meses de historia (cubre `last_360d`) |
| `period` | `4` | 4 semanas ≈ 1 mes → ventanas `_*w` del fe |
| `pivot_path` | `.../pivote_test/C4_2023` | su pivote (columnas `numcliente`, `vintage`) |
| `lista_trans` | `sum_mt`, `mean_mt`, `std_mt`, `max_mt`, `min_mt`, `trend_mt`, `longest_1_streak_onz_mt`, `first_1_onz_mt`, `transitions_onz_mt`, `first_streaks_onz_mt`, `entropy_onz_mt`, `kolmogorov_complexity_onz_mt` | `sum_mt` = `v_count_1`; `longest_1_streak_onz_mt` = `v_max_racha_1` |
| `email_sender`/`email_receivers` | `rm36973` | **ajustar** (o correr con `--no-email`) |

## Cómo ejecutar (clúster)

Con el entrypoint resumable (recomendado — relanza sin recomputar):

```bash
cd <repo>
export LAIKA_CONFIG_DIR=config_adobe_mobile
spark3-submit --queue root.datalabs --master yarn --deploy-mode client main_dynamic.py
```

Con el flujo clásico (`run_fs.sh` / `main.py`), copiar primero los dos
JSON a `config/`:

```bash
cp config_adobe_mobile/configuration.json config/
cp config_adobe_mobile/config_custom.json   config/
sh run_fs.sh
```

Salida: `<working_path>/202306/adobe_mobile/` con `extract/`, `groupby/`,
`pivot/`, `features/` (incl. `raw_features` y `<trans>_features`),
`mobs/`, `vector_assembler/`.

## Mapeo de variables pedidas → columnas generadas

La convención del datapull es `mob_<nombre>_…` (no `mb_`). Las variables
`sum_last_Nd` salen como columnas `mob_*_ld_<N>` en `pivot/` y
`features/raw_features/` (el `va` las mete dentro del vector, no como
columna suelta — ver nota abajo).

| Pedida (adobe_source) | Columna generada | Dónde queda |
|---|---|---|
| `mb_Flag_interest_LOP_sum_last_30d` | `mob_flag_interest_lop_ld_30` | `pivot/`, `features/raw_features/` |
| `mb_Flag_interest_LOP_sum_last_150d` | `mob_flag_interest_lop_ld_150` | idem |
| `mb_Flag_interest_PIL_sum_last_360d` | `mob_flag_interest_pil_ld_360` | idem (ventana 360 añadida) |
| `mb_T_inicio_sum_last_330d` | `mob_t_inicio_ld_330` | idem (ventana 330 añadida) |
| `mb_T_vista_sum_last_240d` | `mob_t_vista_ld_240` | idem (ventana 240 añadida) |
| `mb_T_session_dev_std_last_150d` | `mob_t_session_ld_stddev_150` | idem (`stddev` añadido a `last_days.time_stats`) |
| `mb_v_count_1_Flag_interest_LOP_12m` | `sum_mt_mob_flag_interest_lop_48w` | `features/sum_mt_features/` + vector `va` |
| `mb_v_count_1_Flag_interest_LOP_<Nm>` | `sum_mt_mob_flag_interest_lop_<4N>w` | 1m→`_4w`, 3m→`_12w`, 6m→`_24w` |
| `mb_v_max_racha_0_Flag_interest_TDC_1m` | **sin equivalente directo** — ver abajo | `longest_1_streak_onz_mt` da racha de **unos** |

## Lectura rápida del resultado

```python
# las *_sum_last_Nd viven en raw_features (y en pivot/)
df = spark.read.parquet(
    "<working_path>/202306/adobe_mobile/features/raw_features")
df.select("numcliente", "vintage",
          "mob_flag_interest_lop_ld_30",
          "mob_flag_interest_lop_ld_150",
          "mob_flag_interest_pil_ld_360",
          "mob_t_inicio_ld_330",
          "mob_t_vista_ld_240",
          "mob_t_session_ld_stddev_150")
```

## Notas honestas

- **`v_count_1_*_Nm`** → `sum_mt_..._<4N>w` cuenta **semanas con
  actividad** dentro de la ventana acumulada (la serie `mob_flag_*`
  semanal es `max(flag)` por semana). `48w` ≈ 12 meses.
- **`v_max_racha_0`** (racha de *ceros*): no existe transform directo;
  `longest_1_streak_onz_mt` calcula racha de **unos**
  (`longest_1_streak_onz_mt_mob_flag_interest_tdc_4w` ≈ `v_max_racha_1`).
  Para racha de ceros, post-procesar el vector binario semanal
  `mob_flag_interest_tdc_<w>` (o el diario `mob_flag_interest_tdc_d_<día>`):

  ```python
  def max_racha_0(cols):
      best = cur = 0
      for c in cols:
          cur = cur + 1 if c == 0 else 0
          best = max(best, cur)
      return best
  ```
- **`dev_std`** → `stddev` de Spark (desviación **muestral**).
- **`T_inicio` / `T_vista` / `T_session`** están en **segundos**
  (diferencias `unix_timestamp` dentro de la sesión).
- Los clientes sin actividad llegan con flags a 0
  (`pivoting.keep_pivot_clients: true` — los 99,863 del pivote quedan
  cubiertos) y sus ventanas imputadas a `-99999` vía mobs.
- Sufijos de 3 dígitos (`_ld_105+`, incl. 240/330/360) no entran a los
  transforms `*_mt` (regla `≤2 dígitos`) — están directamente
  agregados en `raw_features`, que es lo que se necesita.
- `n_mis=12` + `retraso=0` → extract de ~12 meses terminando en el mes
  del vintage (jun-2023): `last_360d` (≈5-jul-2022) queda cubierto por
  el primer mes extraído (jul-2022).
