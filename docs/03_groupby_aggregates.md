# 03 — GroupBy, Aggregates, Pivoting y Ratios

## `groupby` — agregación por fuente

**Módulos:** `libs/group_by.py` (clase `GroupBy`) + `libs/aggregattes/` (21 agregadores)

Es el paso que convierte las tablas históricas (cliente × periodo × variables crudas) en
**features agregadas**. El `DataPull` invoca `_perform_groupby`, que instancia `GroupBy`
con `previous_path`, `agg_dict` (sub-config `source_dict["group_by"]`), `n_mis`,
`period`, `tables` y el `pivot_path`, y llama `run()`.

### El dispatcher `_obtain_object`

`GroupBy` es solo un selector: según la fuente elige la clase agregadora concreta en
`libs/aggregattes/`:

| Fuente | Clase | Qué agrega |
|---|---|---|
| `financiera` | `FinancieraAggregate` | Flags de producto por clave (`cves_dict`, `case when b1 in (...)`) |
| `channels` | `ChannelsAggregate` | Conteos y sumas por canal (con `greatest`/`expr`) |
| `product_data` | `ProductDataAggregate` | Join baseline+ccp y features `ind_cnb/cpb/mor/disp/effec` por ventana |
| `corporate` | `CorporateAggregate` | Segmentos `cat_ind_i2/i5`, `cat_iv2/iv5` vía `when/otherwise` anidados (15 categorías) |
| `profundidad` | `ProfundidadAggregate` | Filtra `tfrom==0` (profundidad solo del periodo actual) |
| `pnl` | `PNLAggregate` | Agregación genérica `'{0}({1}) as {1}'` sobre `tempPnl` |
| `pnl_new` | `PNLNewAggregate` | Igual que PNL, nueva versión |
| `pnl_cohort` | `PNLCohortAggregate` | PNL filtrado a `tfrom==0` (cohorte) |
| `laika` | `LaikaAggregate` | Agregador pequeño (header 2026-03-24) |
| `suguru` | `SuguruAggregate` | Filtra `tfrom==0` |
| `s15` / `s15_nf` | `S15Aggregate` / `S15NFAggregate` | Drop `inf_dt`/`tfrom_date` + `fillna(0)` |
| `mokitara` | `MorkitaraAggregate` | GroupBy+agg con pivoteo por snapshot (`sum/max/min/count`) |
| `atms` | `AtmsAggregate` | Agregación de uso de cajeros |
| `macrogiros` | `MacrogirosAggregate` | Features `{giros}`/`{giros}_cnt` + `tot_pur`/`no_trans` por `query_dict` |
| `bureau_cohort` | `BureauCohortAggregate` | Bureau por cohorte con filtro `tfrom<=2` |
| `bureau_pull` | `BureauPullAggregate` | Bureau sin filtro final (`low_filtered_columns`) |
| `liabilities` | `LiabilitiesPullAggregate` | Dos query builders: `liab` y **Pagomático** (`PGMT_QUERY_DICT`) |
| `rev_balnce` | `RevBalanceAggregate` | `epp_aggregate` (promos, join broadcast, oferta), `plan_aggregate`, `revbalance_aggregate` |
| `payroll_income` | `PayrollIncomeAggregate` | Pipeline completo de pasos: pivot, vintage_dates, fix_1/fix_2 (imputación), remove_last_months, cap_and_floor, normalize, vector_removal |
| `payroll_streaks` | `PayrollStreaksAggregate` | Pipeline de pasos: filter (Asigna Nómina + tef_1/tef_2) → tagger (semana del mes, tipo de pago) → renamer → patterns (19 reglas `*_rule`) → pivot por `n_tef` |

### Patrón común de los agregadores

Casi todos siguen la misma plantilla:

```python
class XAggregate:
    def __init__(self, previous_path, pivot_path, tables, agg_dict, n_mis, period, sqlContext, mode): ...
    def _create_groupby_query(self, ...) -> str:      # SQL sobre la vista temp de la tabla
    def aggregate(self) -> DataFrame:                  # lee *_historic, crea tempview, ejecuta query
    def _save_parquet / _delete_intermediate / _clean  # I/O
```

Las queries usan `query_dict` (colname → función) y `low_filtered_columns` para
generar features `{col}`/`{col}_cnt`. Las fuentes `payroll_*` reemplazan el SQL por un
pipeline de `cached_property` + clases `_Step` internas (ver detalle en el propio archivo).

**Salida:** `{working_path}/{year_id}/{source}/groupby/`

---

## `pivoting` — `GeneratePivot` (libs/group_by.py, líneas 41–153)

Pivotea la salida agregada según `source_dict["pivoting"]` (`pivot_dict`):
`pivot_data()` itera los paths configurados y `pivot_data_aux(path, group_by)` hace el
pivot real — columnas `tfrom` pasan a ser features `{var}_{tfrom}` — y escribe en `pivot/`.
También es reutilizado por `product_data` (`pivot_data_aux` se importa desde el
agregador).

**Salida:** `{working_path}/{year_id}/{source}/pivot/`

---

## `ratios` — `GenerateRatios` (libs/generate_ratios.py)

Genera **ratios entre ventanas temporales** de las variables ya calculadas:
`ratios_dict` recibe `time_window` (`_get_time_window(n_mis, period)` → lista de ventanas)
y `n_time`. `run()` lee el paso anterior y produce columnas ratio (numerador/denominador
entre tfroms o ventanas). Salida en `ratios/` — excepto `corporate`, que lo escribe
directo en `features/` (salta el paso `fe`).

---

## Orden real por fuente (ejemplos de `steps_list`)

```text
pnl_new / pnl_cohort : extract → groupby → ratios → fe → mobs → va → cols2vec
payroll_income       : extract(+2 mis) → weeks → mt → groupby → fe → mobs → va → cols2vec
payroll_streaks      : extract → weeks → mt → groupby → fe → mobs → va → cols2vec
bureau_pull          : extract → groupby → fe → mobs → va → cols2vec
```

*(La lista exacta vive en `configuration.json` por fuente; `configuration.json` está
incompleto en esta transcripción — solo `bureau_pull` y los tramos finales son legibles,
los `steps_list` de arriba son los inferidos del código.)*
