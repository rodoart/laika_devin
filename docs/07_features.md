# 07 — Catálogo de features

Qué produce la etapa `fe` (`FeatureEngineering`, `libs/feature_engineering.py`)
y cómo se nombran las columnas/parquets resultantes.

Entrada: la tabla wide del pivot — una fila por `(numcliente,
vintage_date)` y una columna por variable × tfrom
(`<var>_<t0..tn>`, más las etiquetas de patrón de
`payroll_streaks`). Salida: un parquet por transformación
`features/<trans>_features`, todos keyed por
`(numcliente, vintage_date)`.

Las transformaciones a ejecutar se declaran en
`config_custom.json -> lista_trans` y se corren en paralelo con
`n_threads` threads. **El orden no importa**: cada transformación es
independiente salvo `ratiosmod` (ver abajo).

## Grupos de ventanas

Cada transformación se aplica sobre dos posibles agrupaciones de
columnas, elegidas por la lista que consume:

| Lista | Contenido |
|---|---|
| `lista_metodo` | Ventanas deslizantes de tamaño `period` (meses) sobre el histórico completo de cada variable. |
| `list_mvars` | Una ventana por variable: todos los tfroms de esa variable. |

`period` viene de `config_custom.json` (típicamente 3). Con `n_mis=12`
y `period=3`, cada variable genera ventanas `t0..t2`, `t0..t5`,
`t0..t8`, `t0..t11` para las transformaciones multi-ventana
(`incremental`, `so`, `weighted_mean`, …) — el sufijo de la feature
indica el largo (`i2`, `i5`, …).

## Transformaciones estándar (`_XTransformation`)

| Nombre | Fórmula / descripción |
|---|---|
| `mean` | Media de la ventana. |
| `std` | Desviación estándar. |
| `sum` | Suma de la ventana. |
| `min` / `max` | Mínimo / máximo. |
| `logmean` | Media de `log(x)` (valores > 0). |
| `square` | Media de `x²`. |
| `inverse` | Media de `1/x` (x≠0). |
| `trend` | Pendiente de regresión lineal sobre los índices de la ventana. |
| `rmeans` | Ratios entre medias de ventanas consecutivas. |
| `incremental` | Nº de subidas consecutivas: `Σ 1[x_{i+1} > x_i]` por subventanas `period`-alineadas. |
| `vp` | Media y std de las variaciones porcentuales `(x_{i+1}-x_i)/x_i` (denominador 0 → ×1e5). |
| `rt` | Media y std de los ratios `x_{i+1}/x_i` (denominador 0 → ×1e5). |
| `so` | Oscilador estocástico `(μ − min)/(max − min)` por ventana `period`; rango 0 → 1. |
| `avgmov` | Medias móviles acumuladas a lo largo del histórico. |
| `rms` | Root mean square `√(1/n Σ x_i²)`. |
| `cmpx` | Complejidad `√(Σ (x_{i+1}−x_i)²)` — magnitud del zig-zag. |
| `skew` | Skewness muestral (Fisher-Pearson G1) — 0 si n<4. |
| `curt` | Curtosis muestral (G2) — 0 si n<4. |
| `weighted_mean` | Media ponderada con pesos lineales `linspace(1, w)`; una feature por `w` de `fe_dict.weights_dict` (sufijo de 3 dígitos). |
| `ratiosmod` | **Meta-transformación**: ratios y deltas entre `min/max/mean/std/sum` ya calculados. |

### `ratiosmod` — dependencias

`ratiosmod` combina los parquets `min/max/mean/std/sum_features`
existentes. Solo calcula el par `(a,b)` si **ambos** parquets base
existen; si falta alguno elimina todas las combinaciones que lo usan:

- Ratios `rt_mod = a/b` (0 si b=0): `min/max`, `min/mean`,
  `max/mean`, `std/sum`, `std/mean`.
- Deltas `df_mod = a−b`: `max−min`, `mean−min`, `max−mean`.

Existe la variante **`ratiosmod_mt`** (mismo esquema sobre las bases
`*_mt`: `min_mt/max_mt/mean_mt/std_mt/sum_mt`, ignorando
`ignore_this_value_when_calculating_mt_features`). Escribe
`r{a}{b}_features` / `d{a}{b}_features` con nombres `rmin_mtmax_mt`, etc.

**Para activarla** basta añadirla a `lista_trans` de `config_custom`
(el propio `run()` la ejecuta la última, después de sus bases). Si
alguna base falta en `lista_trans`, esos pares se omiten en silencio —
ej. para `ratiosmod_mt` hacen falta las 5 `*_mt` (incluida `min_mt`):

```json
"lista_trans": [ ..., "min_mt", "max_mt", "mean_mt", "std_mt",
                 "sum_mt", "ratiosmod_mt" ]
```

Al re-ejecutar, los parquets ya presentes se recargan (resume a mitad
de etapa): `_fe_performed` detecta los dirs `r*/d*_features` extra y
no repite la transformación.

## Variantes MT (`_x_mtTransformation`)

Mismo catálogo estándar, aplicado con el modificador **MT**
("missing treated"): se ignoran los valores
`fe_dict.ignore_this_value_when_calculating_mt_features`
(default `-99999`) al computar. Sufijo `_mt` en nombre de feature y
parquet (`mean_mt_features`, …).

## Variantes ONZ (`OnesAndZerosMethods`)

Pensadas para columnas de patrón binario (0/1/9) como las de
`payroll_streaks`:

| Nombre | Descripción |
|---|---|
| `entropy_onz` | Entropía de Shannon de la distribución de valores. |
| `first_1_onz` | Posición (1-based) del primer 1; `default_non_found_value` si no hay. |
| `longest_1_streak_onz` | Longitud de la racha de 1s más larga. |
| `first_streaks_onz` | Posición de la primera racha de 1s de longitud k, k=2..7 (una feature por k: `first_{k}o1_streak_onz`). |
| `find_pattern_onz` | Detección de subpatrones concretos en la cadena. |
| `transitions_onz` | Nº de transiciones 0↔1. |
| `autocorrelation_onz` | Autocorrelación de la serie binaria. |
| `kolmogorov_complexity_onz` | Aproximación a la complejidad de Kolmogórov (compresibilidad del patrón). |
| `pattern_diversity_onz` | Diversidad de subpatrones distintos. |
| `is_new_onz_mt` | Solo en la variante MT: flag de patrón nuevo vs. histórico. |

Sufijos: `*_onz` (sin tratar) y `*_onz_mt` (ignorando el valor MT).

## Convención de nombres

```
<trans>_<ventana>_<variable>      p. ej. mean_i5_tef, sum_i2_importe
features/<trans>_features/        un parquet por transformación
```

## Raw variables

`_create_raw_variables` materializa `features/raw_features` con las
columnas de entrada tal cual (`_check_raw` solo verifica); es la base
que referencian las queries de transformación sobre la vista `df`.
`_fill_na_zero` rellena nulos a 0 antes de calcular.

## MOBs

Los MOBs (`mobs_preparation`) se imputan por fuente con las clases
`PreviousFixer*` de `libs/mobs/`; `list_no_mobs` en
`configuration.json` excluye parquets de features de ese cálculo
(actualmente `incremental`, `raw`, `rmeans`, `rt`, `vp`).

## Ensamblado final

- `va` (`FeaturesVectorAssembler`): junta todos los `*_features` en
  una tabla única por `(numcliente, vintage_date)`.
- `cols2vec` (`ColumnsToVector`): empaqueta las columnas en un vector
  `features` (VectorUDT) listo para el modelo.
