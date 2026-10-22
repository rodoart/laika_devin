# Datapull — Documentación

Serie de documentos que explican **qué es y cómo funciona el datapull** del equipo COE:
el pipeline Spark que extrae tablas fuente desde Hive, las normaliza en ventanas de tiempo
(`tfrom`), aplica tratamiento de faltantes, agrega, genera variables (features) y deja el
resultado listo en Parquet para consumo de modelos.

## Contenido

| Notebook | Tema |
|---|---|
| [00_datapull.md](00_datapull.md) | Visión general: entry point, clase `DataPull`, orquestación, configuración, notificaciones |
| [01_extract.md](01_extract.md) | Extracción de datos (Hive → Parquet), cohortes, tratamientos especiales |
| [02_weeks_missing_treatment.md](02_weeks_missing_treatment.md) | Reetiquetado semanal (`weeks`) y tratamiento de faltantes (`mt`) |
| [03_groupby_aggregates.md](03_groupby_aggregates.md) | Agregación por fuente (`groupby`), pivoteo (`pivoting`) y ratios |
| [04_feature_engineering.md](04_feature_engineering.md) | Transformaciones de variables (estándar, MT, ONZ, externas) |
| [05_mobs_vector_assembly.md](05_mobs_vector_assembly.md) | Cálculo de MOBs, ensamblado de vectores y salida final |
| [06_step_framework.md](06_step_framework.md) | Framework de `Step`s: ejecución dinámica, persistencia/reanudación, supervisor y sesiones |
| [07_features.md](07_features.md) | Catálogo de features: transformaciones estándar, variantes MT/ONZ, `ratiosmod`, convención de nombres |
| [08_execution.md](08_execution.md) | Cómo ejecutar: clúster (`run_fs.sh`, `main_dynamic`, supervisor) y local (env, datos tiny, flags) |
| [09_adobe_source.md](09_adobe_source.md) | Fuentes Adobe Clickstream: integración `adobe_bancanet`/`adobe_mobile`, dominios de tfrom, mapeo de variables |

## En una frase

> **Datapull** = para cada fuente configurada, ejecutar una secuencia de pasos
> (`steps_list`) que van transformando tablas crudas en una matriz de variables
> indexada por `numcliente` × `vintage_date` × `tfrom`, lista para modelado.
