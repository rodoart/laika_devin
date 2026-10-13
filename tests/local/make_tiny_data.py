"""
Generate tiny input tables for local datapull tests.

Creates, under LAIKA_TEST_WORKDIR:
    pivot/                                   (numcliente, vintage_date)
    extract/asigna_nomina_dt1_t_m_historic/  (asigna nomina)
    extract/nomina_dt1_t_m_historic/         (nomina pagos)

3 clients x 2 vintages x <=16 weekly tfroms (n_mis=4 -> n_mis*4).
Payment patterns: client 1001 weekly, 1002 fortnightly, 1003 monthly.

Run:  python tests/local/make_tiny_data.py
"""
import os
from datetime import date, timedelta

from bootstrap import WORKDIR, POSIX_WORKDIR, get_spark
from dateutil.relativedelta import relativedelta

N_MIS = 4
VINTAGES = [date(2024, 1, 31), date(2024, 2, 29)]
DAY_OF_WEEK_STARTS = [22, 16, 8, 1]  # same as _FilterStep.dates_collection

# (numcliente, tef_1, tef_2, paid_tfroms)
CLIENTS = {
    1001: (111.0, 999.0, list(range(16))),                # weekly
    1002: (222.0, 888.0, [0, 2, 4, 6, 8, 10, 12, 14]),    # fortnightly
    1003: (333.0, 777.0, [0, 4, 8, 12]),                  # monthly
}


def tfrom_dates_for_vintage(vintage: date, n_mis: int) -> list:
    """Replicates _FilterStep.dates_collection date math (reversed tfrom order)."""
    last_tfrom = n_mis * 4 - 1
    tfrom = 0.0
    tfrom_month = date(vintage.year, vintage.month, 1) + relativedelta(months=1)
    out = []
    while tfrom <= last_tfrom:
        tfrom_month = tfrom_month - relativedelta(months=1, day=1)
        for day_start in DAY_OF_WEEK_STARTS:
            tfrom_date = tfrom_month - relativedelta(day=day_start)
            out.append((tfrom, tfrom_date, tfrom_month))
            tfrom += 1.0
    return out


def main() -> None:
    spark = get_spark("laika_make_tiny_data")
    spark.sparkContext.setLogLevel("ERROR")

    # --- pivot ---
    pivot_rows = [
        {"numcliente": c, "vintage_date": v}
        for c in CLIENTS for v in VINTAGES
    ]
    pivot = spark.createDataFrame(pivot_rows)
    pivot.write.mode("overwrite").parquet(f"{POSIX_WORKDIR}/pivot")

    # --- asigna nomina (client -> tef_1, tef_2) ---
    asigna_rows = []
    for cli, (tef1, tef2, _) in CLIENTS.items():
        for v in VINTAGES:
            asigna_rows.append({
                "numcliente": float(cli), "vintage_date": v,
                "tef": tef1, "tef_1": tef1, "tef_2": tef2,
                "tfrom": 0.0,
                "tfrom_date": v, "process_date": v,
            })
    asigna = spark.createDataFrame(asigna_rows)
    asigna.write.mode("overwrite").parquet(
        f"{POSIX_WORKDIR}/extract/asigna_nomina_dt1_t_m_historic")

    # --- nomina (payments on tef_1 at paid tfroms) ---
    nomina_rows = []
    for cli, (tef1, _tef2, paid) in CLIENTS.items():
        for v in VINTAGES:
            for tfrom, tfrom_date, tfrom_month in tfrom_dates_for_vintage(v, N_MIS):
                if int(tfrom) not in paid:
                    continue
                nomina_rows.append({
                    "numcliente": float(cli), "vintage_date": v,
                    "tef": tef1, "importe": 1000.0,
                    "tfrom": tfrom, "tfrom_date": tfrom_date,
                    "tfrom_month": tfrom_month, "process_date": v,
                    "fecha": tfrom_date,
                })
    nomina = spark.createDataFrame(nomina_rows)
    nomina.write.mode("overwrite").parquet(
        f"{POSIX_WORKDIR}/extract/nomina_dt1_t_m_historic")

    print(f"pivot:            {pivot.count()} rows")
    print(f"asigna_historic:  {asigna.count()} rows")
    print(f"nomina_historic:  {nomina.count()} rows")
    print(f"workdir:          {WORKDIR}")
    spark.stop()


if __name__ == "__main__":
    main()
