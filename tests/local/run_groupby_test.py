"""
Local smoke test: run PayrollStreaksAggregate (the `groupby` step of the
payroll_streaks source) end-to-end on tiny synthetic tables, using the real
config from config/configuration.json + config-custom_payroll_streaks_*.json.

Run (with the laika_datapull_py310 conda env):
    python tests/local/make_tiny_data.py
    python tests/local/run_groupby_test.py

Outputs land in LAIKA_TEST_WORKDIR (parquet + summary.txt).
"""
import json
from pathlib import Path

from bootstrap import PROJECT_ROOT, WORKDIR, POSIX_WORKDIR, get_spark, install_aggregates_alias


def main() -> None:
    install_aggregates_alias()
    from libs.aggregattes.payroll_streaks import PayrollStreaksAggregate

    config_rigid = json.loads((PROJECT_ROOT / "config" / "configuration.json").read_text(encoding="utf-8"))
    config_custom = json.loads(
        (PROJECT_ROOT / "config" / "config-custom_payroll_streaks_regular_features.json")
        .read_text(encoding="utf-8"))

    source_dict = config_rigid["payroll_streaks"]
    agg_dict = source_dict["group_by"]
    tables = [t + "_historic" for t in source_dict["extract"]["table_name"]]

    previous_path = f"{POSIX_WORKDIR}/extract"
    pivot_path = f"{POSIX_WORKDIR}/pivot"
    n_mis = int(config_custom["n_mis"])
    period = int(config_custom["period"])

    spark = get_spark("laika_groupby_test")
    spark.sparkContext.setLogLevel("ERROR")

    agg = PayrollStreaksAggregate(
        previous_path=previous_path,
        pivot_path=pivot_path,
        tables=tables,
        agg_dict=agg_dict,
        n_mis=n_mis,
        period=period,
        sqlContext=spark,
        mode="overwrite",
    )
    result = agg.aggregate()

    out_path = f"{POSIX_WORKDIR}/output/payroll_streaks_groupby"
    result.write.mode("overwrite").parquet(out_path)

    n_cols = len(result.columns)
    n_rows = result.count()
    summary = [
        f"rows: {n_rows}",
        f"cols: {n_cols}",
        "",
        "columns:",
        *[f"  {c}" for c in result.columns],
    ]
    (WORKDIR / "output").mkdir(parents=True, exist_ok=True)
    (WORKDIR / "output" / "summary.txt").write_text("\n".join(summary), encoding="utf-8")
    print("\n".join(summary[:10]))
    print(f"... ({n_cols} cols total)")
    print(f"result parquet: {out_path}")
    spark.stop()


if __name__ == "__main__":
    main()
