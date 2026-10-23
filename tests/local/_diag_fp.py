"""Diagnostico: fingerprint de un parquet del workdir dorado vs golden json."""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
import bootstrap  # noqa: E402
import importlib.util  # noqa: E402

spec = importlib.util.spec_from_file_location(
    "re", Path(__file__).parent / "regression_e2e.py")
re_mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(re_mod)

spark = bootstrap.get_spark("diag")
gold = json.loads(
    (Path(__file__).parent / "golden_regression.json").read_text())
w = Path(__file__).parent / "workdir_regression_dyn"

for rel in [
    "202601/payroll_income/extract/asigna_nomina_dt1_t_m_historic",
    "202601/bureau_pull/groupby/adv_colocacion_1_t_m_historic",
]:
    fp = re_mod._df_fingerprint(spark, w / rel)
    ref = gold[rel]
    gs = set(map(tuple, ref["schema"]))
    cs = set(map(tuple, fp["schema"]))
    print(rel)
    print("  schema equal:", fp["schema"] == ref["schema"])
    print("  gold_only:", sorted(gs - cs)[:8])
    print("  cur_only:", sorted(cs - gs)[:8])
    print("  hash:", fp["hash"] == ref["hash"], "n:", fp["n"], ref["n"])
