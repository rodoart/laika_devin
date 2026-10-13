import sys, json
sys.path.insert(0, 'tests/local')
import bootstrap
from pathlib import Path

import libs.aggregattes.payroll_streaks as ps

spark = bootstrap.get_spark('dbg2')
spark.sparkContext.setLogLevel('ERROR')

config_rigid = json.loads(Path('config/configuration.json').read_text(encoding='utf-8'))
group_by = config_rigid['payroll_streaks']['group_by']

df = spark.read.parquet(bootstrap.POSIX_WORKDIR + '/tmp/payment_frequencies')
df = df.withColumnRenamed('paid_in_week', 'pattern')
print('BASE:', df.columns)

# parent stub with what _Step/_PatternStep need
class P: pass
p = P()
p._columns_not_to_be_renamed = [
    "numcliente", "vintage_date", "n_tef", "tfrom", "tfrom_date", "tfrom_month",
    "week_of_month", "count_paid_in_month", "weeks_of_month",
    "payment_type_per_month", "client_payment_type"]
p.prefixes = [v['prefix'] for k, v in group_by.items()
              if isinstance(v, dict) and 'prefix' in v]
p.hdfs = {}
p.sqlContext = spark
p.is_dynamic = True
p._c = lambda x: x

pattern_keys = [
    "raw_pattern", "sorted_shortened_pattern_forced_weekly",
    "sorted_shortened_pattern_forced_monthly", "sorted_shortened_pattern_forced_fortnightly",
    "sorted_shortened_pattern", "shortened_pattern_forced_weekly",
    "shortened_pattern_forced_monthly", "shortened_pattern_forced_fortnightly",
    "shortened_pattern", "sorted_shortened_pattern_forced_monthly_optimist",
    "sorted_shortened_pattern_forced_fortnightly_optimist", "sorted_shortened_pattern_optimist",
    "shortened_pattern_forced_monthly_optimist", "shortened_pattern_forced_fortnightly_optimist",
    "shortened_pattern_optimist", "payment_type_pattern", "unusual_pattern",
    "reverse_pattern", "inverse_pattern",
]
prev = df
prev_prefix = ''
for pk in pattern_keys:
    cfg = group_by[pk]
    method = getattr(ps, f'{pk}_rule')
    if pk == 'reverse_pattern':
        def method(df, columns_to_apply, prefix, remove_previous_prefix, previous_prefix, _m=ps.reverse_pattern_rule):
            return _m(df, columns_to_apply, prefix, remove_previous_prefix, previous_prefix,
                      vintage_date_column='vintage_date', n_tef_column='n_tef',
                      numcliente_column='numcliente')
    step = ps._PatternStep(parent=p, input_df=prev, step_config=cfg,
                           step_method=method, previous_prefix=prev_prefix)
    cols = step._get_columns_to_apply(step.df)
    print(f"{pk:55s} | apply_to: {sorted(cols)}")
    if pk == 'inverse_pattern':
        raw = method(prev, cols, step.prefix, step.remove_previous_prefix, step.previous_prefix)
        dup0 = [c for c in set(raw.columns) if raw.columns.count(c) > 1]
        print('   rule output dup:', dup0)
        print('   rule cols:', sorted(raw.columns))
    prev = step.final
    prev_prefix = step.prefix
    dup = [c for c in set(prev.columns) if prev.columns.count(c) > 1]
    if dup:
        print('   !! DUPLICATED:', dup)
spark.stop()
