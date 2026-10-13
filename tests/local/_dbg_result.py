import sys
sys.path.insert(0, 'tests/local')
import bootstrap
spark = bootstrap.get_spark('dbg3')
spark.sparkContext.setLogLevel('ERROR')
df = spark.read.parquet(bootstrap.POSIX_WORKDIR + '/output/payroll_streaks_groupby')
df.select('numcliente','vintage_date','tfrom','raw_pattern_tef_1','srt_shr_w_pattern_tef_1','inv_raw_pattern_tef_1','unusual_tef_1').orderBy('numcliente','vintage_date','tfrom').show(20, truncate=False)
spark.stop()
