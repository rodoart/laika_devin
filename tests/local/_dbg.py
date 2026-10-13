import sys, os
sys.path.insert(0, 'tests/local')
import bootstrap
spark = bootstrap.get_spark('dbg')
jvm = spark.sparkContext._gateway.jvm
print('JLP:', jvm.java.lang.System.getProperty('java.library.path'))
print('PATH head:', os.environ['PATH'][:300])
spark.stop()
