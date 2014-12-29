# SparkSQL的三种使用方式

本篇介绍SparkSQL的三种使用方式

1. Spark Shell
2. CLI
3. Thrift Server


### 通过spark-shell调用SparkSQL
在启动spark shell时添加mysql-jdbc-driver.jar的依赖，然后import HiveContext，就可以正常使用SparkSQL了

运行spark shell
```
export SPARK_CLASSPATH=/usr/lib/hadoop/lib/hadoop-lzo.jar:\
/usr/lib/hive/lib/mysql-jdbc-driver.jar
./bin/spark-shell --master local
```

```
import org.apache.spark.sql.hive.HiveContext
val hiveContext = new HiveContext(sc)
hiveContext.sql("use spark_test")
hiveContext.sql("show tables").collect().foreach(println)
```

输出
```
[tbldate]
[tblstock]
[tblstockdetail]
```

### 通过CLI调用SparkSQL
SparkSQL提供了类似于sql命令行的CLI接口，用户可以直接使用sql语言。

运行spark sql
```
export SPARK_CLASSPATH=/usr/lib/hadoop/lib/hadoop-lzo.jar:\
/usr/lib/hive/lib/mysql-jdbc-driver.jar
./bin/spark-sql --master local
```

```
use spark_test;
show tables;
```

输出
```
[tbldate]
[tblstock]
[tblstockdetail]
```

### 通过ThriftServer调用SparkSQL
开启thrift server
```
export SPARK_CLASSPATH=/usr/lib/hadoop/lib/hadoop-lzo.jar:\
/usr/lib/hive/lib/mysql-jdbc-driver.jar
./sbin/start-thriftserver.sh \
--hiveconf hive.server2.thrift.port=10000 \
--hiveconf hive.server2.thrift.bind.host=localhost \
--master local
```

开启beeline client
```
bash /data/git/spark/com.iqiyi/spark-1.1.0-2.5.0-cdh5.2.0-qiyi/bin/beeline

!connect jdbc:hive2://localhost:10000

use spark_test;
show tables;
```

输出
```
+-----------------+
|     result      |
+-----------------+
| tbldate         |
| tblstock        |
| tblstockdetail  |
+-----------------+
```


