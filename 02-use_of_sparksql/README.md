# SparkSQL的使用基础

SparkSQL引入了一种新的RDD——SchemaRDD，SchemaRDD由行对象（row）以及描述行对象中每列数据类型的schema组成；SchemaRDD很象传统数据库中的表。SchemaRDD可以通过RDD、Parquet文件、JSON文件、或者通过使用hiveql查询hive数据来建立。SchemaRDD除了可以和RDD一样操作外，还可以通过registerTempTable注册成临时表，然后通过SQL语句进行操作。

通过函数registerTempTable注册的表是一个临时表，生命周期只在所定义的sqlContext或hiveContext实例之中。换而言之，在一个sqlContext（或hiveContext）中registerTempTable的表不能在另一个sqlContext（或hiveContext）中使用。

另外，spark1.1开始提供了语法解析器选项spark.sql.dialect，就目前而言，spark提供了两种语法解析器：sql语法解析器和hiveql语法解析器。
1. sqlContext现在只支持sql语法解析器（SQL-92语法）
2. hiveContext现在支持sql语法解析器和hivesql语法解析器，默认为hivesql语法解析器，用户可以通过配置切换成sql语法解析器，来运行hiveql不支持的语法，如select 1。

hiveContext继承了sqlContext，所以拥有sqlContext的特性之外，还拥有自身的特性（最大的特性就是支持hive）。

### 编译Spark

maven编译

```
mvn -Pyarn -Dhadoop.version=2.5.0-cdh5.2.0 -Pkinesis-asl -Phive -DskipTests
```

maven编译并打包

```
./make-distribution.sh --tgz --name name -Pyarn \
-Dhadoop.version=2.5.0-cdh5.2.0 -Pkinesis-asl -Phive -DskipTests
```

### 本篇所用到的数据

[people.json](/data/sparksql/people.json)

```
{"name":"Michael"}
{"name":"Andy", "age":30}
{"name":"Justin", "age":19}
```


[people.txt](/data/sparksql/people.txt)

```
Michael, 29
Andy, 30
Justin, 19
```

[Date.txt](/data/sparksql/Date.txt)

[Stock.txt](/data/sparksql/Stock.txt)

[StockDetail.txt](/data/sparksql/StockDetail.txt)

[创建hive表格代码](/data/sparksql/Hive_CMD.sql)


