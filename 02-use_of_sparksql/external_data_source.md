# 外部数据源
随着Spark1.2的发布，Spark SQL开始正式支持外部数据源。Spark SQL开放了一系列接入外部数据源的接口，来让开发者可以实现。这使得Spark SQL支持了更多的类型数据源，如json, parquet, avro, csv格式。只要我们愿意，我们可以开发出任意的外部数据源来连接到Spark SQL。HBASE，Cassandra都可以用外部数据源的方式来实现无缝集成。


### API方式创建外部数据源表
在Spark1.2之前就已经支持了api方式穿件外部数据源表，支持
1. json
2. parquet
3. hive meta store

```
    val sqlContext = new SQLContext(sc)
    import sqlContext._

    val peopleParquet = parquetFile("./sparksql/people.parquet")

    peopleParquet.registerTempTable("parquetTable")

    sql("SELECT name FROM parquetTable WHERE age >= 25").
      map(t => "Name: " + t(0)).collect().foreach(println)
```

### DDL创建外部数据源表
在Spark1.2之后，支持了一种CREATE TEMPORARY TABLE USING OPTIONS的DDL语法来创建外部数据源的表。Spark自带实现了
1. org.apache.spark.sql.parquet
2. org.apache.spark.sql.json

未来准备把hive meta store也整合到该框架中。

```
    val sqlContext = new SQLContext(sc)
    import sqlContext._

    val peopleParquet = sql(
      s"""
        |CREATE TEMPORARY TABLE parquetTable
        |USING org.apache.spark.sql.parquet
        |OPTIONS (
        |path './sparksql/people.parquet'
        |)""".stripMargin)

    println(peopleParquet.toDebugString)

    sql("SELECT name FROM parquetTable WHERE age >= 25").
      map(t => "Name: " + t(0)).collect().foreach(println)
```

### 自定义外部数据源格式
使用DDL方式创建外部数据源表，使得自定义数据格式成为可能，用户可以自己实现读取外部表的代码，动态插入SparkSQL中，就可以实现自定义外部数据源格式。

可以参考Databricks在github上提交了读取avro格式的自定义数据源，[spark-avro](https://github.com/databricks/spark-avro)。




