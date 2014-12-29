# SqlContext的使用

SparkSQL提供了两种方式生成SchemaRDD:

1. 通过定义class class，使用类的反射机制生成Schema
2. 自定义Schema，应用到RDD上，生成SchemaRDD

此外SparkSQL还支持Parquet和Json等数据格式的读写以及DSL调用。


### 通过Case Class生成SchemaRDD
SparkSQL中可以通过定义Case Class来将普通的RDD隐式转换成SchemaRDD，从而注册为SQL临时表。

例子代码

```
case class Person(name: String, age: Int) //定义case class

val sqlContext = new SQLContext(sc)
import sqlContext._

val rddpeople = sc.textFile("./sparksql/people.txt").
    map(_.split(",")).map(p => Person(p(0), p(1).trim.toInt))

rddpeople.registerTempTable("rddTable")

rddpeople.printSchema()
sql("SELECT name FROM rddTable WHERE age >= 13 AND age <= 19").
    collect().foreach(println)
```

输出
```
root
  |-- name: string (nullable = true)
  |-- age: integer (nullable = false)
[Justin]
```

RDD隐式转为SchemaRDD的定义在SQLContext中：

```
implicit def createSchemaRDD[A <: Product](rdd: RDD[A])(
  implicit arg0: scala.reflect.api.JavaUniverse.TypeTag[A]
 ): SchemaRDD
```

同时SchemaRDDLike里面定义了registerTempTable方法，用于将RDD注册为临时表格

```
def registerTempTable(tableName: String): Unit
```

而SchemaRDD继承SchemaRDDLike

```
class SchemaRDD extends RDD[Row] with SchemaRDDLike
```

### 通过Apply Schema生成SchemaRDD
由于Scala中对Case Class有22列的限制，而且使用Case Class的方式创建SchemaRDD需要提前知道Schema的格式，所以SparkSQL另外提供了一种更加动态的创建SchemaRDD的方式。SQLContext定义了applySchema方法，可以把RDD+Schema转化为SchemaRDD

```
def applySchema(rowRDD: RDD[Row], schema: StructType): SchemaRDD
```

例子代码

```
val sqlContext = new SQLContext(sc)
import sqlContext._

//创建于数据结构匹配的schema
val schemaString = "name age"
val schema = StructType(schemaString.split(" ").
   map(fieldName => StructField(fieldName, StringType, true)))

//创建rowRDD
val rowRDD = sc.textFile("./sparksql/people.txt").
    map(_.split(",")).
    map(p => Row(p(0), p(1).trim))

//用applySchema将schema应用到rowRDD
val rddpeople2 = applySchema(rowRDD, schema)
rddpeople2.printSchema()
sql("SELECT name FROM rddTable2 WHERE age >= 13 AND age <= 19").
    map(t => "Name: " + t(0)).collect().foreach(println)
```

输出

```
root
 |-- name: string (nullable = true)
 |-- age: string (nullable = true)
Name: Justin
```

### 通过Parquet文件生成SchemaRDD
SQLContext提供了parquetFile和saveAsParquetFile方法用来读写parquet格式的数据

[parquet](http://parquet.incubator.apache.org/)是一种基于列式存储的数据存储格式

例子代码
```
val sqlContext = new SQLContext(sc)
import sqlContext._

rddpeople2.saveAsParquetFile("./sparksql/people.parquet")
val parquetpeople = sqlContext.parquetFile("./sparksql/people.parquet")
parquetpeople.registerTempTable("parquetTable")

parquetpeople.printSchema()
sqlContext.sql("SELECT name FROM parquetTable WHERE age >= 25").
   map(t => "Name: " + t(0)).collect().foreach(println)
```

输出
```
root
  |-- name: string (nullable = true)
  |-- age: string (nullable = true)
Name: Michael
Name: Andy
```

### 通过Json文件生成SchemaRDD
SQLContext定义了jsonFile方法，用来读取json格式的数据

```
def jsonFile(path: String): SchemaRDD
```

例子代码

```
val sqlContext = new SQLContext(sc)
import sqlContext._

val jsonpeople = jsonFile("./sparksql/people.json")
jsonpeople.registerTempTable("jsonTable")

jsonpeople.printSchema()
sql("SELECT name FROM jsonTable WHERE age >= 25").
   map(t => "Name: " + t(0)).collect().foreach(println)
```

输出
```
root
  |-- age: integer (nullable = true)
  |-- name: string (nullable = true)
    Name: Andy
```

### SQL Queries & Language Integrated Queries
有两种方式可以调用sql查询

方法一： 通过调用SQLContext提供的sql函数

```
def sql(sqlText: String): SchemaRDD
```

例子代码
```
val sqlContext = new SQLContext(sc)
import sqlContext._
sql("SELECT name FROM jsonTable WHERE age >= 25").
   map(t => "Name: " + t(0)).collect().foreach(println)
```

方法二： 通过SchemaRDD提供的Language Integrated Queries
```
def where(dynamicUdf: (DynamicRow) -> Boolean): SchemaRDD
def select(exprs: Expression*): SchemaRDD
def orderBy(sortExprs: SortOrder*): SchemaRDD
//etc
```

例子代码
```
rddpeople.
      where('age >= 13).
      where('age <= 19).
      select('name).
      map(t => "Name: " + t(0)).
      collect().foreach(println)
```


