# 常用操作
 下面介绍一下hive/console的常用操作，主要是和运行计划相关的常用操作。在操作前，首先定义一个表people和查询query。在控制台逐行运行

```
import org.apache.spark.sql.hive.HiveContext

case class Person(name:String, age:Int, state:String)

val hiveContext = new HiveContext(sc)
import hiveContext._

sc.parallelize(
Person("Michael",29,"CA")::
Person("Andy",30,"NY")::
Person("Justin",19,"CA")::
Person("Justin",25,"CA")::Nil).
registerTempTable("people")

val query= sql("select * from people")
```

### 查看查询的schema
```
query.printSchema
```

输出
```
root
 |-- name: string (nullable = true)
 |-- age: integer (nullable = false)
 |-- state: string (nullable = true)
```

### 查看查询的整个运行计划
```
query.queryExecution
```

输出
```
== Parsed Logical Plan ==
'Project [*]
 'UnresolvedRelation None, people, None

== Analyzed Logical Plan ==
Project [name#0,age#1,state#2]
 LogicalRDD [name#0,age#1,state#2], MapPartitionsRDD[1] at mapPartitions at ExistingRDD.scala:36

== Optimized Logical Plan ==
LogicalRDD [name#0,age#1,state#2], MapPartitionsRDD[1] at mapPartitions at ExistingRDD.scala:36

== Physical Plan ==
PhysicalRDD [name#0,age#1,state#2], MapPartitionsRDD[1] at mapPartitions at ExistingRDD.scala:36

Code Generation: false
== RDD ==
```

### 查看查询的Unresolved LogicalPlan
```
query.queryExecution.logical
```

输出
```
'Project [*]
 'UnresolvedRelation None, people, None
```

### 查看查询的analyzed LogicalPlan
```
query.queryExecution.analyzed
```

输出
```
Project [name#0,age#1,state#2]
 LogicalRDD [name#0,age#1,state#2], MapPartitionsRDD[1] at mapPartitions at ExistingRDD.scala:36
```

### 查看优化后的LogicalPlan
```
query.queryExecution.optimizedPlan
```

输出
```
LogicalRDD [name#0,age#1,state#2], MapPartitionsRDD[1] at mapPartitions at ExistingRDD.scala:36
```

### 查看物理计划
```
query.queryExecution.sparkPlan
```

输出
```
PhysicalRDD [name#0,age#1,state#2], MapPartitionsRDD[1] at mapPartitions at ExistingRDD.scala:36
```

### 查看RDD的转换过程
```
query.toDebugString
```

输出
```
== Query Plan ==
== Physical Plan ==
PhysicalRDD [name#0,age#1,state#2], MapPartitionsRDD[1] at mapPartitions at ExistingRDD.scala:36 []
 |  MapPartitionsRDD[1] at mapPartitions at ExistingRDD.scala:36 []
 |  ParallelCollectionRDD[0] at parallelize at <console>:21 []
```



