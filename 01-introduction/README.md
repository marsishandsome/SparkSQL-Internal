# SparkSQL介绍
2014年9月SparkSQL发布了1.1.0版本，紧接着在12月又发布了1.2.0版本。

### SparkSQL1.1.0
[(release note)](https://spark.apache.org/releases/spark-release-1-1-0.html)

版本主要的变动有：
1. 增加了JDBC/ODBC Server (ThriftServer)，用户可以在应用程序中连接到SparkSQL并使用其中的表和缓存表。
2. 增加了对JSON文件的支持
3. 增加了对parquet文件的本地优化
4. 增加了支持将python、scala、java的lambda函数注册成UDF，并能在SQL中直接使用。
5. 引入了动态字节码生成技术（bytecode generation，即CG），明显地提升了复杂表达式求值查询的速率。
6. 统一API接口，如sql()、SchemaRDD生成等。


### SparkSQL1.2.0
[(release note)](https://spark.apache.org/releases/spark-release-1-2-0.html)

版本主要的变动有：

1. 增加访问外部数据的新API，通过该API可以在运行时将外部数据注册为临时表格，并且支持predicte pushdown方式的优化，Spark原生支持Parquet和JSON格式，用户可以自己编写读取其他格数据的代码。
2. 增加支持Hive 0.13，增加对fixed-precision decimal数据类型的支持。
3. 增加[dynamically partitioned inserts](https://issues.apache.org/jira/browse/SPARK-3007)
4. 优化了cache SchemaRDD，并重新定义了其语义，并增加了[statistics-based partition pruning](https://issues.apache.org/jira/browse/SPARK-2961)。
















