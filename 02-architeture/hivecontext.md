# HiveContext的运行过程
由于历史原因，实际应用中很多数据已经定义了hive meta data，使用SparkSQL的HiveContext可以无缝访问这些数据。使用HiveContext前只需要把hive-site.xml复制到spark/conf/中。

HiveContext继承自SQLContext，在hiveContext的的运行过程中除了override的函数和变量，可以使用和sqlContext一样的函数和变量。
```
/**
 * An instance of the Spark SQL execution engine that integrates with data stored in Hive.
 * Configuration for Hive is read from hive-site.xml on the classpath.
 */
class HiveContext(sc: SparkContext) extends SQLContext(sc) {
...
override def sql(sqlText: String): SchemaRDD = {
    // TODO: Create a framework for registering parsers instead of just hardcoding if statements.
    if (dialect == "sql") {
      super.sql(sqlText)
    } else if (dialect == "hiveql") {
      new SchemaRDD(this, ddlParser(sqlText).getOrElse(HiveQl.parseSql(sqlText)))
    }  else {
      sys.error(s"Unsupported SQL dialect: $dialect.  Try 'sql' or 'hiveql'")
    }
  }
...
}
```
HiveContext使用HiveContext.sql(sqlText)来提交用户查询。hiveContext.sql首先根据用户的语法设置（spark.sql.dialect）决定具体的执行过程，如果dialect == "sql"则采用sqlContext的sql语法执行过程；如果是dialect == "hiveql"，则采用hiveql语法执行过程。在这里我们主要看看hiveql语法执行过程。

首先会尝试利用DDLParser进行ddl语法解析，如果失败的话，则进行HiveQl进行sql语法解析，并返回SchemaRDD。

```
 /** Returns a LogicalPlan for a given HiveQL string. */
  def parseSql(sql: String): LogicalPlan = hqlParser(sql)

  protected val hqlParser = {
    val fallback = new ExtendedHiveQlParser
    new SparkSQLParser(fallback(_))
  }

private[sql] class SparkSQLParser(fallback: String => LogicalPlan) extends AbstractSparkSQLParser {
...
  private lazy val others: Parser[LogicalPlan] =
    wholeInput ^^ {
      case input => fallback(input)
    }
...
}
```
因为sparkSQL所支持的hiveql除了兼容hive语句外，还兼容一些sparkSQL本身的语句，所以在hiveql语句语法解析的时候：
1. 首先调用专门解析SparkSQL语法的解析器SparkSQLParser
2. 当SparksQLParser无法解析的时候，会调用ExtendedHiveQlParser进行解析

hiveContext的运行过程基本和sqlContext一致，除了override的catalog、functionRegistry、analyzer、planner、optimizedPlan、toRdd。
hiveContext的catalog，是指向 Hive Metastore。hiveContext的analyzer，使用了新的catalog和functionRegistry。hiveContext的planner，使用新定义的hivePlanner。
```
/* A catalyst metadata catalog that points to the Hive Metastore. */
  @transient
  override protected[sql] lazy val catalog = new HiveMetastoreCatalog(this) with OverrideCatalog

  // Note that HiveUDFs will be overridden by functions registered in this context.
  @transient
  override protected[sql] lazy val functionRegistry =
    new HiveFunctionRegistry with OverrideFunctionRegistry

  /* An analyzer that uses the Hive metastore. */
  @transient
  override protected[sql] lazy val analyzer =
    new Analyzer(catalog, functionRegistry, caseSensitive = false) {
      override val extendedRules =
        catalog.CreateTables ::
        catalog.PreInsertionCasts ::
        ExtractPythonUdfs ::
        Nil
    }

    @transient
  override protected[sql] val planner = hivePlanner
```

Spark1.2.0中HiveContext的执行基本上和SqlContext保持了统一。
