# External Data Source

### 外部数据源的注册流程

##### DDLParser
```Create Temmporary Table Using```语法的解析由DDLParser负责，最后生成一个CreateTableUsing的类。

```
protected lazy val createTable: Parser[LogicalPlan] =
    CREATE ~ TEMPORARY ~ TABLE ~> ident ~ (USING ~> className) ~ (OPTIONS ~> options) ^^ {
      case tableName ~ provider ~ opts =>
        CreateTableUsing(tableName, provider, opts)
    }
```

##### CreateTableUsing (Logical Plan)
CreateTableUsing继承自RunnableCommand，是一个Logical Plan。

CreateTableUsing类接受三个参数：
1. tableName：表名
2. provider：解析具体文件数据的类或包
3. options：解析时需要的其他参数

```
private[sql] case class CreateTableUsing(
    tableName: String,
    provider: String,
    options: Map[String, String]) extends RunnableCommand {

  def run(sqlContext: SQLContext) = {
    val loader = Utils.getContextOrSparkClassLoader
    val clazz: Class[_] = try loader.loadClass(provider) catch {
      case cnf: java.lang.ClassNotFoundException =>
        try loader.loadClass(provider + ".DefaultSource") catch {
          case cnf: java.lang.ClassNotFoundException =>
            sys.error(s"Failed to load class for data source: $provider")
        }
    }
    val dataSource = clazz.newInstance().asInstanceOf[org.apache.spark.sql.sources.RelationProvider]
    val relation = dataSource.createRelation(sqlContext, options)

    sqlContext.baseRelationToSchemaRDD(relation).registerTempTable(tableName)
    Seq.empty
  }
}
```
CreateTableUsing的run方法定义了这个Logical Plan需要做的事情：
1. 加载provider类
2. 如果1失败，加载provider包中的DefaultSource类
3. 新建加载进来的类的对象，类型转换为org.apache.spark.sql.sources.RelationProvider
4. 通过RelationProvider的createRelation方法创建BaseRelation对象
5. 通过SqlContext的baseRelationToSchemaRDD方法创建并注册SchemaRDD

baseRelationToSchemaRDD方法
1. 首先把baseRelation包装成LogicalRelation(baseRelation)
2. 然后调用logicalPlanToSparkQuery
3. 最后被包装成 SchemaRDD(SqlContext, LogicalPlan(baseRelation))
```
implicit def baseRelationToSchemaRDD(baseRelation: BaseRelation): SchemaRDD = {
    logicalPlanToSparkQuery(LogicalRelation(baseRelation))
  }

implicit def logicalPlanToSparkQuery(plan: LogicalPlan): SchemaRDD = new SchemaRDD(this, plan)
```

##### ExecutedCommand (Physical Plan)
CreateTableUsing这个Logical Plan最后会被转换为ExecutedCommand的Physcial Plan。执行ExecutedCommand的时候，会调用CreateTableUsing的run函数，从而触发resolver类的加载以及SchemaRDD的注册。
```
case class ExecutedCommand(cmd: RunnableCommand) extends SparkPlan {
 protected[sql] lazy val sideEffectResult: Seq[Row] = cmd.run(sqlContext)

  override def output = cmd.output

  override def children = Nil

  override def executeCollect(): Array[Row] = sideEffectResult.toArray

  override def execute(): RDD[Row] = sqlContext.sparkContext.parallelize(sideEffectResult, 1)
}
```

### 外部数据源的核心类
下面介绍一下外部数据源的两个核心类RelationProvider和BaseRelation。

##### RelationProvider
RelationProvider是外部数据源的入口，如果想让SparkSQL读取某种格式的外部数据，必须继承该类。RelationProvider只有一个方法createRelation，接收SqlContext和一堆参数，返回BaseRelation。
```
trait RelationProvider {
  /** Returns a new base relation with the given parameters. */
  def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation
}
```

来看一下SparkSQL自带的读取JSON格式数据的DefaultSource：
1. 该类继承自RelationProvider
2. createRelation方法首先读取参数path（数据路径）和samplingRate（取样比例）
3. 然后创建JSONRelation

```
private[sql] class DefaultSource extends RelationProvider {
  /** Returns a new base relation with the given parameters. */
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    val fileName = parameters.getOrElse("path", sys.error("Option 'path' not specified"))
    val samplingRatio = parameters.get("samplingRatio").map(_.toDouble).getOrElse(1.0)

    JSONRelation(fileName, samplingRatio)(sqlContext)
  }
}
```

##### BaseRelation
SparkSQL用BaseRelation类来把一个数据文件抽象成一张表格，在BaseRelation中定义了Schema。
```
abstract class BaseRelation {
  def sqlContext: SQLContext
  def schema: StructType
  def sizeInBytes = sqlContext.defaultSizeInBytes
}
```

BaseRelation有四个子类，如果想让SparkSQL读取某种格式的外部数据，必须继承下面四个类中的其中一个。 四个子类都提供了一个buildScan的方法，返回值都一样是RDD[Row]，而参数各不一样。
* TableScan是最基本的BaseRelation，不接收任何参数
* PrunedScan只读取某些特定列，接收Stirng数组作为参数
* PrunedFilteredScan在PrunedScan的基础上还增加行过滤，额外接收Filter数组
* CatalystScan类似于PrunedFilteredScan，不同点是filters的类型为Expression

```
abstract class TableScan extends BaseRelation {
  def buildScan(): RDD[Row]
}

abstract class PrunedScan extends BaseRelation {
  def buildScan(requiredColumns: Array[String]): RDD[Row]
}

abstract class PrunedFilteredScan extends BaseRelation {
  def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row]
}

abstract class CatalystScan extends BaseRelation {
  def buildScan(requiredColumns: Seq[Attribute], filters: Seq[Expression]): RDD[Row]
}
```

看一下SparkSQL为读取json格式的数据实现的JSONRelation:
1. JSONRelation继承自TableScan，说明不做任何过滤
2. 首先通过SparkContext的textFile生成baseRDD
3. 然后调用JsonRDD的inferSchema，从原始数据中分析出Schema信息
4. 最后调用JsonRDD的jsonStringToRow，把Json格式的数据转成SchemaRDD格式

```
private[sql] case class JSONRelation(fileName: String, samplingRatio: Double)(
    @transient val sqlContext: SQLContext)
  extends TableScan {

  private def baseRDD = sqlContext.sparkContext.textFile(fileName)

  override val schema =
    JsonRDD.inferSchema(
      baseRDD,
      samplingRatio,
      sqlContext.columnNameOfCorruptRecord)

  override def buildScan() =
    JsonRDD.jsonStringToRow(baseRDD, schema, sqlContext.columnNameOfCorruptRecord)
}
```

### 外部数据源Table查询的计划解析流程

##### DataSourcesStrategy (Logical Plan -> Physical Plan)
DataSourcesStrategy是Logical Plan转换成Physical Plan中
```
private[sql] object DataSourceStrategy extends Strategy {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case PhysicalOperation(projectList, filters, l @ LogicalRelation(t: CatalystScan)) =>
      pruneFilterProjectRaw(
        l,
        projectList,
        filters,
        (a, f) => t.buildScan(a, f)) :: Nil

    case PhysicalOperation(projectList, filters, l @ LogicalRelation(t: PrunedFilteredScan)) =>
      pruneFilterProject(
        l,
        projectList,
        filters,
        (a, f) => t.buildScan(a, f)) :: Nil

    case PhysicalOperation(projectList, filters, l @ LogicalRelation(t: PrunedScan)) =>
      pruneFilterProject(
        l,
        projectList,
        filters,
        (a, _) => t.buildScan(a)) :: Nil

    case l @ LogicalRelation(t: TableScan) =>
      execution.PhysicalRDD(l.output, t.buildScan()) :: Nil

    case _ => Nil
  }
  ...
}
```

### 如何自定义一个外部数据源




### 参考
[Spark SQL之External DataSource外部数据源（一）示例](http://blog.csdn.net/oopsoom/article/details/42061077)

[Spark SQL之External DataSource外部数据源（二）源码分析](http://blog.csdn.net/oopsoom/article/details/42064075)
