# Physical Plan

物理计划是Spark SQL执行Spark job的前置，也是最后一道计划。

### SparkPlanner
Optimizer接受输入的Analyzed Logical Plan后，会有SparkPlanner来对Optimized Logical Plan进行转换，生成Physical plans。

```
    lazy val analyzed = ExtractPythonUdfs(analyzer(logical))
    lazy val withCachedData = useCachedData(analyzed)
    lazy val optimizedPlan = optimizer(withCachedData)
    // TODO: Don't just pick the first one...
    lazy val sparkPlan = {
      SparkPlan.currentContext.set(self)
      planner(optimizedPlan).next()
    }
```

SparkPlanner的apply方法，会返回一个Iterator[PhysicalPlan]。SparkPlanner继承了SparkStrategies，SparkStrategies继承了QueryPlanner。SparkStrategies包含了一系列特定的Strategies，这些Strategies是继承自QueryPlanner中定义的Strategy，它定义接受一个Logical Plan，生成一系列的Physical Plan

```
protected[sql] class SparkPlanner extends SparkStrategies {
    val sparkContext: SparkContext = self.sparkContext

    val sqlContext: SQLContext = self

    def codegenEnabled = self.codegenEnabled

    def numPartitions = self.numShufflePartitions

    val strategies: Seq[Strategy] =
      extraStrategies ++ (
      CommandStrategy(self) ::
      DataSourceStrategy ::
      TakeOrdered ::
      HashAggregation ::
      LeftSemiJoin ::
      HashJoin ::
      InMemoryScans ::
      ParquetOperations ::
      BasicOperators ::
      CartesianProduct ::
      BroadcastNestedLoopJoin :: Nil)
...
}
```

QueryPlanner 是SparkPlanner的基类，定义了一系列的关键点，如Strategy，planLater和apply。
```
abstract class QueryPlanner[PhysicalPlan <: TreeNode[PhysicalPlan]] {
  /** A list of execution strategies that can be used by the planner */
  def strategies: Seq[GenericStrategy[PhysicalPlan]]

  /**
   * Returns a placeholder for a physical plan that executes `plan`. This placeholder will be
   * filled in automatically by the QueryPlanner using the other execution strategies that are
   * available.
   */
  protected def planLater(plan: LogicalPlan) = apply(plan).next()

  def apply(plan: LogicalPlan): Iterator[PhysicalPlan] = {
    // Obviously a lot to do here still...
    val iter = strategies.view.flatMap(_(plan)).toIterator
    assert(iter.hasNext, s"No plan for $plan")
    iter
  }
}
```

### Spark Plan
Spark Plan是Catalyst里经过所有Strategies apply 的最终的物理执行计划的抽象类，它只是用来执行spark job的。

```
  lazy val executedPlan: SparkPlan = prepareForExecution(sparkPlan)
```

prepareForExecution其实是一个RuleExecutor[SparkPlan]，当然这里的Rule就是SparkPlan了。

```
  protected[sql] val prepareForExecution = new RuleExecutor[SparkPlan] {
    val batches =
      Batch("Add exchange", Once, AddExchange(self)) :: Nil
  }
```

Spark Plan继承Query Plan[Spark Plan]，里面定义的partition，requiredChildDistribution以及spark sql启动执行的execute方法。

```
abstract class SparkPlan extends QueryPlan[SparkPlan] with Logging with Serializable {
  self: Product =>

  /** Specifies how data is partitioned across different nodes in the cluster. */
  def outputPartitioning: Partitioning = UnknownPartitioning(0) // TODO: WRONG WIDTH!

  /** Specifies any partition requirements on the input data for this operator. */
  def requiredChildDistribution: Seq[Distribution] =
    Seq.fill(children.size)(UnspecifiedDistribution)

  /**
   * Runs this query returning the result as an RDD.
   */
  def execute(): RDD[Row]
  ...
}

```

### Strategies
下面来看一下在生成物理计划中使用到的十几种strategy。


##### CommandStrategy(self)
CommandStrategy是专门针对Command类型的Logical Plan，即set key = value 、 explain sql、 cache table xxx 这类操作
1. RunnableCommand可以执行继承自RunnableCommand的命令（输出是Seq[Row]），并将Seq[Row]转化为RDD。
2. SetCommand主要实现方式是SparkContext的参数
3. ExplainCommand主要实现方式是利用executed Plan打印出tree string
4. CacheTableCommand主要实现方式将RDD以列式方式缓存到内存中
5. UncacheTableCommand

```
case class CommandStrategy(context: SQLContext) extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case r: RunnableCommand => ExecutedCommand(r) :: Nil
      case logical.SetCommand(kv) =>
        Seq(execution.SetCommand(kv, plan.output)(context))
      case logical.ExplainCommand(logicalPlan, extended) =>
        Seq(execution.ExplainCommand(logicalPlan, plan.output, extended)(context))
      case logical.CacheTableCommand(tableName, optPlan, isLazy) =>
        Seq(execution.CacheTableCommand(tableName, optPlan, isLazy))
      case logical.UncacheTableCommand(tableName) =>
        Seq(execution.UncacheTableCommand(tableName))
      case _ => Nil
    }
  }
```

##### DataSourceStrategy
根据不同的BaseRelation生产不同的PhysicalRDD。支持4种BaseRelation:
1. TableScan  默认的Scan策略
2. PrunedScan 这里可以传入指定的列，requiredColumns，列裁剪，不需要的列不会从外部数据源加载。
3. PrunedFilterScan 在列裁剪的基础上，并且加入Filter机制，在加载数据也的时候就进行过滤，而不是在客户端请求返回时做Filter。
4. CatalystScan Catalyst的支持传入expressions来进行Scan。支持列裁剪和Filter。

```
/**
 * A Strategy for planning scans over data sources defined using the sources API.
 */
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

##### TakeOrdered
TakeOrdered是用于Limit操作的，如果有Limit和Sort操作。则返回一个TakeOrdered的Spark Plan。主要也是利用RDD的takeOrdered方法来实现的排序后取TopN。
```
 object TakeOrdered extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case logical.Limit(IntegerLiteral(limit), logical.Sort(order, child)) =>
        execution.TakeOrdered(limit, order, planLater(child)) :: Nil
      case _ => Nil
    }
  }
```

##### HashAggregation
聚合操作可以映射为RDD的shuffle操作。

```
object HashAggregation extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      // Aggregations that can be performed in two phases, before and after the shuffle.

      // Cases where all aggregates can be codegened.
      case PartialAggregation(
             namedGroupingAttributes,
             rewrittenAggregateExpressions,
             groupingExpressions,
             partialComputation,
             child)
             if canBeCodeGened(
                  allAggregates(partialComputation) ++
                  allAggregates(rewrittenAggregateExpressions)) &&
               codegenEnabled =>
          execution.GeneratedAggregate(
            partial = false,
            namedGroupingAttributes,
            rewrittenAggregateExpressions,
            execution.GeneratedAggregate(
              partial = true,
              groupingExpressions,
              partialComputation,
              planLater(child))) :: Nil

      // Cases where some aggregate can not be codegened
      case PartialAggregation(
             namedGroupingAttributes,
             rewrittenAggregateExpressions,
             groupingExpressions,
             partialComputation,
             child) =>
        execution.Aggregate(
          partial = false,
          namedGroupingAttributes,
          rewrittenAggregateExpressions,
          execution.Aggregate(
            partial = true,
            groupingExpressions,
            partialComputation,
            planLater(child))) :: Nil

      case _ => Nil
    }
    ...
  }
```

##### LeftSemiJoin
如果Logical Plan里的Join是joinType为LeftSemi的话，就会执行这种策略，这里ExtractEquiJoinKeys是一个pattern定义在patterns.scala里，主要是做模式匹配用的。这里匹配只要是等值的join操作，都会封装为ExtractEquiJoinKeys对象，它会解析当前join，最后返回(joinType, rightKeys, leftKeys, condition, leftChild, rightChild)的格式。最后返回一个execution.LeftSemiJoinHash这个Spark Plan。

```
object LeftSemiJoin extends Strategy with PredicateHelper {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      // Find left semi joins where at least some predicates can be evaluated by matching join keys
      case ExtractEquiJoinKeys(LeftSemi, leftKeys, rightKeys, condition, left, right) =>
        val semiJoin = joins.LeftSemiJoinHash(
          leftKeys, rightKeys, planLater(left), planLater(right))
        condition.map(Filter(_, semiJoin)).getOrElse(semiJoin) :: Nil
      // no predicate can be evaluated by matching hash keys
      case logical.Join(left, right, LeftSemi, condition) =>
        joins.LeftSemiJoinBNL(planLater(left), planLater(right), condition) :: Nil
      case _ => Nil
    }
  }
```

##### HashJoin
HashJoin是我们最见的操作，innerJoin类型，里面提供了2种Spark Plan，BroadcastHashJoin 和 ShuffledHashJoin
BroadcastHashJoin的实现是一种广播变量的实现方法，如果设置了spark.sql.join.broadcastTables这个参数的表（表面逗号隔开）就会用spark的Broadcast Variables方式先将一张表给查询出来，然后广播到各个机器中，相当于Hive中的map join。ShuffledHashJoin是一种最传统的默认的join方式，会根据shuffle key进行shuffle的hash join。

```
object HashJoin extends Strategy with PredicateHelper {

    private[this] def makeBroadcastHashJoin(
        leftKeys: Seq[Expression],
        rightKeys: Seq[Expression],
        left: LogicalPlan,
        right: LogicalPlan,
        condition: Option[Expression],
        side: joins.BuildSide) = {
      val broadcastHashJoin = execution.joins.BroadcastHashJoin(
        leftKeys, rightKeys, side, planLater(left), planLater(right))
      condition.map(Filter(_, broadcastHashJoin)).getOrElse(broadcastHashJoin) :: Nil
    }

    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case ExtractEquiJoinKeys(Inner, leftKeys, rightKeys, condition, left, right)
        if sqlContext.autoBroadcastJoinThreshold > 0 &&
           right.statistics.sizeInBytes <= sqlContext.autoBroadcastJoinThreshold =>
        makeBroadcastHashJoin(leftKeys, rightKeys, left, right, condition, joins.BuildRight)

      case ExtractEquiJoinKeys(Inner, leftKeys, rightKeys, condition, left, right)
        if sqlContext.autoBroadcastJoinThreshold > 0 &&
           left.statistics.sizeInBytes <= sqlContext.autoBroadcastJoinThreshold =>
          makeBroadcastHashJoin(leftKeys, rightKeys, left, right, condition, joins.BuildLeft)

      case ExtractEquiJoinKeys(Inner, leftKeys, rightKeys, condition, left, right) =>
        val buildSide =
          if (right.statistics.sizeInBytes <= left.statistics.sizeInBytes) {
            joins.BuildRight
          } else {
            joins.BuildLeft
          }
        val hashJoin = joins.ShuffledHashJoin(
          leftKeys, rightKeys, buildSide, planLater(left), planLater(right))
        condition.map(Filter(_, hashJoin)).getOrElse(hashJoin) :: Nil

      case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, condition, left, right) =>
        joins.HashOuterJoin(
          leftKeys, rightKeys, joinType, condition, planLater(left), planLater(right)) :: Nil

      case _ => Nil
    }
  }
```

##### InMemoryScans
InMemoryScans主要是对InMemoryRelation这个Logical Plan操作。调用的其实是Spark Planner里的pruneFilterProject这个方法。
```
object InMemoryScans extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case PhysicalOperation(projectList, filters, mem: InMemoryRelation) =>
        pruneFilterProject(
          projectList,
          filters,
          identity[Seq[Expression]], // All filters still need to be evaluated.
          InMemoryColumnarTableScan(_,  filters, mem)) :: Nil
      case _ => Nil
    }
  }
```

##### ParquetOperations
支持ParquetOperations的读写，插入Table等。

```
object ParquetOperations extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      // TODO: need to support writing to other types of files.  Unify the below code paths.
      case logical.WriteToFile(path, child) =>
        val relation =
          ParquetRelation.create(path, child, sparkContext.hadoopConfiguration, sqlContext)
        // Note: overwrite=false because otherwise the metadata we just created will be deleted
        InsertIntoParquetTable(relation, planLater(child), overwrite = false) :: Nil
      case logical.InsertIntoTable(table: ParquetRelation, partition, child, overwrite) =>
        InsertIntoParquetTable(table, planLater(child), overwrite) :: Nil
      case PhysicalOperation(projectList, filters: Seq[Expression], relation: ParquetRelation) =>
        val prunePushedDownFilters =
          if (sqlContext.parquetFilterPushDown) {
            (predicates: Seq[Expression]) => {
              // Note: filters cannot be pushed down to Parquet if they contain more complex
              // expressions than simple "Attribute cmp Literal" comparisons. Here we remove all
              // filters that have been pushed down. Note that a predicate such as "(A AND B) OR C"
              // can result in "A OR C" being pushed down. Here we are conservative in the sense
              // that even if "A" was pushed and we check for "A AND B" we still want to keep
              // "A AND B" in the higher-level filter, not just "B".
              predicates.map(p => p -> ParquetFilters.createFilter(p)).collect {
                case (predicate, None) => predicate
              }
            }
          } else {
            identity[Seq[Expression]] _
          }
        pruneFilterProject(
          projectList,
          filters,
          prunePushedDownFilters,
          ParquetTableScan(
            _,
            relation,
            if (sqlContext.parquetFilterPushDown) filters else Nil)) :: Nil

      case _ => Nil
    }
  }
```

#####  BasicOperators
所有定义在org.apache.spark.sql.execution里的基本的Spark Plan，它们都在org.apache.spark.sql.execution包下basicOperators.scala内。有Project、Filter、Sample、Union、Limit、TakeOrdered、Sort、ExistingRdd。这些是基本元素，实现都相对简单，基本上都是RDD里的方法来实现的。

```
// Can we automate these 'pass through' operations?
  object BasicOperators extends Strategy {
    def numPartitions = self.numPartitions

    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case logical.Distinct(child) =>
        execution.Distinct(partial = false,
          execution.Distinct(partial = true, planLater(child))) :: Nil

      case logical.Sort(sortExprs, child) if sqlContext.externalSortEnabled =>
        execution.ExternalSort(sortExprs, global = true, planLater(child)):: Nil
      case logical.Sort(sortExprs, child) =>
        execution.Sort(sortExprs, global = true, planLater(child)):: Nil

      case logical.SortPartitions(sortExprs, child) =>
        // This sort only sorts tuples within a partition. Its requiredDistribution will be
        // an UnspecifiedDistribution.
        execution.Sort(sortExprs, global = false, planLater(child)) :: Nil
      case logical.Project(projectList, child) =>
        execution.Project(projectList, planLater(child)) :: Nil
      case logical.Filter(condition, child) =>
        execution.Filter(condition, planLater(child)) :: Nil
      case logical.Aggregate(group, agg, child) =>
        execution.Aggregate(partial = false, group, agg, planLater(child)) :: Nil
      case logical.Sample(fraction, withReplacement, seed, child) =>
        execution.Sample(fraction, withReplacement, seed, planLater(child)) :: Nil
      case SparkLogicalPlan(alreadyPlanned) => alreadyPlanned :: Nil
      case logical.LocalRelation(output, data) =>
        val nPartitions = if (data.isEmpty) 1 else numPartitions
        PhysicalRDD(
          output,
          RDDConversions.productToRowRdd(sparkContext.parallelize(data, nPartitions),
            StructType.fromAttributes(output))) :: Nil
      case logical.Limit(IntegerLiteral(limit), child) =>
        execution.Limit(limit, planLater(child)) :: Nil
      case Unions(unionChildren) =>
        execution.Union(unionChildren.map(planLater)) :: Nil
      case logical.Except(left, right) =>
        execution.Except(planLater(left), planLater(right)) :: Nil
      case logical.Intersect(left, right) =>
        execution.Intersect(planLater(left), planLater(right)) :: Nil
      case logical.Generate(generator, join, outer, _, child) =>
        execution.Generate(generator, join = join, outer = outer, planLater(child)) :: Nil
      case logical.NoRelation =>
        execution.PhysicalRDD(Nil, singleRowRdd) :: Nil
      case logical.Repartition(expressions, child) =>
        execution.Exchange(HashPartitioning(expressions, numPartitions), planLater(child)) :: Nil
      case e @ EvaluatePython(udf, child, _) =>
        BatchPythonEvaluation(udf, e.output, planLater(child)) :: Nil
      case LogicalRDD(output, rdd) => PhysicalRDD(output, rdd) :: Nil
      case _ => Nil
    }
  }
```

##### CartesianProduct
笛卡尔积的Join，有待过滤条件的Join。主要是利用RDD的cartesian实现的。

```
object CartesianProduct extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case logical.Join(left, right, _, None) =>
        execution.joins.CartesianProduct(planLater(left), planLater(right)) :: Nil
      case logical.Join(left, right, Inner, Some(condition)) =>
        execution.Filter(condition,
          execution.joins.CartesianProduct(planLater(left), planLater(right))) :: Nil
      case _ => Nil
    }
  }
```

##### BroadcastNestedLoopJoin
BroadcastNestedLoopJoin是用于Left Outer Join， RightOuter， FullOuter这三种类型的join。而上述的Hash Join仅仅用于InnerJoin，这点要区分开来。

```
object BroadcastNestedLoopJoin extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case logical.Join(left, right, joinType, condition) =>
        val buildSide =
          if (right.statistics.sizeInBytes <= left.statistics.sizeInBytes) {
            joins.BuildRight
          } else {
            joins.BuildLeft
          }
        joins.BroadcastNestedLoopJoin(
          planLater(left), planLater(right), buildSide, joinType, condition) :: Nil
      case _ => Nil
    }
  }
```




