# Physical Plan to RDD
一段sql真正的执行是当你调用它的action方法才会执行Spark Job，最后计算得到RDD。

```
lazy val toRdd: RDD[Row] = executedPlan.execute()
```

Spark Plan基本包含4种操作类型:
1. BasicOperator
2. Join
3. Aggregate
4. Sort

### BasicOperator

##### Project
Project 的大致含义是：传入一系列表达式Seq[NamedExpression]，给定输入的Row，经过Convert（Expression的计算eval）操作，生成一个新的Row。Project的实现是调用其child.execute()方法，然后调用mapPartitions对每一个Partition进行操作。这个f函数其实是new了一个MutableProjection，然后循环的对每个partition进行Convert。

```
case class Project(projectList: Seq[NamedExpression], child: SparkPlan) extends UnaryNode {
  override def output = projectList.map(_.toAttribute)

  @transient lazy val buildProjection = newMutableProjection(projectList, child.output)

  def execute() = child.execute().mapPartitions { iter =>
    val resuableProjection = buildProjection()
    iter.map(resuableProjection)
  }
}
```

根据是否开启code generation，newMutableProjection会生成两种不同的策略：GenerateMutableProjection和InterpretedMutableProjection。
```
  protected def newMutableProjection(
      expressions: Seq[Expression],
      inputSchema: Seq[Attribute]): () => MutableProjection = {
    log.debug(
      s"Creating MutableProj: $expressions, inputSchema: $inputSchema, codegen:$codegenEnabled")
    if(codegenEnabled) {
      GenerateMutableProjection(expressions, inputSchema)
    } else {
      () => new InterpretedMutableProjection(expressions, inputSchema)
    }
  }
```

GenerateMutableProjection通过自动生成代码的方式计算project。
```
/**
 * Generates byte code that produces a [[MutableRow]] object that can update itself based on a new
 * input [[Row]] for a fixed set of [[Expression Expressions]].
 */
object GenerateMutableProjection extends CodeGenerator[Seq[Expression], () => MutableProjection] {
  import scala.reflect.runtime.{universe => ru}
  import scala.reflect.runtime.universe._

  val mutableRowName = newTermName("mutableRow")

  protected def canonicalize(in: Seq[Expression]): Seq[Expression] =
    in.map(ExpressionCanonicalizer(_))

  protected def bind(in: Seq[Expression], inputSchema: Seq[Attribute]): Seq[Expression] =
    in.map(BindReferences.bindReference(_, inputSchema))

  protected def create(expressions: Seq[Expression]): (() => MutableProjection) = {
    val projectionCode = expressions.zipWithIndex.flatMap { case (e, i) =>
      val evaluationCode = expressionEvaluator(e)

      evaluationCode.code :+
      q"""
        if(${evaluationCode.nullTerm})
          mutableRow.setNullAt($i)
        else
          ${setColumn(mutableRowName, e.dataType, i, evaluationCode.primitiveTerm)}
      """
    }

    val code =
      q"""
        () => { new $mutableProjectionType {

          private[this] var $mutableRowName: $mutableRowType =
            new $genericMutableRowType(${expressions.size})

          def target(row: $mutableRowType): $mutableProjectionType = {
            $mutableRowName = row
            this
          }

          /* Provide immutable access to the last projected row. */
          def currentValue: $rowType = mutableRow

          def apply(i: $rowType): $rowType = {
            ..$projectionCode
            mutableRow
          }
        } }
      """

    log.debug(s"code for ${expressions.mkString(",")}:\n$code")
    toolBox.eval(code).asInstanceOf[() => MutableProjection]
  }
}
```

InterpretedMutableProjection继承自MutableProjection，MutableProjection就是bind references to a schema 和eval的过程：将一个Row转换为另一个已经定义好schema column的Row。如果输入的Row已经有Schema了，则传入的Seq[Expression]也会bound到当前的Schema。
```
/**
 * A [[MutableProjection]] that is calculated by calling `eval` on each of the specified
 * expressions.
 * @param expressions a sequence of expressions that determine the value of each column of the
 *                    output row.
 */
case class InterpretedMutableProjection(expressions: Seq[Expression]) extends MutableProjection {
  def this(expressions: Seq[Expression], inputSchema: Seq[Attribute]) =
    this(expressions.map(BindReferences.bindReference(_, inputSchema)))

  private[this] val exprArray = expressions.toArray
  private[this] var mutableRow: MutableRow = new GenericMutableRow(exprArray.size)
  def currentValue: Row = mutableRow

  override def target(row: MutableRow): MutableProjection = {
    mutableRow = row
    this
  }

  override def apply(input: Row): Row = {
    var i = 0
    while (i < exprArray.length) {
      mutableRow(i) = exprArray(i).eval(input)
      i += 1
    }
    mutableRow
  }
}
```

##### Filter
Filter的具体实现是传入的condition进行对input row的eval计算，最后返回的是一个Boolean类型，
 如果表达式计算成功，返回true，则这个分区的这条数据就会保存下来，否则会过滤掉。
```
case class Filter(condition: Expression, child: SparkPlan) extends UnaryNode {
  override def output = child.output

  @transient lazy val conditionEvaluator = newPredicate(condition, child.output)

  def execute() = child.execute().mapPartitions { iter =>
    iter.filter(conditionEvaluator)
  }
}
```

##### Sample
Sample取样操作其实是调用了child.execute()的结果后，返回的是一个RDD，对这个RDD调用其sample函数，原生方法。
```
case class Sample(fraction: Double, withReplacement: Boolean, seed: Long, child: SparkPlan)
  extends UnaryNode
{
  override def output = child.output

  // TODO: How to pick seed?
  override def execute() = child.execute().sample(withReplacement, fraction, seed)
}
```

##### Union
Union操作支持多个子查询的Union，所以传入的child是一个Seq[SparkPlan]。execute()方法的实现是对其所有的children，每一个进行execute()，即select查询的结果集合RDD。通过调用SparkContext的union方法，将所有子查询的结果合并起来。
```
case class Union(children: Seq[SparkPlan]) extends SparkPlan {
  // TODO: attributes output by union should be distinct for nullability purposes
  override def output = children.head.output
  override def execute() = sparkContext.union(children.map(_.execute()))
}
```

##### Limit
Limit的实现分2种情况：
1. limit作为结尾的操作符，即select xxx from yyy limit zzz。 并且是被executeCollect调用，则直接在driver里使用take方法。
2. limit不是作为结尾的操作符，即limit后面还有查询，那么就在每个分区调用limit，最后repartition到一个分区来计算global limit。

```
case class Limit(limit: Int, child: SparkPlan)
  extends UnaryNode {
  /** We must copy rows when sort based shuffle is on */
  private def sortBasedShuffleOn = SparkEnv.get.shuffleManager.isInstanceOf[SortShuffleManager]

  override def output = child.output
  override def outputPartitioning = SinglePartition
  ...
  override def execute() = {
    val rdd: RDD[_ <: Product2[Boolean, Row]] = if (sortBasedShuffleOn) {
      child.execute().mapPartitions { iter =>
        iter.take(limit).map(row => (false, row.copy()))
      }
    } else {
      child.execute().mapPartitions { iter =>
        val mutablePair = new MutablePair[Boolean, Row]()
        iter.take(limit).map(row => mutablePair.update(false, row))
      }
    }
    val part = new HashPartitioner(1)
    val shuffled = new ShuffledRDD[Boolean, Row, Row](rdd, part)
    shuffled.setSerializer(new SparkSqlSerializer(new SparkConf(false)))
    shuffled.mapPartitions(_.take(limit).map(_._2))
  }
}
```

##### TakeOrdered
TakeOrdered是经过排序后的limit N，一般是用在sort by 操作符后的limit。可以简单理解为TopN操作符。

```
case class TakeOrdered(limit: Int, sortOrder: Seq[SortOrder], child: SparkPlan) extends UnaryNode {

  override def output = child.output
  override def outputPartitioning = SinglePartition

  val ord = new RowOrdering(sortOrder, child.output)

  // TODO: Is this copying for no reason?
  override def executeCollect() = child.execute().map(_.copy()).takeOrdered(limit)(ord)
    .map(ScalaReflection.convertRowToScala(_, this.schema))

  // TODO: Terminal split should be implemented differently from non-terminal split.
  // TODO: Pick num splits based on |limit|.
  override def execute() = sparkContext.makeRDD(executeCollect(), 1)
}
```
##### Sort
Sort也是通过RowOrdering这个类来实现排序的，child.execute()对每个分区进行map，每个分区根据RowOrdering的order来进行排序，生成一个新的有序集合。也是通过调用Spark RDD的sorted方法来实现的。

```
case class Sort(
    sortOrder: Seq[SortOrder],
    global: Boolean,
    child: SparkPlan)
  extends UnaryNode {
  override def requiredChildDistribution =
    if (global) OrderedDistribution(sortOrder) :: Nil else UnspecifiedDistribution :: Nil

  override def execute() = attachTree(this, "sort") {
    child.execute().mapPartitions( { iterator =>
      val ordering = newOrdering(sortOrder, child.output)
      iterator.map(_.copy()).toArray.sorted(ordering).iterator
    }, preservesPartitioning = true)
  }

  override def output = child.output
}
```

##### ExternalSort
外部排序：内存不够的情况下，会把数据spill到磁盘。

```
case class ExternalSort(
    sortOrder: Seq[SortOrder],
    global: Boolean,
    child: SparkPlan)
  extends UnaryNode {
  override def requiredChildDistribution =
    if (global) OrderedDistribution(sortOrder) :: Nil else UnspecifiedDistribution :: Nil

  override def execute() = attachTree(this, "sort") {
    child.execute().mapPartitions( { iterator =>
      val ordering = newOrdering(sortOrder, child.output)
      val sorter = new ExternalSorter[Row, Null, Row](ordering = Some(ordering))
      sorter.insertAll(iterator.map(r => (r, null)))
      sorter.iterator.map(_._1)
    }, preservesPartitioning = true)
  }

  override def output = child.output
}
```

##### Distinct

```
case class Distinct(partial: Boolean, child: SparkPlan) extends UnaryNode {
  override def output = child.output

  override def requiredChildDistribution =
    if (partial) UnspecifiedDistribution :: Nil else ClusteredDistribution(child.output) :: Nil

  override def execute() = {
    child.execute().mapPartitions { iter =>
      val hashSet = new scala.collection.mutable.HashSet[Row]()

      var currentRow: Row = null
      while (iter.hasNext) {
        currentRow = iter.next()
        if (!hashSet.contains(currentRow)) {
          hashSet.add(currentRow.copy())
        }
      }

      hashSet.iterator
    }
  }
}
```
##### Except
Collection减法操作。

```
case class Except(left: SparkPlan, right: SparkPlan) extends BinaryNode {
  override def output = left.output

  override def execute() = {
    left.execute().map(_.copy()).subtract(right.execute().map(_.copy()))
  }
}
```

##### Intersect
Collection交集操作。

```
case class Intersect(left: SparkPlan, right: SparkPlan) extends BinaryNode {
  override def output = children.head.output

  override def execute() = {
    left.execute().map(_.copy()).intersection(right.execute().map(_.copy()))
  }
}
```

### Join Related Operators

##### HashJoin
在讲解Join Related Operator之前，有必要了解一下HashJoin这个位于execution包下的joins.scala文件里的trait。Join操作主要包含BroadcastHashJoin、LeftSemiJoinHash、ShuffledHashJoin均实现了HashJoin这个trait。

HashJoin这个trait的主要成员有：
* buildSide是左连接还是右连接，有一种基准的意思。
* leftKeys是左孩子的expressions, rightKeys是右孩子的expressions。
* left是左孩子物理计划，right是右孩子物理计划。
* buildSideKeyGenerator是一个Projection是根据传入的Row对象来计算buildSide的Expression的。
* streamSideKeyGenerator是一个MutableProjection是根据传入的Row对象来计算streamSide的Expression的。
* 这里buildSide如果是left的话，可以理解为buildSide是左表，那么去连接这个左表的右表就是streamSide。

HashJoin关键的操作是joinIterators，简单来说就是join两个表，把每个表看着Iterators[Row].
方式：
1. 首先遍历buildSide，计算buildKeys然后利用一个HashMap，形成 (buildKeys, Iterators[Row])的格式。
2. 遍历StreamedSide，计算streamedKey，去HashMap里面去匹配key，来进行join
3. 最后生成一个joinRow，这个将2个row对接。

```
trait HashJoin {
  self: SparkPlan =>

  val leftKeys: Seq[Expression]
  val rightKeys: Seq[Expression]
  val buildSide: BuildSide
  val left: SparkPlan
  val right: SparkPlan

  protected lazy val (buildPlan, streamedPlan) = buildSide match {
    case BuildLeft => (left, right)
    case BuildRight => (right, left)
  }

  protected lazy val (buildKeys, streamedKeys) = buildSide match {
    case BuildLeft => (leftKeys, rightKeys)
    case BuildRight => (rightKeys, leftKeys)
  }

  override def output = left.output ++ right.output

  @transient protected lazy val buildSideKeyGenerator: Projection =
    newProjection(buildKeys, buildPlan.output)

  @transient protected lazy val streamSideKeyGenerator: () => MutableProjection =
    newMutableProjection(streamedKeys, streamedPlan.output)

  protected def hashJoin(streamIter: Iterator[Row], hashedRelation: HashedRelation): Iterator[Row] =
  {
    new Iterator[Row] {
      private[this] var currentStreamedRow: Row = _
      private[this] var currentHashMatches: CompactBuffer[Row] = _
      private[this] var currentMatchPosition: Int = -1

      // Mutable per row objects.
      private[this] val joinRow = new JoinedRow2

      private[this] val joinKeys = streamSideKeyGenerator()

      override final def hasNext: Boolean =
        (currentMatchPosition != -1 && currentMatchPosition < currentHashMatches.size) ||
          (streamIter.hasNext && fetchNext())

      override final def next() = {
        val ret = buildSide match {
          case BuildRight => joinRow(currentStreamedRow, currentHashMatches(currentMatchPosition))
          case BuildLeft => joinRow(currentHashMatches(currentMatchPosition), currentStreamedRow)
        }
        currentMatchPosition += 1
        ret
      }

      /**
       * Searches the streamed iterator for the next row that has at least one match in hashtable.
       *
       * @return true if the search is successful, and false if the streamed iterator runs out of
       *         tuples.
       */
      private final def fetchNext(): Boolean = {
        currentHashMatches = null
        currentMatchPosition = -1

        while (currentHashMatches == null && streamIter.hasNext) {
          currentStreamedRow = streamIter.next()
          if (!joinKeys(currentStreamedRow).anyNull) {
            currentHashMatches = hashedRelation.get(joinKeys.currentValue)
          }
        }

        if (currentHashMatches == null) {
          false
        } else {
          currentMatchPosition = 0
          true
        }
      }
    }
  }
}
```

##### LeftSemiJoinHash
将右表的join keys放到HashSet里，然后遍历左表，查找左表的join key是否能匹配。

```
case class LeftSemiJoinHash(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    left: SparkPlan,
    right: SparkPlan) extends BinaryNode with HashJoin {

  override val buildSide = BuildRight

  override def requiredChildDistribution =
    ClusteredDistribution(leftKeys) :: ClusteredDistribution(rightKeys) :: Nil

  override def output = left.output

  override def execute() = {
    buildPlan.execute().zipPartitions(streamedPlan.execute()) { (buildIter, streamIter) =>
      val hashSet = new java.util.HashSet[Row]()
      var currentRow: Row = null

      // Create a Hash set of buildKeys
      while (buildIter.hasNext) {
        currentRow = buildIter.next()
        val rowKey = buildSideKeyGenerator(currentRow)
        if (!rowKey.anyNull) {
          val keyExists = hashSet.contains(rowKey)
          if (!keyExists) {
            hashSet.add(rowKey)
          }
        }
      }

      val joinKeys = streamSideKeyGenerator()
      streamIter.filter(current => {
        !joinKeys(current).anyNull && hashSet.contains(joinKeys.currentValue)
      })
    }
  }
}
```

##### BroadcastHashJoin
是InnerHashJoin的实现。这里用到了concurrent并发里的future，异步的广播buildPlan的表执行后的的RDD。如果接收到了广播后的表，那么就用streamedPlan来匹配这个广播的表。实现是RDD的mapPartitions和HashJoin里的joinIterators最后生成join的结果。

```
case class BroadcastHashJoin(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    buildSide: BuildSide,
    left: SparkPlan,
    right: SparkPlan)
  extends BinaryNode with HashJoin {

  override def outputPartitioning: Partitioning = streamedPlan.outputPartitioning

  override def requiredChildDistribution =
    UnspecifiedDistribution :: UnspecifiedDistribution :: Nil

  @transient
  private val broadcastFuture = future {
    // Note that we use .execute().collect() because we don't want to convert data to Scala types
    val input: Array[Row] = buildPlan.execute().map(_.copy()).collect()
    val hashed = HashedRelation(input.iterator, buildSideKeyGenerator, input.length)
    sparkContext.broadcast(hashed)
  }

  override def execute() = {
    val broadcastRelation = Await.result(broadcastFuture, 5.minute)

    streamedPlan.execute().mapPartitions { streamedIter =>
      hashJoin(streamedIter, broadcastRelation.value)
    }
  }
}
```

##### ShuffledHashJoin
ShuffleHashJoin顾名思义就是需要shuffle数据，outputPartitioning是左孩子的的Partitioning。
会根据这个Partitioning进行shuffle。然后利用SparkContext里的zipPartitions方法对每个分区进行zip。这里的requiredChildDistribution，的是ClusteredDistribution，这个会在HashPartitioning里面进行匹配。

```
case class ShuffledHashJoin(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    buildSide: BuildSide,
    left: SparkPlan,
    right: SparkPlan)
  extends BinaryNode with HashJoin {

  override def outputPartitioning: Partitioning = left.outputPartitioning

  override def requiredChildDistribution =
    ClusteredDistribution(leftKeys) :: ClusteredDistribution(rightKeys) :: Nil

  override def execute() = {
    buildPlan.execute().zipPartitions(streamedPlan.execute()) { (buildIter, streamIter) =>
      val hashed = HashedRelation(buildIter, buildSideKeyGenerator)
      hashJoin(streamIter, hashed)
    }
  }
}
```
