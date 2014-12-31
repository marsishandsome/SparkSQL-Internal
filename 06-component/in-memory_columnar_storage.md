# In-Memory Columnar Storage

Spark SQL可以将数据缓存到内存中，我们可以见到的通过调用cache table tableName即可将一张表缓存到内存中，来极大的提高查询效率。这就涉及到内存中的数据的存储形式，我们知道基于关系型的数据可以存储为基于行存储结构或者基于列存储结构，或者基于行和列的混合存储，即Row Based Storage、Column Based Storage、 PAX Storage。

Spark SQL 将数据加载到内存是以列的存储结构。称为In-Memory Columnar Storage。若直接存储Java Object会产生很大的内存开销，并且这样是基于Row的存储结构。查询某些列速度略慢，虽然数据以及载入内存，查询效率还是低于面向列的存储结构。

Spark SQL的In-Memory Columnar Storage是位于spark列下面org.apache.spark.sql.columnar包内。核心的类有 ColumnBuilder,  InMemoryColumnarTableScan, ColumnAccessor, ColumnType。如果列有压缩的情况：compression包下面有具体的build列和access列的类。

当我们调用```sql("cache table src") ```时，会产生一个catalyst.plans.logical.CacheTableCommand，是一个LogicalPlan。

```
case class CacheTableCommand(tableName: String, plan: Option[LogicalPlan], isLazy: Boolean)
  extends Command

abstract class Command extends LeafNode {
  self: Product =>
  def output: Seq[Attribute] = Seq.empty
}

abstract class LeafNode extends LogicalPlan with trees.LeafNode[LogicalPlan] {
  self: Product =>
}
```

在生成物理计划的时候，会转换成execution.CacheTableComman的Physical Plan。
```
case logical.CacheTableCommand(tableName, optPlan, isLazy) =>
        Seq(execution.CacheTableCommand(tableName, optPlan, isLazy))
```

```
case class CacheTableCommand(
    tableName: String,
    plan: Option[LogicalPlan],
    isLazy: Boolean)
  extends LeafNode with Command {

  override protected lazy val sideEffectResult = {
    import sqlContext._

    plan.foreach(_.registerTempTable(tableName))
    cacheTable(tableName)

    if (!isLazy) {
      // Performs eager caching
      table(tableName).count()
    }

    Seq.empty[Row]
  }

  override def output: Seq[Attribute] = Seq.empty
}
```

接着调用CacheManager的cacheTable函数，然后调cacheQuery函数。在cacheQuery里面生成了InMemoryRelation对象，就是列式存储的数据结构。
```
private[sql] trait CacheManager {
    ...
    def cacheTable(tableName: String): Unit = cacheQuery(table(tableName), Some(tableName))
    ...
     private[sql] def cacheQuery(
      query: SchemaRDD,
      tableName: Option[String] = None,
      storageLevel: StorageLevel = MEMORY_AND_DISK): Unit = writeLock {
    val planToCache = query.queryExecution.analyzed
    if (lookupCachedData(planToCache).nonEmpty) {
      logWarning("Asked to cache already cached data.")
    } else {
      cachedData +=
        CachedData(
          planToCache,
          InMemoryRelation(
            useCompression,
            columnBatchSize,
            storageLevel,
            query.queryExecution.executedPlan,
            tableName))
    }
  }
  ...
}
```

### 用法
SparkSQL中有三种方法触发cache：
1. sqlContext.sql("cache table tableName")
2. sqlContext.cacheTable("tableName")
3. schemaRDD.cache()

以上三种用法都会使用到列存储的方式对rdd进行缓存。如果调用了普通rdd的cache方法，是不会触发列式存储的cache。

在Spark1.2.0中，cache table的执行时eager模式的，如果想触发lazy模式，可以主动添加lazy关键字，例如```cache lazy table tabelName```。

而在Spark1.2.0之前，cache table的默认语义是lazy的，所以需要主动触发action才会真正执行cache操作。

### InMemoryRelation
 InMemoryRelation继承自LogicalPlan。_cachedColumnBuffers这个类型为RDD[Array[ByteBuffer]]的私有字段。这个封装就是面向列的存储ByteBuffer。相较于plain java object存储记录，用ByteBuffer能显著的提高存储效率，减少内存占用。并且按列查询的速度会非常快。

构造一个InMemoryRelation需要该Relation
1. output Attribute
2. 是否需要useCoompression来压缩，默认为false
3. 一次处理的多少行数据batchSize
4. child 即SparkPlan
5. table名
6. _cachedColumnBuffers最终将table放入内存的存储句柄，是一个RDD[Array[ByteBuffer]
7. _statistics是统计信息

```
private[sql] case class InMemoryRelation(
    output: Seq[Attribute],
    useCompression: Boolean,
    batchSize: Int,
    storageLevel: StorageLevel,
    child: SparkPlan,
    tableName: Option[String])(
    private var _cachedColumnBuffers: RDD[CachedBatch] = null,
    private var _statistics: Statistics = null)
  extends LogicalPlan with MultiInstanceRelation
```

可以通过设置：
spark.sql.inMemoryColumnarStorage.compressed为true来设置内存中的列存储是否需要压缩。
spark.sql.inMemoryColumnarStorage.batchSize来设置一次处理多少row
spark.sql.defaultSizeInBytes来设置初始化的column的bufferbytes的默认大小，这里只是其中一个参数。


缓存主流程：
1. 判断_cachedColumnBuffers是否为null，如果不是null，则已经Cache了当前table，重复cache不会触发cache操作，如果是null，则调用buildBuffers。
2. child是SparkPlan，即执行hive table scan，测试我拿sbt/sbt hive/console里test里的src table为例，操作是扫描这张表。这个表有2个字的key是int, value 是string
3. 拿到child的output, 这里的output就是 key, value2个列。
4. 执行mapPartitions操作，对当前RDD的每个分区的数据进行操作。
5. 对于每一个分区，迭代里面的数据生成新的Iterator。每个Iterator里面是Array[ByteBuffer]
6. 对于child.output的每一列，都会生成一个ColumnBuilder，最后组合为一个columnBuilders是一个数组。
7. 数组内每个CommandBuilder持有一个ByteBuffer
8. 遍历原始分区的记录，将对于的行转为列，并将数据存到ByteBuffer内。
9. 最后将此RDD调用cache方法，将RDD缓存。
10. 将cached赋给_cachedColumnBuffers。

```
if (_cachedColumnBuffers == null) {
    buildBuffers()
  }

private def buildBuffers(): Unit = {
    val output = child.output
    val cached = child.execute().mapPartitions { rowIterator =>
      new Iterator[CachedBatch] {
        def next() = {
          val columnBuilders = output.map { attribute =>
            val columnType = ColumnType(attribute.dataType)
            val initialBufferSize = columnType.defaultSize * batchSize
            ColumnBuilder(columnType.typeId, initialBufferSize, attribute.name, useCompression)
          }.toArray

          var rowCount = 0
          while (rowIterator.hasNext && rowCount < batchSize) {
            val row = rowIterator.next()
            var i = 0
            while (i < row.length) {
              columnBuilders(i).appendFrom(row, i)
              i += 1
            }
            rowCount += 1
          }

          val stats = Row.fromSeq(
            columnBuilders.map(_.columnStats.collectedStatistics).foldLeft(Seq.empty[Any])(_ ++ _))

          batchStats += stats
          CachedBatch(columnBuilders.map(_.build().array()), stats)
        }

        def hasNext = rowIterator.hasNext
      }
    }.persist(storageLevel)

    cached.setName(tableName.map(n => s"In-memory table $n").getOrElse(child.toString))
    _cachedColumnBuffers = cached
  }
```

### Columnar Storage



