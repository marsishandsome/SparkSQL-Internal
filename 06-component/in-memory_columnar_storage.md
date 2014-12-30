# In-Memory Columnar Storage

Spark SQL可以将数据缓存到内存中，我们可以见到的通过调用cache table tableName即可将一张表缓存到内存中，来极大的提高查询效率。这就涉及到内存中的数据的存储形式，我们知道基于关系型的数据可以存储为基于行存储结构或者基于列存储结构，或者基于行和列的混合存储，即Row Based Storage、Column Based Storage、 PAX Storage。

Spark SQL 将数据加载到内存是以列的存储结构。称为In-Memory Columnar Storage。若直接存储Java Object会产生很大的内存开销，并且这样是基于Row的存储结构。查询某些列速度略慢，虽然数据以及载入内存，查询效率还是低于面向列的存储结构。

Spark SQL的In-Memory Columnar Storage是位于spark列下面org.apache.spark.sql.columnar包内。核心的类有 ColumnBuilder,  InMemoryColumnarTableScan, ColumnAccessor, ColumnType。如果列有压缩的情况：compression包下面有具体的build列和access列的类。

当我们调用spark sql 里的```sql("cache table src") ```时，会产生一个CacheTableCommand，CacheTableCommand是一个


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
