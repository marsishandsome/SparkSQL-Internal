# Tree和Rule
SparkSQL首先会对SQL语句进行语法解析(Parser)，形成一颗Tree，后续的操作（包括绑定，优化）都是基于Tree的操作，方法都是通过Rule进行模式匹配，对不同的模式进行不同的操作。

### Tree
Tree的相关代码定义在sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/trees。TreeNode的定义为```abstract class TreeNode[BaseType <: TreeNode[BaseType]]```

Logical Plans、Expressions、Physical Operators都可以使用Tree表示

##### Tree主要有两类操作
1. 集合相关操作，如foreach, map, flatMap, collect
2. Tree相关操作，如transformDown,transformUp将Rule应用到给定的数段，然后用结果替代就的数段；再如transformChildrenDown,transformChildrenUp对一个给定的节点进行操作，通过迭代将Rule应用到该节点以及子节点。

```
   /**
   * Returns a copy of this node where `rule` has been recursively applied to it and all of its
   * children (pre-order). When `rule` does not apply to a given node it is left unchanged.
   * @param rule the function used to transform this nodes children
   */
  def transformDown(rule: PartialFunction[BaseType, BaseType]): BaseType
```

```
   /**
   * Returns a copy of this node where `rule` has been recursively applied to all the children of
   * this node.  When `rule` does not apply to a given node it is left unchanged.
   * @param rule the function used to transform this nodes children
   */
  def transformChildrenDown(rule: PartialFunction[BaseType, BaseType]): this.type
```

##### TreeNode分为三大类
1. UnaryNode 一元节点，即只有一个子节点。如Limit、Filter操作
2. BinaryNode 二元节点，即有左右子节点的二叉节点。如Jion、Union操作
3. LeafNode 叶子节点，没有子节点的节点。主要用户命令类操作，如SetCommand

```
/**
 * A [[TreeNode]] that has two children, [[left]] and [[right]].
 */
trait BinaryNode[BaseType <: TreeNode[BaseType]] {
  def left: BaseType
  def right: BaseType

  def children = Seq(left, right)
}

/**
 * A [[TreeNode]] with no children.
 */
trait LeafNode[BaseType <: TreeNode[BaseType]] {
  def children = Nil
}

/**
 * A [[TreeNode]] with a single [[child]].
 */
trait UnaryNode[BaseType <: TreeNode[BaseType]] {
  def child: BaseType
  def children = child :: Nil
}
```

### Rule
Rule的相关代码定义在sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/rules，Rule类是抽象类，调用apply方法可以进行Tree的transformation。

```
abstract class Rule[TreeType <: TreeNode[_]] extends Logging {

  /** Name for this rule, automatically inferred based on class name. */
  val ruleName: String = {
    val className = getClass.getName
    if (className endsWith "$") className.dropRight(1) else className
  }

  def apply(plan: TreeType): TreeType
}
```
Rule具体的实现定义了RuleExecutor中，通过定义batchs，可以简便的、模块化地对Tree进行transform操作。Once和FixedPoint，可以对Tree进行一次操作或多次操作（如对某些Tree进行多次迭代操作的时候，达到FixedPoint次数迭代或达到前后两次的树结构没变化才停止操作，具体参看RuleExecutor.apply）

```
abstract class RuleExecutor[TreeType <: TreeNode[_]] extends Logging {
  /**
   * An execution strategy for rules that indicates the maximum number of executions. If the
   * execution reaches fix point (i.e. converge) before maxIterations, it will stop.
   */
  abstract class Strategy { def maxIterations: Int }

  /** A strategy that only runs once. */
  case object Once extends Strategy { val maxIterations = 1 }

  /** A strategy that runs until fix point or maxIterations times, whichever comes first. */
  case class FixedPoint(maxIterations: Int) extends Strategy

  /** A batch of rules. */
  protected case class Batch(name: String, strategy: Strategy, rules: Rule[TreeType]*)

  /** Defines a sequence of rule batches, to be overridden by the implementation. */
  protected val batches: Seq[Batch]
  /**
   * Executes the batches of rules defined by the subclass. The batches are executed serially
   * using the defined execution strategy. Within each batch, rules are also executed serially.
   */
  def apply(plan: TreeType): TreeType = {
  ...
  }
}
```






