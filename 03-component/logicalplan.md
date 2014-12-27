# TreeNode

在sparkSQL的运行架构中，LogicalPlan贯穿了大部分的过程，其中catalyst中的SqlParser、Analyzer、Optimizer都要对LogicalPlan进行操作。LogicalPlan继承自QueryPlan，而QueryPlan继承自TreeNode。

```
abstract class LogicalPlan extends QueryPlan[LogicalPlan] with Logging

abstract class QueryPlan[PlanType <: TreeNode[PlanType]] extends TreeNode[PlanType]

abstract class TreeNode[BaseType <: TreeNode[BaseType]]
```

TreeNode Library是Catalyst的核心类库，语法树的构建都是由一个个TreeNode组成。在Catalyst里，这些Node都是继承自Logical Plan，可以说每一个TreeNode节点就是一个Logical Plan(包含Expression）（直接继承自TreeNode）。主要继承关系类图如下：

![](/images/tree-node.png)

### 核心方法 transform 方法
  transform该方法接受一个PartialFunction，Analyzer到的Batch里面的Rule。
  是会将Rule迭代应用到该节点的所有子节点，最后返回这个节点的副本。
  如果rule没有对一个节点进行PartialFunction的操作，就返回这个节点本身。

 ```
/**
   * Returns a copy of this node where `rule` has been recursively applied to the tree.
   * When `rule` does not apply to a given node it is left unchanged.
   * Users should not expect a specific directionality. If a specific directionality is needed,
   * transformDown or transformUp should be used.
   * @param rule the function use to transform this nodes children
   */
  def transform(rule: PartialFunction[BaseType, BaseType]): BaseType = {
    transformDown(rule)
  }
```

### transformDown方法
transform方法真正的调用是transformDown，这里提到了用先序遍历来对子节点进行递归的Rule应用。如果在对当前节点应用rule成功，修改后的节点afterRule，来对其children节点进行rule的应用。
```
 /**
   * Returns a copy of this node where `rule` has been recursively applied to it and all of its
   * children (pre-order). When `rule` does not apply to a given node it is left unchanged.
   * @param rule the function used to transform this nodes children
   */
  def transformDown(rule: PartialFunction[BaseType, BaseType]): BaseType = {
    val afterRule = rule.applyOrElse(this, identity[BaseType])
    // Check if unchanged and then possibly return old copy to avoid gc churn.
    if (this fastEquals afterRule) {
      transformChildrenDown(rule)
    } else {
      afterRule.transformChildrenDown(rule)
    }
  }
```

### transformChildrenDown方法
最重要的方法transformChildrenDown:
  对children节点进行递归的调用PartialFunction，利用最终返回的newArgs来生成一个新的节点，这里调用了makeCopy()来生成节点。
 ```
  /**
   * Returns a copy of this node where `rule` has been recursively applied to all the children of
   * this node.  When `rule` does not apply to a given node it is left unchanged.
   * @param rule the function used to transform this nodes children
   */
  def transformChildrenDown(rule: PartialFunction[BaseType, BaseType]): this.type = {
    var changed = false
    val newArgs = productIterator.map {
      case arg: TreeNode[_] if children contains arg =>
        val newChild = arg.asInstanceOf[BaseType].transformDown(rule)
        if (!(newChild fastEquals arg)) {
          changed = true
          newChild
        } else {
          arg
        }
      case Some(arg: TreeNode[_]) if children contains arg =>
        val newChild = arg.asInstanceOf[BaseType].transformDown(rule)
        if (!(newChild fastEquals arg)) {
          changed = true
          Some(newChild)
        } else {
          Some(arg)
        }
      case m: Map[_,_] => m
      case args: Traversable[_] => args.map {
        case arg: TreeNode[_] if children contains arg =>
          val newChild = arg.asInstanceOf[BaseType].transformDown(rule)
          if (!(newChild fastEquals arg)) {
            changed = true
            newChild
          } else {
            arg
          }
        case other => other
      }
      case nonChild: AnyRef => nonChild
      case null => null
    }.toArray
    if (changed) makeCopy(newArgs) else this
  }
```

### makeCopy方法，反射生成节点副本
```
/**
   * Creates a copy of this type of tree node after a transformation.
   * Must be overridden by child classes that have constructor arguments
   * that are not present in the productIterator.
   * @param newArgs the new product arguments.
   */
  def makeCopy(newArgs: Array[AnyRef]): this.type = attachTree(this, "makeCopy") {
    try {
      // Skip no-arg constructors that are just there for kryo.
      val defaultCtor = getClass.getConstructors.find(_.getParameterTypes.size != 0).head
      if (otherCopyArgs.isEmpty) {
        defaultCtor.newInstance(newArgs: _*).asInstanceOf[this.type]
      } else {
        defaultCtor.newInstance((newArgs ++ otherCopyArgs).toArray: _*).asInstanceOf[this.type]
      }
    } catch {
      case e: java.lang.IllegalArgumentException =>
        throw new TreeNodeException(
          this, s"Failed to copy node.  Is otherCopyArgs specified correctly for $nodeName? "
            + s"Exception message: ${e.getMessage}.")
    }
  }
```





