# UDF
在sql语句中，除了可以使用+ - * /等表达式外，还可以使用用户定义的函数UDF。下面是SqlParser中对UDF的语法定义：

```
protected lazy val function: Parser[Expression] =
    ( SUM   ~> "(" ~> expression             <~ ")" ^^ { case exp => Sum(exp) }
    ...
    | ident ~ ("(" ~> repsep(expression, ",")) <~ ")" ^^
      { case udfName ~ exprs => UnresolvedFunction(udfName, exprs) }
    )
```

将SqlParser传入的udfName和exprs封装成一个叫 UnresolvedFunction的类，该类继承自Expression。只是这个Expression的dataType等一系列属性和eval计算方法均无法访问，强制访问会抛出异常，因为它没有被Resolved，只是一个载体。

```
case class UnresolvedFunction(name: String, children: Seq[Expression]) extends Expression {
  override def dataType = throw new UnresolvedException(this, "dataType")
  override def foldable = throw new UnresolvedException(this, "foldable")
  override def nullable = throw new UnresolvedException(this, "nullable")
  override lazy val resolved = false

  // Unresolved functions are transient at compile time and don't get evaluated during execution.
  override def eval(input: Row = null): EvaluatedType =
    throw new TreeNodeException(this, s"No function to evaluate expression. type: ${this.nodeName}")

  override def toString = s"'$name(${children.mkString(",")})"
}
```

在SqlContext中有一个functionRegistry对象，使用的是SimpleFunctionRegistry，用来存储用户定义的函数。
```
protected[sql] lazy val functionRegistry: FunctionRegistry = new SimpleFunctionRegistry
```

### UDF注册

##### UDFRegistration
registerFunction是UDFRegistration下的方法，SQLContext现在实现了UDFRegistration这个trait，只要导入SQLContext，即可以使用udf功能。

```
class SQLContext(@transient val sparkContext: SparkContext)
  extends org.apache.spark.Logging
  with SQLConf
  with CacheManager
  with ExpressionConversions
  with UDFRegistration
  with Serializable
```

registerFunction接受一个name和 一个func，可以是Function1到Function22，即这个udf的参数只支持1-22个。
```
private[sql] trait UDFRegistration {
...
def registerFunction[T: TypeTag](name: String, func: Function1[_, T]): Unit = {
    def builder(e: Seq[Expression]) = ScalaUdf(func, ScalaReflection.schemaFor[T].dataType, e)
    functionRegistry.registerFunction(name, builder)
  }
  ...
  def registerFunction[T: TypeTag](name: String, func: Function22[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, T]): Unit = {
    def builder(e: Seq[Expression]) = ScalaUdf(func, ScalaReflection.schemaFor[T].dataType, e)
    functionRegistry.registerFunction(name, builder)
  }
}
```

内部builder通过ScalaUdf来构造一个Expression，这里ScalaUdf继承自Expression，传入scala的function作为UDF的实现。
```
case class ScalaUdf(function: AnyRef, dataType: DataType, children: Seq[Expression])
  extends Expression {
  ...
}
```

##### SimpleFunctionRegistry
SimpleFunctionRegistry中使用HashMap存储用户定义的函数。

```
type FunctionBuilder = Seq[Expression] => Expression
```

```
class SimpleFunctionRegistry extends FunctionRegistry {
  val functionBuilders = new mutable.HashMap[String, FunctionBuilder]()

  def registerFunction(name: String, builder: FunctionBuilder) = {
    functionBuilders.put(name, builder)
  }

  override def lookupFunction(name: String, children: Seq[Expression]): Expression = {
    functionBuilders(name)(children)
  }
}
```

### UDF计算
UDF既然已经被封装为catalyst树里的一个Expression节点，那么计算的时候也就是计算ScalaUdf的eval方法。先通过Row和表达式计算function所需要的参数，最后通过反射调用function，来达到计算udf的目的。

```
case class ScalaUdf(function: AnyRef, dataType: DataType, children: Seq[Expression])
  extends Expression {

  override def eval(input: Row): Any = {
    val result = children.size match {
      case 0 => function.asInstanceOf[() => Any]()
      case 1 =>
        function.asInstanceOf[(Any) => Any](
          ScalaReflection.convertToScala(children(0).eval(input), children(0).dataType))
          ...
}
```


