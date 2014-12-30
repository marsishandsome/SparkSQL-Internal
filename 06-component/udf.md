# UDF
在SqlParser除了官方定义的函数外，还可以定义自定义函数，sql parser会进行解析。

```
protected lazy val function: Parser[Expression] =
    ( SUM   ~> "(" ~> expression             <~ ")" ^^ { case exp => Sum(exp) }
    ...
    | ident ~ ("(" ~> repsep(expression, ",")) <~ ")" ^^
      { case udfName ~ exprs => UnresolvedFunction(udfName, exprs) }
    )
```

将SqlParser传入的udfName和exprs封装成一个class UnresolvedFunction继承自Expression。只是这个Expression的dataType等一系列属性和eval计算方法均无法访问，强制访问会抛出异常，因为它没有被Resolved，只是一个载体。

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

在SqlContext中有一个functionRegistry，使用的是SimpleFunctionRegistry，用来存储用户定义的函数。
```
protected[sql] lazy val functionRegistry: FunctionRegistry = new SimpleFunctionRegistry
```

### UDF注册

##### UDFRegistration
registerFunction是UDFRegistration下的方法，SQLContext现在实现了UDFRegistration这个trait，只要导入SQLContext，即可以使用udf功能。

UDFRegistration核心方法registerFunction。registerFunction方法签名

```
def registerFunction[T: TypeTag](name: String, func: Function1[_, T]): Unit
```

接受一个udfName 和 一个FunctionN，可以是Function1到Function22。即这个udf的参数只支持1-22个。（scala的痛啊）内部builder通过ScalaUdf来构造一个Expression，这里ScalaUdf继承自Expression（可以简单的理解目前的SimpleUDF即是一个Catalyst的一个Expression），传入scala的function作为UDF的实现，并且用反射检查字段类型是否是Catalyst允许的。

```
/**
 * Functions for registering scala lambda functions as UDFs in a SQLContext.
 */
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
UDF既然已经被封装为catalyst树里的一个Expression节点，那么计算的时候也就是计算ScalaUdf的eval方法。先通过Row和表达式计算function所需要的参数，最后通过反射调用function，来达到计算udf的目的。scalaUdf接受一个function, dataType，和一系列表达式。

```
/**
 * User-defined function.
 * @param dataType  Return type of function.
 */
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


