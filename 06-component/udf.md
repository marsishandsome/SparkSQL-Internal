# UDF
在SqlParser除了非官方定义的函数外，还可以定义自定义函数，sql parser会进行解析。

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

在SqlContext中有一个functionRegistry

```
protected[sql] lazy val functionRegistry: FunctionRegistry = new SimpleFunctionRegistry
```


