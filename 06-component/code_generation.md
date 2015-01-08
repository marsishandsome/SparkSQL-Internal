# Code Generation
SparkSQL利用Scala提供的Reflection和Quasiquotes机制来实现Code Generation。

### Scala Reflection
Java本身就已经提供了Reflection功能，在Scala2.10之前，Scala并不提供额外的Reflection机制，Scala2.10在Java Reflection基础上又实现了一套更加powerful的Reflection机制。Scala中的Reflection分为两大类：Runtime Reflection和Compile-time Reflection。

##### Runtime Reflection
Runtime Reflection主要包括
1. 动态获取对象的类型
2. 动态新建对象
3. 动态调用对象的方法

这部分和Java提供的Reflection比较类似。

##### Compile-time Reflection
Scala的Compile-time Reflection允许程序在编译期间修改自己的代码，从而实现meta-programming。Complie-time Reflection是通过macros实现的，macros允许程序可以在编译期间修改自己的语法树。

##### Abstract Syntax Tree
Scala使用Abstract Syntax Tree来表示Scala程序，Scala的Reflection提供了多种方法来操作AST:
1. reify方法可以把一个表达式变成一个AST
2. 在Compile-time，通过macros可以操作AST
3. 在Runtime，通过toolboxes也可以操作AST

下面的例子演示了如果在Runtime利用toolboxes操作AST
1. 在Runtime利用ToolBox的parse函数，将一段代码变了AST
2. 然后通过ToolBox的eval函数，将生成的AST进行求值，返回该段代码的返回值f，f是一个实现+1操作的函数
3. 最后调用返回的函数f

```
import scala.tools.reflect.ToolBox

object HelloToolBox {
  def main(args: Array[String]): Unit = {
    val toolBox = scala.reflect.runtime.universe.
      runtimeMirror(getClass.getClassLoader).mkToolBox()

    val code =
      """
        val f = {x: Int => x + 1}
        f
      """

    val tree = toolBox.parse(code)
    println(tree)

    val func = toolBox.eval(tree)
    val result = func.asInstanceOf[Int => Int](1)
    println(result)

  }
}
```

程序的输出为：
```
{
  val f = ((x: Int) => x.$plus(1));
  f
}
2
```

### Scala Quasiquotes
Quasiquotes是Scala提供的方便操作AST的库，当把代码放到```q"..."```里面时，将会返回一个AST。
```
scala> val tree = q"i am { a quasiquote }"
tree: universe.Tree = i.am(a.quasiquote)
```

下面的例子演示了如何利用Quasiquotes来实现Code Generation:
```
import scala.reflect.runtime.currentMirror
import scala.tools.reflect.ToolBox

object HelloQuasiquotes {

  def main(args: Array[String]): Unit = {
    val universe: scala.reflect.runtime.universe.type = scala.reflect.runtime.universe
    import universe._

    val toolbox = currentMirror.mkToolBox()
    val tree = q"""
              val f = {x: Int => x + 1}
              f
             """

    println(showCode(tree))

    val func = toolbox.eval(tree)
    val result = func.asInstanceOf[Int => Int](1)
    println(result)
  }
}
```

程序输出为：
```
{
  val f = ((x: Int) => x.+(1));
  f
}
2
```

### SparkSQL Code Generation
SparkSQL使用Quasiquotes来实现Code Genration。Quasiquotes是Scala 2.11的新功能，当使用Scala 2.10来编译Spark时，只能通过[macro paradise compiler plugin](http://docs.scala-lang.org/overviews/macros/paradise.html)的方式使用quasiquotes。

##### CodeGenerator
CodeGenerator是代码生成的基类，里面定义了
1. expressionEvaluator：把一个Expression变成EvaluatedExpression，即AST
2. toolBox: 可以在返回的AST上求值，返回动态生成的代码的调用入口

```
abstract class CodeGenerator[InType <: AnyRef, OutType <: AnyRef] extends Logging {
  import scala.reflect.runtime.{universe => ru}
  import scala.reflect.runtime.universe._
  import scala.tools.reflect.ToolBox

  protected val toolBox = runtimeMirror(getClass.getClassLoader).mkToolBox()

  def expressionEvaluator(e: Expression): EvaluatedExpression = {
    ...
  }
  ...
}
```

EvaluatedExpression可以理解为已经生成好的代码，这段代码可以在Row上求值，EvaluatedEpression包括
1. code：AST的List，用来求值的代码
2. nullTerm：代码中的变量名，表示最后的值是否是null
3. primitiveTerm：代码中的变量名，表示最后的值的原始类型，无效如果nullTerm=true
4. objectTerm：代码中的变量名，表示最后的值的包装类型

```
  protected case class EvaluatedExpression(
      code: Seq[Tree],
      nullTerm: TermName,
      primitiveTerm: TermName,
      objectTerm: TermName)
```

下面来看一下Code Generation的关键函数expressionEvaluator。
1. 调用freshName为primitiveTerm、nullTerm、objectTerm生成变量名
2. 调用primitiveEvaluation，把Expression变成Seq[Tree]，可能会失败
3. 如果primitiveEvaluation失败，则生成表达式树求值的代码
4. 返回EvaluatedExpression

```
def expressionEvaluator(e: Expression): EvaluatedExpression = {
    val primitiveTerm = freshName("primitiveTerm")
    val nullTerm = freshName("nullTerm")
    val objectTerm = freshName("objectTerm")
    val primitiveEvaluation: PartialFunction[Expression, Seq[Tree]] = {...}
    val code: Seq[Tree] =
      primitiveEvaluation.lift.apply(e).getOrElse {
        log.debug(s"No rules to generate $e")
        val tree = reify { e }
        q"""
          val $objectTerm = $tree.eval(i)
          val $nullTerm = $objectTerm == null
          val $primitiveTerm = $objectTerm.asInstanceOf[${termForType(e.dataType)}]
         """.children
      }
      ...
    EvaluatedExpression(code ++ debugCode, nullTerm, primitiveTerm, objectTerm)
}
```

primitiveEvaluation是一个PartialFunction，把Expression变成Seq[Tree]。

下面分析一下And是如何生成代码的：
1. 分别生成e1和e2的AST
2. 分几种情况判断And的结果是否为null以及是否为primitive
3. 返回e1、e2和add的代码

```
val primitiveEvaluation: PartialFunction[Expression, Seq[Tree]] = {
    ...
    case And(e1, e2) =>
        val eval1 = expressionEvaluator(e1)
        val eval2 = expressionEvaluator(e2)

        eval1.code ++ eval2.code ++
        q"""
          var $nullTerm = false
          var $primitiveTerm: ${termForType(BooleanType)} = false

          if ((!${eval1.nullTerm} && !${eval1.primitiveTerm}) ||
              (!${eval2.nullTerm} && !${eval2.primitiveTerm})) {
            $nullTerm = false
            $primitiveTerm = false
          } else if (${eval1.nullTerm} || ${eval2.nullTerm} ) {
            $nullTerm = true
          } else {
            $nullTerm = false
            $primitiveTerm = true
          }
         """.children
  ...
}
```

Add的代码看上去更加简洁，只有一句话，其实是因为把(e1, e2)隐式转换成了class Evaluate2，并调用其evaluate方法。
```
case Add(e1, e2) =>      (e1, e2) evaluate { case (eval1, eval2) => q"$eval1 + $eval2" }
```

evaluate方法接受一个function，返回一个Seq[Tree]。function接收两个scala reflaction中的TermName，返回一个AST。

1. evaluate方首先调用expressionEvaluator，分别计算两个表达式的AST
2. 然后生成传递进来的function的AST，参数为1中得到的primitiveTerm
3. 生成function代码的nullTerm
4. 生成function代码的primitiveTerm，如果是null的话返回该类型的默认值

```
implicit class Evaluate2(expressions: (Expression, Expression)) {
    def evaluate(f: (TermName, TermName) => Tree): Seq[Tree] =
        evaluateAs(expressions._1.dataType)(f)

      def evaluateAs(resultType: DataType)(f: (TermName, TermName) => Tree): Seq[Tree] = {
        // TODO: Right now some timestamp tests fail if we enforce this...
        if (expressions._1.dataType != expressions._2.dataType) {
          log.warn(s"${expressions._1.dataType} != ${expressions._2.dataType}")
        }

        val eval1 = expressionEvaluator(expressions._1)
        val eval2 = expressionEvaluator(expressions._2)
        val resultCode = f(eval1.primitiveTerm, eval2.primitiveTerm)

        eval1.code ++ eval2.code ++
        q"""
          val $nullTerm = ${eval1.nullTerm} || ${eval2.nullTerm}
          val $primitiveTerm: ${termForType(resultType)} =
            if($nullTerm) {
              ${defaultPrimitive(resultType)}
            } else {
              $resultCode.asInstanceOf[${termForType(resultType)}]
            }
        """.children : Seq[Tree]
      }
    }
    ...
}
```

来看一下Code Generation生成出来的Add的示例代码：
```
(() => {
  final class $anon extends org.apache.spark.sql.catalyst.expressions.MutableProjection {
    def <init>() = {
      super.<init>();
      ()
    };
    private[this] var mutableRow: org.apache.spark.sql.catalyst.expressions.MutableRow = new org.apache.spark.sql.catalyst.expressions.GenericMutableRow(1);
    def target(row: org.apache.spark.sql.catalyst.expressions.MutableRow): org.apache.spark.sql.catalyst.expressions.MutableProjection = {
      mutableRow = row;
      this
    };
    def currentValue: org.apache.spark.sql.catalyst.expressions.Row = mutableRow;
    def apply(i: org.apache.spark.sql.catalyst.expressions.Row): org.apache.spark.sql.catalyst.expressions.Row = {
      val nullTerm$4: Boolean = i.isNullAt(0);
      val primitiveTerm$3: org.apache.spark.sql.catalyst.types.IntegerType.JvmType = if (nullTerm$4)
        -1
      else
        i.getInt(0);
      ();
      val nullTerm$7: Boolean = i.isNullAt(1);
      val primitiveTerm$6: org.apache.spark.sql.catalyst.types.IntegerType.JvmType = if (nullTerm$7)
        -1
      else
        i.getInt(1);
      ();
      val nullTerm$1 = nullTerm$4.$bar$bar(nullTerm$7);
      val primitiveTerm$0: org.apache.spark.sql.catalyst.types.IntegerType.JvmType = if (nullTerm$1)
        -1
      else
        primitiveTerm$3.$plus(primitiveTerm$6).asInstanceOf[org.apache.spark.sql.catalyst.types.IntegerType.JvmType];
      ();
      if (nullTerm$1)
        mutableRow.setNullAt(0)
      else
        mutableRow.setInt(0, primitiveTerm$0);
      mutableRow
    }
  };
  new $anon()
})
```

有四个类继承了CodeGenrator
1. GenerateProjection
2. GenerateMutableProjection
3. GenerateOrdering
4. GeneratePredicate


##### GeneratePredicate
下面来分析一下GeneratePredicate这个类。create函数根据一个表达式生成了一段函数代码，该函数的输入是Row（即一行数据），输出是Boolean（即该行是否被过滤）。
1. 首先调用expressionEvaluator方法，计算出Expression的AST
2. 然后生成返回的函数的代码，函数首先调用1中所得到的AST，如果1中AST的nullTerm=true，则返回false，否则返回1中AST的实际返回值primitiveTerm
3. 最后调用toolBox的eval函数，获得AST中function的实例入口地址

```
object GeneratePredicate extends CodeGenerator[Expression, (Row) => Boolean] {
  import scala.reflect.runtime.{universe => ru}
  import scala.reflect.runtime.universe._

  protected def canonicalize(in: Expression): Expression = ExpressionCanonicalizer(in)

  protected def bind(in: Expression, inputSchema: Seq[Attribute]): Expression =
    BindReferences.bindReference(in, inputSchema)

  protected def create(predicate: Expression): ((Row) => Boolean) = {
    val cEval = expressionEvaluator(predicate)

    val code =
      q"""
        (i: $rowType) => {
          ..${cEval.code}
          if (${cEval.nullTerm}) false else ${cEval.primitiveTerm}
        }
      """
    toolBox.eval(code).asInstanceOf[Row => Boolean]
  }
}
```

### 参考
http://docs.scala-lang.org/overviews/reflection/symbols-trees-types.html

http://docs.scala-lang.org/overviews/quasiquotes/intro.html

http://docs.scala-lang.org/overviews/quasiquotes/setup.html

http://docs.scala-lang.org/overviews/macros/paradise.html

http://www.infoq.com/cn/news/2013/01/scala-macros

https://databricks.com/blog/2014/06/02/exciting-performance-improvements-on-the-horizon-for-spark-sql.html

https://gist.github.com/marmbrus/9efb31d2b5154aea6652



