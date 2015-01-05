# SqlParser
SqlParser是一个SQL语言的解析器，功能是将SQL语句解析成Unresolved Logical Plan。

### Scala的词法和语法解析器
SqlParser使用的是Scala提供的StandardTokenParsers和PackratParsers，分别用于词法解析和语法解析，首先为了理解对SQL语句解析过程的理解，先来看看下面这个简单数字表达式的解析过程。

```
import scala.util.parsing.combinator.PackratParsers
import scala.util.parsing.combinator.syntactical._

object MyLexical extends StandardTokenParsers with PackratParsers{

  //定义分割符
  lexical.delimiters ++= List(".", ";", "+", "-", "*")
  //定义表达式，支持加，减，乘
  lazy val expr: PackratParser[Int] = plus | minus | multi
  //加法表示式的实现
  lazy val plus: PackratParser[Int] = num ~ "+" ~ num ^^ { case n1 ~ "+" ~ n2 => n1.toInt + n2.toInt}
  //减法表达式的实现
  lazy val minus: PackratParser[Int] = num ~ "-" ~ num ^^ { case n1 ~ "-" ~ n2 => n1.toInt - n2.toInt}
  //乘法表达式的实现
  lazy val multi: PackratParser[Int] = num ~ "*" ~ num ^^ { case n1 ~ "*" ~ n2 => n1.toInt * n2.toInt}
  lazy val num = numericLit

  def parse(input: String) = {
    //定义词法读入器myread，并将扫描头放置在input的首位
    val myread = new PackratReader(new lexical.Scanner(input))
    print("处理表达式 " + input)
    phrase(expr)(myread) match {
      case Success(result, _) => println(" Success!"); println(result); Some(result)
      case n => println(n); println("Err!"); None
    }
  }

  def main(args: Array[String]) {
    val prg = "6 * 3" :: "24-/*aaa*/4" :: "a+5" :: "21/3" :: Nil
    prg.map(parse)
  }
}
```

运行结果：
```
处理表达式 6 * 3 Success!     //lexical对空格进行了处理，得到6*3
18     //6*3符合乘法表达式，调用n1.toInt * n2.toInt，得到结果并返回
处理表达式 24-/*aaa*/4 Success!  //lexical对注释进行了处理，得到20-4
20    //20-4符合减法表达式，调用n1.toInt - n2.toInt，得到结果并返回
处理表达式 a+5[1.1] failure: number expected
      //lexical在解析到a，发现不是整数型，故报错误位置和内容
a+5
^
Err!
处理表达式 21/3[1.3] failure: ``*'' expected but ErrorToken(illegal character) found
      //lexical在解析到/，发现不是分割符，故报错误位置和内容
21/3
  ^
Err!

```
在运行的时候，首先对表达式 6 \* 3 进行解析，词法读入器myread将扫描头置于6的位置；当phrase()函数使用定义好的数字表达式expr处理6 \* 3的时候，每读入一个词就和expr进行匹配，如读入6\*3和expr进行匹配，先匹配表达式plus，\*和\+匹配不上；就继续匹配表达式minus，\*和\-匹配不上；就继续匹配表达式multi，这次匹配上了，等读入3的时候，因为3是num类型，就调用调用n1.toInt \* n2.toInt进行计算。

注意，这里的expr、plus、minus、multi、num都是表达式，|、~、^^是复合因子，表达式和复合因子可以组成一个新的表达式，如plus（num ~ "+" ~ num ^^ { case n1 ~ "+" ~ n2 => n1.toInt + n2.toInt}）就是一个由num、+、num、函数构成的复合表达式；而expr（plus | minus | multi）是由plus、minus、multi构成的复合表达式；复合因子的含义定义在类scala/util/parsing/combinator/Parsers.scala，下面是几个常用的复合因子：

| 表达式 | 含义 |
| -- | -- |
| p ~ q | p成功，才会q，放回p,q的结果 |
| p ~> q | p成功，才会q，返回q的结果 |
| p <~ q | p成功，才会q，返回p的结果 |
| p 或 q | p失败则q，返回第一个成功的结果 |
| p ^^ f | 如果p成功，将函数f应用到p的结果上 |
| p ^? f | 如果p成功，如果函数f可以应用到p的结果上的话，就将p的结果用f进行转换 |


针对上面的6 \* 3使用的是multi表达式

```(num ~ "\*" ~ num ^^ { case n1 ~ "\*" ~ n2 => n1.toInt \* n2.toInt})```

其含义就是：num后跟\*再跟num，如果满足就将使用函数n1.toInt \* n2.toInt。

### SqlParser入口
SqlParser的入口在SqlContext的sql()函数，该函数会调用parserSql并返回SchemaRDD。
```
/**
   * Executes a SQL query using Spark, returning the result as a SchemaRDD.  The dialect that is
   * used for SQL parsing can be configured with 'spark.sql.dialect'.
   *
   * @group userf
   */
  def sql(sqlText: String): SchemaRDD = {
    if (dialect == "sql") {
      new SchemaRDD(this, parseSql(sqlText))
    } else {
      sys.error(s"Unsupported SQL dialect: $dialect")
    }
  }
```

parseSql会调用ddlParser，如果不成功就调用sqlParser。接着sqlParser会调用SparkSQLParser，并且把catalyst.SqlParser传递进去。
```
 protected[sql] def parseSql(sql: String): LogicalPlan = {
    ddlParser(sql).getOrElse(sqlParser(sql))
  }

    protected[sql] val sqlParser = {
    val fallback = new catalyst.SqlParser
    new catalyst.SparkSQLParser(fallback(_))
  }
```

SparkSQLParser的功能是解析SparkSQL特有的语法，例如cache，lazy等。SparkSQLParser会首先按照自己定义的词法和语法进行解析，当遇到以下两种情况的时候，会调用传递进来的catalyst.SqlParser:
1. Cache关键字后面的语法解析
2. 其他SparkSQLParser未定义的语法

```
private[sql] class SparkSQLParser(fallback: String => LogicalPlan) extends AbstractSparkSQLParser {
...
private lazy val cache: Parser[LogicalPlan] =
    CACHE ~> LAZY.? ~ (TABLE ~> ident) ~ (AS ~> restInput).? ^^ {
      case isLazy ~ tableName ~ plan =>
        CacheTableCommand(tableName, plan.map(fallback), isLazy.isDefined)
    }
...
  private lazy val others: Parser[LogicalPlan] =
    wholeInput ^^ {
      case input => fallback(input)
    }
}
```


### SqlParser
SqlParser继承自AbstractSparkSQLParser，而AbstractSparkSQLParser继承自StandardTokenParsers和 PackratParsers。SqlParser中定义了SQL语言的词法和语法规则：

**词法：** SqlParser首先定义了一堆Keyword，然后通过反射机制把这些Keyword全部加到一个reservedWords的集合当中，最后把这些关键字加到SqlLexical中。SqlLexical中除了定义关键字以外，还定义了分隔符。

```
class SqlParser extends AbstractSparkSQLParser {
...
  protected val ABS = Keyword("ABS")
  protected val ALL = Keyword("ALL")
  protected val AND = Keyword("AND")
  protected val APPROXIMATE = Keyword("APPROXIMATE")
  protected val AS = Keyword("AS")
  protected val ASC = Keyword("ASC")
  protected val AVG = Keyword("AVG")
  protected val BETWEEN = Keyword("BETWEEN")
  ...
  protected val reservedWords =
    this
      .getClass
      .getMethods
      .filter(_.getReturnType == classOf[Keyword])
      .map(_.invoke(this).asInstanceOf[Keyword].str)
  ...
  override val lexical = new SqlLexical(reservedWords)
  ...
}
```

```
class SqlLexical(val keywords: Seq[String]) extends StdLexical {
  ...
  reserved ++= keywords.flatMap(w => allCaseVersions(w))

  delimiters += (
    "@", "*", "+", "-", "<", "=", "<>", "!=", "<=", ">=", ">", "/", "(", ")",
    ",", ";", "%", "{", "}", ":", "[", "]", ".", "&", "|", "^", "~", "<=>"
  )
  ...
}
```

**语法：** sql语法的根节点是```val start: Parser[LogicalPlan]```，语法树的返回类型是```LogicalPlan```。

```
class SqlParser extends AbstractSparkSQLParser {
  ...
  protected lazy val start: Parser[LogicalPlan] =
    ( select *
      ( UNION ~ ALL        ^^^ { (q1: LogicalPlan, q2: LogicalPlan) => Union(q1, q2) }
      | INTERSECT          ^^^ { (q1: LogicalPlan, q2: LogicalPlan) => Intersect(q1, q2) }
      | EXCEPT             ^^^ { (q1: LogicalPlan, q2: LogicalPlan) => Except(q1, q2)}
      | UNION ~ DISTINCT.? ^^^ { (q1: LogicalPlan, q2: LogicalPlan) => Distinct(Union(q1, q2)) }
      )
    | insert
    )

  protected lazy val select: Parser[LogicalPlan] =
    SELECT ~> DISTINCT.? ~
      repsep(projection, ",") ~
      (FROM   ~> relations).? ~
      (WHERE  ~> expression).? ~
      (GROUP  ~  BY ~> rep1sep(expression, ",")).? ~
      (HAVING ~> expression).? ~
      (ORDER  ~  BY ~> ordering).? ~
      (LIMIT  ~> expression).? ^^ {
        case d ~ p ~ r ~ f ~ g ~ h ~ o ~ l  =>
          val base = r.getOrElse(NoRelation)
          val withFilter = f.map(Filter(_, base)).getOrElse(base)
          val withProjection = g
            .map(Aggregate(_, assignAliases(p), withFilter))
            .getOrElse(Project(assignAliases(p), withFilter))
          val withDistinct = d.map(_ => Distinct(withProjection)).getOrElse(withProjection)
          val withHaving = h.map(Filter(_, withDistinct)).getOrElse(withDistinct)
          val withOrder = o.map(Sort(_, withHaving)).getOrElse(withHaving)
          val withLimit = l.map(Limit(_, withOrder)).getOrElse(withOrder)
          withLimit
      }

  protected lazy val insert: Parser[LogicalPlan] =
    INSERT ~> OVERWRITE.? ~ (INTO ~> relation) ~ select ^^ {
      case o ~ r ~ s => InsertIntoTable(r, Map.empty[String, Option[String]], s, o.isDefined)
    }
    ...
}
```

### AbstractSparkSQLParser
sql真正的解析是在AbstractSparkSQLParser中进行的，AbstractSparkSQLParser继承自StandardTokenParsers和PackratParsers。解析功能的核心代码就是：```phrase(start)(new lexical.Scanner(input))```。可以看得出来，该语句就是调用phrase()函数，使用SQL语法表达式start，对词法读入器lexical读入的SQL语句进行解析，其中

1. 词法分析器lexical定义在SqlParser中```override val lexical = new SqlLexical(reservedWords)```
2. 语法分析器start定义在SqlParser中```protected lazy val start: Parser[LogicalPlan] =...```

```
abstract class AbstractSparkSQLParser
  extends StandardTokenParsers with PackratParsers {

  def apply(input: String): LogicalPlan = phrase(start)(new lexical.Scanner(input)) match {
    case Success(plan, _) => plan
    case failureOrError => sys.error(failureOrError.toString)
  }
 ...
}
```




