# Optimizer
Optimizer的主要职责是将Analyzer给Resolved的Logical Plan根据不同的优化策略Batch，来对语法树进行优化。

### Optimizer
Optimizer这个类是在catalyst里的optimizer包下的唯一一个类，Optimizer的工作方式其实类似Analyzer，因为它们都继承自RuleExecutor[LogicalPlan]，都是执行一系列的Batch操作。

Optimizer里的batches包含了3类优化策略：
1. Combine Limits 合并Limits
2. ConstantFolding 常量合并
3. Filter Pushdown 过滤器下推
每个Batch里定义的优化伴随对象都定义在Optimizer里了。

```
abstract class Optimizer extends RuleExecutor[LogicalPlan]

object DefaultOptimizer extends Optimizer {
  val batches =
    Batch("Combine Limits", FixedPoint(100),
      CombineLimits) ::
    Batch("ConstantFolding", FixedPoint(100),
      NullPropagation,
      ConstantFolding,
      LikeSimplification,
      BooleanSimplification,
      SimplifyFilters,
      SimplifyCasts,
      SimplifyCaseConversionExpressions,
      OptimizeIn) ::
    Batch("Decimal Optimizations", FixedPoint(100),
      DecimalAggregates) ::
    Batch("Filter Pushdown", FixedPoint(100),
      UnionPushdown,
      CombineFilters,
      PushPredicateThroughProject,
      PushPredicateThroughJoin,
      ColumnPruning) :: Nil
}
```

### 优化策略详解

##### CombineLimits
如果出现了2个Limit，则将2个Limit合并为一个，这个要求一个Limit是另一个Limit的grandChild。
例如```val query = sql("select * from (select * from table limit 100) limit 10 ") ```,子查询里limit100,外层查询limit10，这里我们当然可以在子查询里不必查那么多，因为外层只需要10个，所以这里会合并Limit10，和Limit100 为 Limit 10。

```
/**
 * Combines two adjacent [[Limit]] operators into one, merging the
 * expressions into one single expression.
 */
object CombineLimits extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case ll @ Limit(le, nl @ Limit(ne, grandChild)) =>
      Limit(If(LessThan(ne, le), ne, le), grandChild)
  }
}
```

##### NullPropagation
Literal字面量是一个能匹配任意基本类型的类。
```
object Literal {
  def apply(v: Any): Literal = v match {
    case i: Int => Literal(i, IntegerType)
    case l: Long => Literal(l, LongType)
    case d: Double => Literal(d, DoubleType)
    case f: Float => Literal(f, FloatType)
    case b: Byte => Literal(b, ByteType)
    case s: Short => Literal(s, ShortType)
    case s: String => Literal(s, StringType)
    case b: Boolean => Literal(b, BooleanType)
    case d: BigDecimal => Literal(Decimal(d), DecimalType.Unlimited)
    case d: Decimal => Literal(d, DecimalType.Unlimited)
    case t: Timestamp => Literal(t, TimestampType)
    case d: Date => Literal(d, DateType)
    case a: Array[Byte] => Literal(a, BinaryType)
    case null => Literal(null, NullType)
  }
}
```

注意Literal是一个LeafExpression，核心方法是eval，给定Row，计算表达式返回值：
```
case class Literal(value: Any, dataType: DataType) extends LeafExpression {

  override def foldable = true
  def nullable = value == null


  override def toString = if (value != null) value.toString else "null"

  type EvaluatedType = Any
  override def eval(input: Row):Any = value
}
```

NullPropagation是一个能将Expression Expressions替换为等价的Literal值的优化，并且能够避免NULL值在SQL语法树的传播。
```
/**
 * Replaces [[Expression Expressions]] that can be statically evaluated with
 * equivalent [[Literal]] values. This rule is more specific with
 * Null value propagation from bottom to top of the expression tree.
 */
object NullPropagation extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case q: LogicalPlan => q transformExpressionsUp {
      case e @ Count(Literal(null, _)) => Cast(Literal(0L), e.dataType)
      case e @ Sum(Literal(c, _)) if c == 0 => Cast(Literal(0L), e.dataType)
      case e @ Average(Literal(c, _)) if c == 0 => Literal(0.0, e.dataType)
      case e @ IsNull(c) if !c.nullable => Literal(false, BooleanType)
      case e @ IsNotNull(c) if !c.nullable => Literal(true, BooleanType)
      case e @ GetItem(Literal(null, _), _) => Literal(null, e.dataType)
      case e @ GetItem(_, Literal(null, _)) => Literal(null, e.dataType)
      case e @ GetField(Literal(null, _), _) => Literal(null, e.dataType)
      case e @ EqualNullSafe(Literal(null, _), r) => IsNull(r)
      case e @ EqualNullSafe(l, Literal(null, _)) => IsNull(l)

      // For Coalesce, remove null literals.
      case e @ Coalesce(children) =>
        val newChildren = children.filter {
          case Literal(null, _) => false
          case _ => true
        }
        if (newChildren.length == 0) {
          Literal(null, e.dataType)
        } else if (newChildren.length == 1) {
          newChildren(0)
        } else {
          Coalesce(newChildren)
        }

      case e @ Substring(Literal(null, _), _, _) => Literal(null, e.dataType)
      case e @ Substring(_, Literal(null, _), _) => Literal(null, e.dataType)
      case e @ Substring(_, _, Literal(null, _)) => Literal(null, e.dataType)

      // Put exceptional cases above if any
      case e: BinaryArithmetic => e.children match {
        case Literal(null, _) :: right :: Nil => Literal(null, e.dataType)
        case left :: Literal(null, _) :: Nil => Literal(null, e.dataType)
        case _ => e
      }
      case e: BinaryComparison => e.children match {
        case Literal(null, _) :: right :: Nil => Literal(null, e.dataType)
        case left :: Literal(null, _) :: Nil => Literal(null, e.dataType)
        case _ => e
      }
      case e: StringRegexExpression => e.children match {
        case Literal(null, _) :: right :: Nil => Literal(null, e.dataType)
        case left :: Literal(null, _) :: Nil => Literal(null, e.dataType)
        case _ => e
      }
      case e: StringComparison => e.children match {
        case Literal(null, _) :: right :: Nil => Literal(null, e.dataType)
        case left :: Literal(null, _) :: Nil => Literal(null, e.dataType)
        case _ => e
      }
    }
  }
}
```

##### ConstantFolding
常量合并是属于Expression优化的一种，对于可以直接计算的常量，不用放到物理执行里去生成对象来计算了，直接可以在计划里就计算出来：
```
/**
 * Replaces [[Expression Expressions]] that can be statically evaluated with
 * equivalent [[Literal]] values.
 */
object ConstantFolding extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case q: LogicalPlan => q transformExpressionsDown {
      // Skip redundant folding of literals. This rule is technically not necessary. Placing this
      // here avoids running the next rule for Literal values, which would create a new Literal
      // object and running eval unnecessarily.
      case l: Literal => l

      // Fold expressions that are foldable.
      case e if e.foldable => Literal(e.eval(null), e.dataType)

      // Fold "literal in (item1, item2, ..., literal, ...)" into true directly.
      case In(Literal(v, _), list) if list.exists {
          case Literal(candidate, _) if candidate == v => true
          case _ => false
        } => Literal(true, BooleanType)
    }
  }
}
```

##### LikeSimplification
简化Like字句，如果符合下面四种情况之一：
1. startsWith
2. endsWith
3. contains
4. equalTo

```
/**
 * Simplifies LIKE expressions that do not need full regular expressions to evaluate the condition.
 * For example, when the expression is just checking to see if a string starts with a given
 * pattern.
 */
object LikeSimplification extends Rule[LogicalPlan] {
  // if guards below protect from escapes on trailing %.
  // Cases like "something\%" are not optimized, but this does not affect correctness.
  val startsWith = "([^_%]+)%".r
  val endsWith = "%([^_%]+)".r
  val contains = "%([^_%]+)%".r
  val equalTo = "([^_%]*)".r

  def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
    case Like(l, Literal(startsWith(pattern), StringType)) if !pattern.endsWith("\\") =>
      StartsWith(l, Literal(pattern))
    case Like(l, Literal(endsWith(pattern), StringType)) =>
      EndsWith(l, Literal(pattern))
    case Like(l, Literal(contains(pattern), StringType)) if !pattern.endsWith("\\") =>
      Contains(l, Literal(pattern))
    case Like(l, Literal(equalTo(pattern), StringType)) =>
      EqualTo(l, Literal(pattern))
  }
}
```

##### BooleanSimplification
这个是对布尔表达式的优化，有点像java布尔表达式中的短路判断，不过这个写的倒是很优雅。看看布尔表达式2边能不能通过只计算1边，而省去计算另一边而提高效率，称为简化布尔表达式。
```
/**
 * Simplifies boolean expressions where the answer can be determined without evaluating both sides.
 * Note that this rule can eliminate expressions that might otherwise have been evaluated and thus
 * is only safe when evaluations of expressions does not result in side effects.
 */
object BooleanSimplification extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case q: LogicalPlan => q transformExpressionsUp {
      case and @ And(left, right) =>
        (left, right) match {
          case (Literal(true, BooleanType), r) => r
          case (l, Literal(true, BooleanType)) => l
          case (Literal(false, BooleanType), _) => Literal(false)
          case (_, Literal(false, BooleanType)) => Literal(false)
          case (_, _) => and
        }

      case or @ Or(left, right) =>
        (left, right) match {
          case (Literal(true, BooleanType), _) => Literal(true)
          case (_, Literal(true, BooleanType)) => Literal(true)
          case (Literal(false, BooleanType), r) => r
          case (l, Literal(false, BooleanType)) => l
          case (_, _) => or
        }

      case not @ Not(exp) =>
        exp match {
          case Literal(true, BooleanType) => Literal(false)
          case Literal(false, BooleanType) => Literal(true)
          case GreaterThan(l, r) => LessThanOrEqual(l, r)
          case GreaterThanOrEqual(l, r) => LessThan(l, r)
          case LessThan(l, r) => GreaterThanOrEqual(l, r)
          case LessThanOrEqual(l, r) => GreaterThan(l, r)
          case Not(e) => e
          case _ => not
        }

      // Turn "if (true) a else b" into "a", and if (false) a else b" into "b".
      case e @ If(Literal(v, _), trueValue, falseValue) => if (v == true) trueValue else falseValue
    }
  }
}
```

##### SimplifyFilters
如果filter总是返回true，则删除filter返回child，如果filter总是返回false，则返回empty的relation。

```
/**
 * Removes filters that can be evaluated trivially.  This is done either by eliding the filter for
 * cases where it will always evaluate to `true`, or substituting a dummy empty relation when the
 * filter will always evaluate to `false`.
 */
object SimplifyFilters extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    // If the filter condition always evaluate to true, remove the filter.
    case Filter(Literal(true, BooleanType), child) => child
    // If the filter condition always evaluate to null or false,
    // replace the input with an empty relation.
    case Filter(Literal(null, _), child) => LocalRelation(child.output, data = Seq.empty)
    case Filter(Literal(false, BooleanType), child) => LocalRelation(child.output, data = Seq.empty)
  }
}
```

##### SimplifyCasts
如果Cast的类型和实际类型一致，则去除没必要的cast。

```
/**
 * Removes [[Cast Casts]] that are unnecessary because the input is already the correct type.
 */
object SimplifyCasts extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
    case Cast(e, dataType) if e.dataType == dataType => e
  }
}
```

##### SimplifyCaseConversionExpressions
去除内层没必要的大小写转换，直接返回外层的转换。

```
/**
 * Removes the inner [[CaseConversionExpression]] that are unnecessary because
 * the inner conversion is overwritten by the outer one.
 */
object SimplifyCaseConversionExpressions extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case q: LogicalPlan => q transformExpressionsUp {
      case Upper(Upper(child)) => Upper(child)
      case Upper(Lower(child)) => Upper(child)
      case Lower(Upper(child)) => Lower(child)
      case Lower(Lower(child)) => Lower(child)
    }
  }
}
```

##### OptimizeIn
将In(value, Seq[Literal])的节点转换为等价的InSet(value, HashSet[Literal])，会快很多。

```
/**
 * Replaces [[In (value, seq[Literal])]] with optimized version[[InSet (value, HashSet[Literal])]]
 * which is much faster
 */
object OptimizeIn extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case q: LogicalPlan => q transformExpressionsDown {
      case In(v, list) if !list.exists(!_.isInstanceOf[Literal]) =>
          val hSet = list.map(e => e.eval(null))
          InSet(v, HashSet() ++ hSet)
    }
  }
}
```

##### DecimalAggregates
计算fixed-precision Decimal类型的sum和avg的时候，先把它转换为Long类型，做计算，最后在转化为Decimal，会比较快。

```
/**
 * Speeds up aggregates on fixed-precision decimals by executing them on unscaled Long values.
 *
 * This uses the same rules for increasing the precision and scale of the output as
 * [[org.apache.spark.sql.catalyst.analysis.HiveTypeCoercion.DecimalPrecision]].
 */
object DecimalAggregates extends Rule[LogicalPlan] {
  import Decimal.MAX_LONG_DIGITS

  /** Maximum number of decimal digits representable precisely in a Double */
  val MAX_DOUBLE_DIGITS = 15

  def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
    case Sum(e @ DecimalType.Expression(prec, scale)) if prec + 10 <= MAX_LONG_DIGITS =>
      MakeDecimal(Sum(UnscaledValue(e)), prec + 10, scale)

    case Average(e @ DecimalType.Expression(prec, scale)) if prec + 4 <= MAX_DOUBLE_DIGITS =>
      Cast(
        Divide(Average(UnscaledValue(e)), Literal(math.pow(10.0, scale), DoubleType)),
        DecimalType(prec + 4, scale + 4))
  }
}
```

##### UnionPushdown
把filter和project pushdown到union下面。

```
/**
  *  Pushes operations to either side of a Union.
  */
object UnionPushdown extends Rule[LogicalPlan] {

  /**
    *  Maps Attributes from the left side to the corresponding Attribute on the right side.
    */
  def buildRewrites(union: Union): AttributeMap[Attribute] = {
    assert(union.left.output.size == union.right.output.size)

    AttributeMap(union.left.output.zip(union.right.output))
  }

  /**
    *  Rewrites an expression so that it can be pushed to the right side of a Union operator.
    *  This method relies on the fact that the output attributes of a union are always equal
    *  to the left child's output.
    */
  def pushToRight[A <: Expression](e: A, rewrites: AttributeMap[Attribute]): A = {
    val result = e transform {
      case a: Attribute => rewrites(a)
    }

    // We must promise the compiler that we did not discard the names in the case of project
    // expressions.  This is safe since the only transformation is from Attribute => Attribute.
    result.asInstanceOf[A]
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    // Push down filter into union
    case Filter(condition, u @ Union(left, right)) =>
      val rewrites = buildRewrites(u)
      Union(
        Filter(condition, left),
        Filter(pushToRight(condition, rewrites), right))

    // Push down projection into union
    case Project(projectList, u @ Union(left, right)) =>
      val rewrites = buildRewrites(u)
      Union(
        Project(projectList, left),
        Project(projectList.map(pushToRight(_, rewrites)), right))
  }
}
```

##### CombineFilters
合并两个相邻的Filter,这个和上述Combine Limit差不多。合并2个节点，就可以减少树的深度从而减少重复执行过滤的代价。
```
/**
 * Combines two adjacent [[Filter]] operators into one, merging the
 * conditions into one conjunctive predicate.
 */
object CombineFilters extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case ff @ Filter(fc, nf @ Filter(nc, grandChild)) => Filter(And(nc, fc), grandChild)
  }
}
```

##### PushPredicateThroughProject
Predict push到project下面，如果predict不依赖于project。

```
/**
 * Pushes [[Filter]] operators through [[Project]] operators, in-lining any [[Alias Aliases]]
 * that were defined in the projection.
 *
 * This heuristic is valid assuming the expression evaluation cost is minimal.
 */
object PushPredicateThroughProject extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case filter @ Filter(condition, project @ Project(fields, grandChild)) =>
      val sourceAliases = fields.collect { case a @ Alias(c, _) =>
        (a.toAttribute: Attribute) -> c
      }.toMap
      project.copy(child = filter.copy(
        replaceAlias(condition, sourceAliases),
        grandChild))
  }

  def replaceAlias(condition: Expression, sourceAliases: Map[Attribute, Expression]): Expression = {
    condition transform {
      case a: AttributeReference => sourceAliases.getOrElse(a, a)
    }
  }
}
```

##### PushPredicateThroughJoin
Predict push到project下面，如果predict不依赖于join。

```
/**
 * Pushes down [[Filter]] operators where the `condition` can be
 * evaluated using only the attributes of the left or right side of a join.  Other
 * [[Filter]] conditions are moved into the `condition` of the [[Join]].
 *
 * And also Pushes down the join filter, where the `condition` can be evaluated using only the
 * attributes of the left or right side of sub query when applicable.
 *
 * Check https://cwiki.apache.org/confluence/display/Hive/OuterJoinBehavior for more details
 */
object PushPredicateThroughJoin extends Rule[LogicalPlan] with PredicateHelper {
  /**
   * Splits join condition expressions into three categories based on the attributes required
   * to evaluate them.
   * @return (canEvaluateInLeft, canEvaluateInRight, haveToEvaluateInBoth)
   */
  private def split(condition: Seq[Expression], left: LogicalPlan, right: LogicalPlan) = {
    val (leftEvaluateCondition, rest) =
        condition.partition(_.references subsetOf left.outputSet)
    val (rightEvaluateCondition, commonCondition) =
        rest.partition(_.references subsetOf right.outputSet)

    (leftEvaluateCondition, rightEvaluateCondition, commonCondition)
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    // push the where condition down into join filter
    case f @ Filter(filterCondition, Join(left, right, joinType, joinCondition)) =>
      val (leftFilterConditions, rightFilterConditions, commonFilterCondition) =
        split(splitConjunctivePredicates(filterCondition), left, right)

      joinType match {
        case Inner =>
          // push down the single side `where` condition into respective sides
          val newLeft = leftFilterConditions.
            reduceLeftOption(And).map(Filter(_, left)).getOrElse(left)
          val newRight = rightFilterConditions.
            reduceLeftOption(And).map(Filter(_, right)).getOrElse(right)
          val newJoinCond = (commonFilterCondition ++ joinCondition).reduceLeftOption(And)

          Join(newLeft, newRight, Inner, newJoinCond)
        case RightOuter =>
          // push down the right side only `where` condition
          val newLeft = left
          val newRight = rightFilterConditions.
            reduceLeftOption(And).map(Filter(_, right)).getOrElse(right)
          val newJoinCond = joinCondition
          val newJoin = Join(newLeft, newRight, RightOuter, newJoinCond)

          (leftFilterConditions ++ commonFilterCondition).
            reduceLeftOption(And).map(Filter(_, newJoin)).getOrElse(newJoin)
        case _ @ (LeftOuter | LeftSemi) =>
          // push down the left side only `where` condition
          val newLeft = leftFilterConditions.
            reduceLeftOption(And).map(Filter(_, left)).getOrElse(left)
          val newRight = right
          val newJoinCond = joinCondition
          val newJoin = Join(newLeft, newRight, joinType, newJoinCond)

          (rightFilterConditions ++ commonFilterCondition).
            reduceLeftOption(And).map(Filter(_, newJoin)).getOrElse(newJoin)
        case FullOuter => f // DO Nothing for Full Outer Join
      }

    // push down the join filter into sub query scanning if applicable
    case f @ Join(left, right, joinType, joinCondition) =>
      val (leftJoinConditions, rightJoinConditions, commonJoinCondition) =
        split(joinCondition.map(splitConjunctivePredicates).getOrElse(Nil), left, right)

      joinType match {
        case Inner =>
          // push down the single side only join filter for both sides sub queries
          val newLeft = leftJoinConditions.
            reduceLeftOption(And).map(Filter(_, left)).getOrElse(left)
          val newRight = rightJoinConditions.
            reduceLeftOption(And).map(Filter(_, right)).getOrElse(right)
          val newJoinCond = commonJoinCondition.reduceLeftOption(And)

          Join(newLeft, newRight, Inner, newJoinCond)
        case RightOuter =>
          // push down the left side only join filter for left side sub query
          val newLeft = leftJoinConditions.
            reduceLeftOption(And).map(Filter(_, left)).getOrElse(left)
          val newRight = right
          val newJoinCond = (rightJoinConditions ++ commonJoinCondition).reduceLeftOption(And)

          Join(newLeft, newRight, RightOuter, newJoinCond)
        case _ @ (LeftOuter | LeftSemi) =>
          // push down the right side only join filter for right sub query
          val newLeft = left
          val newRight = rightJoinConditions.
            reduceLeftOption(And).map(Filter(_, right)).getOrElse(right)
          val newJoinCond = (leftJoinConditions ++ commonJoinCondition).reduceLeftOption(And)

          Join(newLeft, newRight, joinType, newJoinCond)
        case FullOuter => f
      }
  }
}
```

##### ColumnPruning
列裁剪用的比较多，就是减少不必要select的某些列。列裁剪在3种地方可以用：
1. 在聚合操作中，可以做列裁剪
2. 在join操作中，左右孩子可以做列裁剪
3. 合并相邻的Project的列

```
/**
 * Attempts to eliminate the reading of unneeded columns from the query plan using the following
 * transformations:
 *
 *  - Inserting Projections beneath the following operators:
 *   - Aggregate
 *   - Project <- Join
 *   - LeftSemiJoin
 *  - Collapse adjacent projections, performing alias substitution.
 */
object ColumnPruning extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    // Eliminate attributes that are not needed to calculate the specified aggregates.
    case a @ Aggregate(_, _, child) if (child.outputSet -- a.references).nonEmpty =>
      a.copy(child = Project(a.references.toSeq, child))

    // Eliminate unneeded attributes from either side of a Join.
    case Project(projectList, Join(left, right, joinType, condition)) =>
      // Collect the list of all references required either above or to evaluate the condition.
      val allReferences: AttributeSet =
        AttributeSet(
          projectList.flatMap(_.references.iterator)) ++
          condition.map(_.references).getOrElse(AttributeSet(Seq.empty))

      /** Applies a projection only when the child is producing unnecessary attributes */
      def pruneJoinChild(c: LogicalPlan) = prunedChild(c, allReferences)

      Project(projectList, Join(pruneJoinChild(left), pruneJoinChild(right), joinType, condition))

    // Eliminate unneeded attributes from right side of a LeftSemiJoin.
    case Join(left, right, LeftSemi, condition) =>
      // Collect the list of all references required to evaluate the condition.
      val allReferences: AttributeSet =
        condition.map(_.references).getOrElse(AttributeSet(Seq.empty))

      Join(left, prunedChild(right, allReferences), LeftSemi, condition)

    // Combine adjacent Projects.
    case Project(projectList1, Project(projectList2, child)) =>
      // Create a map of Aliases to their values from the child projection.
      // e.g., 'SELECT ... FROM (SELECT a + b AS c, d ...)' produces Map(c -> Alias(a + b, c)).
      val aliasMap = projectList2.collect {
        case a @ Alias(e, _) => (a.toAttribute: Expression, a)
      }.toMap

      // Substitute any attributes that are produced by the child projection, so that we safely
      // eliminate it.
      // e.g., 'SELECT c + 1 FROM (SELECT a + b AS C ...' produces 'SELECT a + b + 1 ...'
      // TODO: Fix TransformBase to avoid the cast below.
      val substitutedProjection = projectList1.map(_.transform {
        case a if aliasMap.contains(a) => aliasMap(a)
      }).asInstanceOf[Seq[NamedExpression]]

      Project(substitutedProjection, child)

    // Eliminate no-op Projects
    case Project(projectList, child) if child.output == projectList => child
  }

  /** Applies a projection only when the child is producing unnecessary attributes */
  private def prunedChild(c: LogicalPlan, allReferences: AttributeSet) =
    if ((c.outputSet -- allReferences.filter(c.outputSet.contains)).nonEmpty) {
      Project(allReferences.filter(c.outputSet.contains).toSeq, c)
    } else {
      c
    }
}
```



