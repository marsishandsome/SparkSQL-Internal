# Analyser

Analyzer的主要职责是将Sql Parser生成的Unresolved Logical Plan转化成Resolved Logical Plan。Analyzer会利用Catalog和FunctionRegistry里面注册的表格和用户定义的函数，将UnresolvedAttribute和UnresolvedRelation转换为Catalyst里全类型的对象。

在介绍Analyzer之前先介绍一下Catalog和FunctionRegistry这两个模块。

### Catalog
Catalog里面记录了Table Name到Logical Plan的映射，提供了注册表格，查找表格等接口。

```
/**
 * An interface for looking up relations by name.  Used by an [[Analyzer]].
 */
trait Catalog {

  def caseSensitive: Boolean

  def tableExists(db: Option[String], tableName: String): Boolean

  def lookupRelation(
    databaseName: Option[String],
    tableName: String,
    alias: Option[String] = None): LogicalPlan

  def registerTable(databaseName: Option[String], tableName: String, plan: LogicalPlan): Unit

  def unregisterTable(databaseName: Option[String], tableName: String): Unit

  def unregisterAllTables(): Unit
  ...
}
```

Catalog具体的实现是SimpleCatalog，里面是用HashMap来记录Table Name到Logical Plan的映射。
```
class SimpleCatalog(val caseSensitive: Boolean) extends Catalog {
  val tables = new mutable.HashMap[String, LogicalPlan]()

  override def registerTable(
      databaseName: Option[String],
      tableName: String,
      plan: LogicalPlan): Unit = {
    val (dbName, tblName) = processDatabaseAndTableName(databaseName, tableName)
    tables += ((tblName, plan))
  }

  override def unregisterTable(
      databaseName: Option[String],
      tableName: String) = {
    val (dbName, tblName) = processDatabaseAndTableName(databaseName, tableName)
    tables -= tblName
  }

  override def unregisterAllTables() = {
    tables.clear()
  }

  override def tableExists(db: Option[String], tableName: String): Boolean = {
    val (dbName, tblName) = processDatabaseAndTableName(db, tableName)
    tables.get(tblName) match {
      case Some(_) => true
      case None => false
    }
  }

  override def lookupRelation(
      databaseName: Option[String],
      tableName: String,
      alias: Option[String] = None): LogicalPlan = {
    val (dbName, tblName) = processDatabaseAndTableName(databaseName, tableName)
    val table = tables.getOrElse(tblName, sys.error(s"Table Not Found: $tableName"))
    val tableWithQualifiers = Subquery(tblName, table)

    // If an alias was specified by the lookup, wrap the plan in a subquery so that attributes are
    // properly qualified with this alias.
    alias.map(a => Subquery(a, tableWithQualifiers)).getOrElse(tableWithQualifiers)
  }
}
```


### FunctionRegistry
FunctionRegistry记录了用户定义的函数名到该函数的表达式的映射，并提供注册函数，查找函数等接口。
FunctionBuilder被定义成为 ```Seq[Expression] => Expression```，可以理解为输入多个Expression作为参数，输出一个Expression作为结果。

```
/** A catalog for looking up user defined functions, used by an [[Analyzer]]. */
trait FunctionRegistry {
  type FunctionBuilder = Seq[Expression] => Expression

  def registerFunction(name: String, builder: FunctionBuilder): Unit

  def lookupFunction(name: String, children: Seq[Expression]): Expression
}
```

FunctionRegistry具体的实现是SimpleFunctionRegistry，里面用HashMap来记录用户定义的函数名到该函数的表达式的映射。

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


### Analyzer
Analyzer里面有一个fixedPoint对象，一个Seq[Batch]对象。

```
/**
 * Provides a logical query plan analyzer, which translates [[UnresolvedAttribute]]s and
 * [[UnresolvedRelation]]s into fully typed objects using information in a schema [[Catalog]] and
 * a [[FunctionRegistry]].
 */
class Analyzer(catalog: Catalog, registry: FunctionRegistry, caseSensitive: Boolean)
  extends RuleExecutor[LogicalPlan] with HiveTypeCoercion {

  val resolver = if (caseSensitive) caseSensitiveResolution else caseInsensitiveResolution

  // TODO: pass this in as a parameter.
  val fixedPoint = FixedPoint(100)

  /**
   * Override to provide additional rules for the "Resolution" batch.
   */
  val extendedRules: Seq[Rule[LogicalPlan]] = Nil

  lazy val batches: Seq[Batch] = Seq(
    Batch("MultiInstanceRelations", Once,
      NewRelationInstances),
    Batch("Resolution", fixedPoint,
      ResolveReferences ::
      ResolveRelations ::
      ResolveSortReferences ::
      NewRelationInstances ::
      ImplicitGenerate ::
      StarExpansion ::
      ResolveFunctions ::
      GlobalAggregates ::
      UnresolvedHavingClauseAttributes ::
      TrimGroupingAliases ::
      typeCoercionRules ++
      extendedRules : _*),
    Batch("Check Analysis", Once,
      CheckResolution,
      CheckAggregation),
    Batch("AnalysisOperators", fixedPoint,
      EliminateAnalysisOperators)
  )
  ...
}
```

Analyzer解析主要是根据这个batches里面的各种Rule来对Unresolved Logical Plan进行解析的。这里Analyzer类本身并没有定义执行的方法，而实现在它的父类RuleExecutor[LogicalPlan]中。

### Rules介绍
在batches里面定义了4个Batch:
1. MultiInstanceRelations (Once)
2. Resolution (fixedPoint)
3. Check Analysis (Once)
4. AnalysisOperators (fixedPoint)

不同的batch是顺序执行的，也就是说MultiInstanceRelations执行完了，才会执行Resolution。

Once表示MultiInstanceRelations的Rule只会执行一次，fixedPoint表示Resolution里面的Rule会反复执行多次，具体几次定义在FixedPoint里面（当然如果运行Rule前后的LogicalPlan没有变化，也会提前停止执行）。

Resolution这个Batch里面定义了十几个Rules，例如ResolveReferences，ResolveRelations，ResolveSortReferences,etc。这些不同的Rule会循环执行fixedPoint次，执行的顺序是依次执行，也就是说，ResolveReferences -> ResolveRelations -> ResolveSortReferences -> ResolveReferences -> ResolveRelations -> ResolveSortReferences -> ...，类似这个顺序。

下面介绍一下每一个Rule具体做什么事情。

##### MultiInstanceRelation
如果一个实例在Logical Plan里出现了多次，则会应用NewRelationInstances这条Rule。
```
Batch("MultiInstanceRelations", Once,
      NewRelationInstances),
```

```
/**
 * A trait that should be mixed into query operators where an single instance might appear multiple
 * times in a logical query plan.  It is invalid to have multiple copies of the same attribute
 * produced by distinct operators in a query tree as this breaks the guarantee that expression
 * ids, which are used to differentiate attributes, are unique.
 *
 * Before analysis, all operators that include this trait will be asked to produce a new version
 * of itself with globally unique expression ids.
 */
trait MultiInstanceRelation {
  def newInstance(): this.type
}
```

```
/**
 * If any MultiInstanceRelation appears more than once in the query plan then the plan is updated so
 * that each instance has unique expression ids for the attributes produced.
 */
object NewRelationInstances extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = {
    ////将logical plan应用partial function得到所有MultiInstanceRelation的plan的集合
    val localRelations = plan collect { case l: MultiInstanceRelation => l}

    val multiAppearance = localRelations
      .groupBy(identity[MultiInstanceRelation]) //group by操作
      .filter { case (_, ls) => ls.size > 1 } //如果只取size大于1的进行后续操作
      .map(_._1)
      .toSet

    //更新plan，使得每个实例的expId是唯一的。
    plan transform {
      case l: MultiInstanceRelation if multiAppearance contains l => l.newInstance
    }
  }
}
```

##### ResolveReferences
将Sql parser解析出来的UnresolvedAttribute全部都转为对应的实际的catalyst.expressions.AttributeReference。这里调用了Logical Plan的resolveChildren方法，将属性转为NamedExepression。
```
/**
   * Replaces [[UnresolvedAttribute]]s with concrete
   * [[catalyst.expressions.AttributeReference AttributeReferences]] from a logical plan node's
   * children.
   */
  object ResolveReferences extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
      case q: LogicalPlan if q.childrenResolved =>
        logTrace(s"Attempting to resolve ${q.simpleString}")
        q transformExpressions {
          case u @ UnresolvedAttribute(name) =>
            // Leave unchanged if resolution fails.  Hopefully will be resolved next round.
            val result = q.resolveChildren(name, resolver).getOrElse(u)
            logDebug(s"Resolving $u to $result")
            result
        }
    }
  }
```


##### ResolveRelations
在```select * from src```中，src表parse后就是一个UnresolvedRelation节点。ResolveRelations就是把src替换成具体的LogicalPlan。而这个table name到LogicalPlan的映射可以从Catalog里获得。

```
/**
   * Replaces [[UnresolvedRelation]]s with concrete relations from the catalog.
   */
  object ResolveRelations extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transform {
      case i @ InsertIntoTable(UnresolvedRelation(databaseName, name, alias), _, _, _) =>
        i.copy(
          table = EliminateAnalysisOperators(catalog.lookupRelation(databaseName, name, alias)))
      case UnresolvedRelation(databaseName, name, alias) =>
        catalog.lookupRelation(databaseName, name, alias)
    }
  }
```

```
def lookupRelation(
    databaseName: Option[String],
    tableName: String,
    alias: Option[String] = None): LogicalPlan
```


##### ResolveSortReferences
在某些SQL的定义里面，可以允许按照没有出现在select里面的attribute进行sort。这个规则是用来检测这些语法，并且自动把sort的attribute加入到select里面，并且在上次加入去到这个attribute的projection。
```
/**
   * In many dialects of SQL is it valid to sort by attributes that are not present in the SELECT
   * clause.  This rule detects such queries and adds the required attributes to the original
   * projection, so that they will be available during sorting. Another projection is added to
   * remove these attributes after sorting.
   */
  object ResolveSortReferences extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
      case s @ Sort(ordering, p @ Project(projectList, child)) if !s.resolved && p.resolved =>
        val unresolved = ordering.flatMap(_.collect { case UnresolvedAttribute(name) => name })
        val resolved = unresolved.flatMap(child.resolve(_, resolver))
        val requiredAttributes = AttributeSet(resolved.collect { case a: Attribute => a })

        val missingInProject = requiredAttributes -- p.output
        if (missingInProject.nonEmpty) {
          // Add missing attributes and then project them away after the sort.
          Project(projectList.map(_.toAttribute),
            Sort(ordering,
              Project(projectList ++ missingInProject, child)))
        } else {
          logDebug(s"Failed to find $missingInProject in ${p.output.mkString(", ")}")
          s // Nothing we can do here. Return original plan.
        }
      case s @ Sort(ordering, a @ Aggregate(grouping, aggs, child)) if !s.resolved && a.resolved =>
        val unresolved = ordering.flatMap(_.collect { case UnresolvedAttribute(name) => name })
        // A small hack to create an object that will allow us to resolve any references that
        // refer to named expressions that are present in the grouping expressions.
        val groupingRelation = LocalRelation(
          grouping.collect { case ne: NamedExpression => ne.toAttribute }
        )

        logDebug(s"Grouping expressions: $groupingRelation")
        val resolved = unresolved.flatMap(groupingRelation.resolve(_, resolver))
        val missingInAggs = resolved.filterNot(a.outputSet.contains)
        logDebug(s"Resolved: $resolved Missing in aggs: $missingInAggs")
        if (missingInAggs.nonEmpty) {
          // Add missing grouping exprs and then project them away after the sort.
          Project(a.output,
            Sort(ordering,
              Aggregate(grouping, aggs ++ missingInAggs, child)))
        } else {
          s // Nothing we can do here. Return original plan.
        }
    }
  }
```

##### ImplicitGenerate
如果在select语句里只有一个表达式，而且这个表达式是一个Generator（Generator是一个1条记录生成到N条记录的映射）。当在解析逻辑计划时，遇到Project节点的时候，就可以将它转换为Generate类（Generate类是将输入流应用一个函数，从而生成一个新的流）。
```
/**
   * When a SELECT clause has only a single expression and that expression is a
   * [[catalyst.expressions.Generator Generator]] we convert the
   * [[catalyst.plans.logical.Project Project]] to a [[catalyst.plans.logical.Generate Generate]].
   */
  object ImplicitGenerate extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transform {
      case Project(Seq(Alias(g: Generator, _)), child) =>
        Generate(g, join = false, outer = false, None, child)
    }
  }
```

##### StarExpansion
在Project操作符里，如果是\*符号，可以将所有的references都展开成实际的字段。
```
/**
   * Expands any references to [[Star]] (*) in project operators.
   */
  object StarExpansion extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transform {
      // Wait until children are resolved
      case p: LogicalPlan if !p.childrenResolved => p
      // If the projection list contains Stars, expand it.
      case p @ Project(projectList, child) if containsStar(projectList) =>
        Project(
          projectList.flatMap {
            case s: Star => s.expand(child.output, resolver)
            case o => o :: Nil
          },
          child)
      case t: ScriptTransformation if containsStar(t.input) =>
        t.copy(
          input = t.input.flatMap {
            case s: Star => s.expand(t.child.output, resolver)
            case o => o :: Nil
          }
        )
      // If the aggregate function argument contains Stars, expand it.
      case a: Aggregate if containsStar(a.aggregateExpressions) =>
        a.copy(
          aggregateExpressions = a.aggregateExpressions.flatMap {
            case s: Star => s.expand(a.child.output, resolver)
            case o => o :: Nil
          }
        )
    }

    /**
     * Returns true if `exprs` contains a [[Star]].
     */
    protected def containsStar(exprs: Seq[Expression]): Boolean =
      exprs.collect { case _: Star => true }.nonEmpty
  }
}
```

##### ResolveFunctions
这里主要是对UDF进行resolve，将这些UDF可以从FunctionRegistry里找到。
```
/**
   * Replaces [[UnresolvedFunction]]s with concrete [[catalyst.expressions.Expression Expressions]].
   */
  object ResolveFunctions extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transform {
      case q: LogicalPlan =>
        q transformExpressions {
          case u @ UnresolvedFunction(name, children) if u.childrenResolved =>
            registry.lookupFunction(name, children)
        }
    }
  }
```

##### GlobalAggregates
如果遇到包含Aggregate的Project，就返回一个Aggregate。
```
/**
   * Turns projections that contain aggregate expressions into aggregations.
   */
  object GlobalAggregates extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transform {
      case Project(projectList, child) if containsAggregates(projectList) =>
        Aggregate(Nil, projectList, child)
    }

    def containsAggregates(exprs: Seq[Expression]): Boolean = {
      exprs.foreach(_.foreach {
        case agg: AggregateExpression => return true
        case _ =>
      })
      false
    }
  }
```

##### UnresolvedHavingClauseAttributes
这条规则会寻找Having子句中Unresolved Attributes，将这些Attributes下降到下面的Aggregates里面，最后在最上面添加Project。
```
/**
   * This rule finds expressions in HAVING clause filters that depend on
   * unresolved attributes.  It pushes these expressions down to the underlying
   * aggregates and then projects them away above the filter.
   */
  object UnresolvedHavingClauseAttributes extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
      case filter @ Filter(havingCondition, aggregate @ Aggregate(_, originalAggExprs, _))
          if aggregate.resolved && containsAggregate(havingCondition) => {
        val evaluatedCondition = Alias(havingCondition,  "havingCondition")()
        val aggExprsWithHaving = evaluatedCondition +: originalAggExprs

        Project(aggregate.output,
          Filter(evaluatedCondition.toAttribute,
            aggregate.copy(aggregateExpressions = aggExprsWithHaving)))
      }
    }

    protected def containsAggregate(condition: Expression): Boolean =
      condition
        .collect { case ae: AggregateExpression => ae }
        .nonEmpty
  }
```

##### TrimGroupingAliases
去除Aggreate中没有操作的alias。
```
/**
   * Removes no-op Alias expressions from the plan.
   */
  object TrimGroupingAliases extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transform {
      case Aggregate(groups, aggs, child) =>
        Aggregate(groups.map(_.transform { case Alias(c, _) => c }), aggs, child)
    }
  }
```


##### CheckResolution
在上述主要的优化规则都运行完后，CheckResolution会运行一次，用来检查是不是所有的节点都已经resolved了，如果不是就抛异常。
```
/**
   * Makes sure all attributes and logical plans have been resolved.
   */
  object CheckResolution extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = {
      plan.transform {
        case p if p.expressions.exists(!_.resolved) =>
          throw new TreeNodeException(p,
            s"Unresolved attributes: ${p.expressions.filterNot(_.resolved).mkString(",")}")
        case p if !p.resolved && p.childrenResolved =>
          throw new TreeNodeException(p, "Unresolved plan found")
      } match {
        // As a backstop, use the root node to check that the entire plan tree is resolved.
        case p if !p.resolved =>
          throw new TreeNodeException(p, "Unresolved plan in tree")
        case p => p
      }
    }
  }
```

##### CheckAggregation
在上述主要的优化规则都运行完后，CheckAggregation也会运行一次，用于检查是否存在non-aggregated attributes，如果不是就抛异常。
```
/**
   * Checks for non-aggregated attributes with aggregation
   */
  object CheckAggregation extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = {
      plan.transform {
        case aggregatePlan @ Aggregate(groupingExprs, aggregateExprs, child) =>
          def isValidAggregateExpression(expr: Expression): Boolean = expr match {
            case _: AggregateExpression => true
            case e: Attribute => groupingExprs.contains(e)
            case e if groupingExprs.contains(e) => true
            case e if e.references.isEmpty => true
            case e => e.children.forall(isValidAggregateExpression)
          }

          aggregateExprs.find { e =>
            !isValidAggregateExpression(e.transform {
              // Should trim aliases around `GetField`s. These aliases are introduced while
              // resolving struct field accesses, because `GetField` is not a `NamedExpression`.
              // (Should we just turn `GetField` into a `NamedExpression`?)
              case Alias(g: GetField, _) => g
            })
          }.foreach { e =>
            throw new TreeNodeException(plan, s"Expression not in GROUP BY: $e")
          }

          aggregatePlan
      }
    }
  }
```


##### EliminateAnalysisOperators
将Subquery移除。
```
/**
 * Removes [[catalyst.plans.logical.Subquery Subquery]] operators from the plan.  Subqueries are
 * only required to provide scoping information for attributes and can be removed once analysis is
 * complete.
 */
object EliminateAnalysisOperators extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case Subquery(_, child) => child
  }
}
```


