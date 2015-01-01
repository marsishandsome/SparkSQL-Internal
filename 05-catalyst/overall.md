# 总结

在log4j.properies里面设置```log4j.rootCategory=TRACE, console```可以将Catalyst详细的优化过程打印到Console中。


```
sql( s"""
        |SELECT name
        |FROM (SELECT name, age FROM rddTable) p
        |WHERE p.age >= 13 AND p.age <= 19
        |""".stripMargin).queryExecution
```
拿上面这句sql查询为例，它会经过一下几个优化过程。

**1: Analyzer阶段 (Batch Resolution)**

```
=== Applying Rule org.apache.spark.sql.catalyst.analysis.Analyzer$ResolveRelations ===
 'Project ['name]                                'Project ['name]
  'Filter (('p.age. >= 13) && ('p.age. <= 19))    'Filter (('p.age. >= 13) && ('p.age. <= 19))
   'Subquery p                                     'Subquery p
    'Project ['name,'age]                           'Project ['name,'age]
!    'UnresolvedRelation None, rddTable, None        Subquery rddTable
!                                                     LogicalRDD [name#0,age#1], MapPartitionsRDD[4] at mapPartitions at ExistingRDD.scala:36

=== Applying Rule org.apache.spark.sql.catalyst.analysis.Analyzer$ResolveReferences ===
!'Project ['name]                                                                               Project [name#0]
! 'Filter (('p.age. >= 13) && ('p.age. <= 19))                                                   Filter ((age#1 >= 13) && (age#1 <= 19))
!  'Subquery p                                                                                    Subquery p
!   'Project ['name,'age]                                                                          Project [name#0,age#1]
     Subquery rddTable                                                                              Subquery rddTable
      LogicalRDD [name#0,age#1], MapPartitionsRDD[4] at mapPartitions at ExistingRDD.scala:36        LogicalRDD [name#0,age#1], MapPartitionsRDD[4] at mapPartitions at ExistingRDD.scala:36

=== Result of Batch Resolution ===
!'Project ['name]                                Project [name#0]
! 'Filter (('p.age. >= 13) && ('p.age. <= 19))    Filter ((age#1 >= 13) && (age#1 <= 19))
!  'Subquery p                                     Subquery p
!   'Project ['name,'age]                           Project [name#0,age#1]
!    'UnresolvedRelation None, rddTable, None        Subquery rddTable
!                                                     LogicalRDD [name#0,age#1], MapPartitionsRDD[4] at mapPartitions at ExistingRDD.scala:36
```

**2: Analyzer阶段 (Batch AnalysisOperators)**

```
=== Applying Rule org.apache.spark.sql.catalyst.analysis.EliminateAnalysisOperators ===
 Project [name#0]                                                                               Project [name#0]
  Filter ((age#1 >= 13) && (age#1 <= 19))                                                        Filter ((age#1 >= 13) && (age#1 <= 19))
!  Subquery p                                                                                     Project [name#0,age#1]
!   Project [name#0,age#1]                                                                         LogicalRDD [name#0,age#1], MapPartitionsRDD[4] at mapPartitions at ExistingRDD.scala:36
!    Subquery rddTable
!     LogicalRDD [name#0,age#1], MapPartitionsRDD[4]at mapPartitions at ExistingRDD.scala:36

=== Result of Batch AnalysisOperators ===
!'Project ['name]                                Project [name#0]
! 'Filter (('p.age. >= 13) && ('p.age. <= 19))    Filter ((age#1 >= 13) && (age#1 <= 19))
!  'Subquery p                                     Project [name#0,age#1]
!   'Project ['name,'age]                           LogicalRDD [name#0,age#1], MapPartitionsRDD[4] at mapPartitions at ExistingRDD.scala:36
!    'UnresolvedRelation None, rddTable, None
```

**3: Optimizer阶段 (Batch Filter Pushdown)**

```
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.PushPredicateThroughProject ===
 Project [name#0]                                                                             Project [name#0]
! Filter ((age#1 >= 13) && (age#1 <= 19))                                                      Project [name#0,age#1]
!  Project [name#0,age#1]                                                                       Filter ((age#1 >= 13) && (age#1 <= 19))
    LogicalRDD [name#0,age#1], MapPartitionsRDD[4] at mapPartitions at ExistingRDD.scala:36      LogicalRDD [name#0,age#1], MapPartitionsRDD[4] at mapPartitions at ExistingRDD.scala:36

=== Applying Rule org.apache.spark.sql.catalyst.optimizer.ColumnPruning ===
 Project [name#0]                                                                             Project [name#0]
! Project [name#0,age#1]                                                                       Filter ((age#1 >= 13) && (age#1 <= 19))
!  Filter ((age#1 >= 13) && (age#1 <= 19))                                                      LogicalRDD [name#0,age#1], MapPartitionsRDD[4] at mapPartitions at ExistingRDD.scala:36
!   LogicalRDD [name#0,age#1], MapPartitionsRDD[4] at mapPartitions at ExistingRDD.scala:36

=== Result of Batch Filter Pushdown ===
 Project [name#0]                                                                             Project [name#0]
  Filter ((age#1 >= 13) && (age#1 <= 19))                                                      Filter ((age#1 >= 13) && (age#1 <= 19))
!  Project [name#0,age#1]                                                                       LogicalRDD [name#0,age#1], MapPartitionsRDD[4] at mapPartitions at ExistingRDD.scala:36
!   LogicalRDD [name#0,age#1], MapPartitionsRDD[4] at mapPartitions at ExistingRDD.scala:36
```


