# Catalyst优化器
Catalyst是SparkSQL的优化器，它是一款基于规则的优化器。简单来说，SparkSQL中定义了一系列优化规则，Catalyst根据这些规则可以对执行计划进行优化。Catalyst在SparkSQL中几乎被用在了物理执行计划生成的每个阶段，包括解析、绑定、优化、物理计划等，于其说是优化器，还不如说是查询引擎。

对于下面这个sql查询
```
select name from(
    select id, name from people
    ) p
    where p.id = 1
```
SparkSQL首先会生成未经优化的计划，Catalyst里面定义了一些规则。在优化的过程当中，Catalyst会检测输入的执行计划当中有没有符合条件的子树，如果有的话就会触发某个特定的优化规则。如下图，Catalyst依此触发了Filter Push Down和Combine Projection，最后生成优化过的物理执行计划。

![](/images/catalyst2.png)

相对于传统的数据库优化引擎，Catalyst的优势在于其可插拔的设计。下面是Catalyst的一个设计图：

![](/images/catalyst.png)

从上图看，catalyst主要的实现组件有：
1. sqlParse，完成sql语句的语法解析功能，目前只提供了一个简单的sql解析器；
2. Analyzer，主要完成绑定工作，将不同来源的Unresolved LogicalPlan和数据元数据（如hive metastore、Schema catalog）进行绑定，生成resolved LogicalPlan；
3. optimizer对resolved LogicalPlan进行优化，生成optimized LogicalPlan；
4. Planner将LogicalPlan转换成PhysicalPlan；
5. CostModel，主要根据过去的性能统计数据，选择最佳的物理执行计划




