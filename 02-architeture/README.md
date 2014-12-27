# SparkSQL的运行架构

在传统关系型数据库当中，一个简单的```select attribute from table where condition```语句，将会经过Parser生成Logical Plan，一般是一颗Tree，Optimizer会在Tree上进行优化并生成Physical Plan，交给执行器去执行，最总得到结果。

SparkSQL也采用了类似的方式进行处理，下面我们先来看看SparkSQL中的两个重要概念Tree和Rule，然后结束一下两个分支SqlContext和HiveContext，最后来看看优化器Catalyst。

