# HiveContext的使用

使用hiveContext之前首先确认两点：

1. 使用的Spark是支持hive的，可以通过查看lib目录下有没有datanucleus-api-jdo-\*.jar,datanucleus-core-\*.jar,datanucleus-rdbms-\*.jar这三个文件
2. hive的配置文件hive-site.xml已经复制到spark的conf目录中

### 使用HiveContext
使用HiveContext首先要构建HiveContext

```
import org.apache.spark.sql.hive.HiveContext
val hiveContext = new HiveContext(sc)
```

然后就可以对hive数据进行操作了，下面我们将使用hive数据仓库里面的数据，首先切换数据库到spark_test，并查看里面的表格

```
hiveContext.sql("use spark_test")
hiveContext.sql("show tables").collect().foreach(println)
```

结果是

```
[tbldate]
[tblstock]
[tblstockdetail]
```

然后查询一下所有订单中每年的销售单数、销售总额：

```
/* 所有订单中每年的销售单数、销售总额
三个表连接后以count(distinct a.ordernumber)
计销售单数，sum(b.amount)计销售总额 */

hiveContext.sql("""select c.theyear,count(distinct a.ordernumber), sum(b.amount)
    from tblStock a
    join tblStockDetail b on a.ordernumber=b.ordernumber
    join tbldate c on a.dateid=c.dateid
    group by c.theyear order by c.theyear""").
    collect().foreach(println)
```

结果是
```
[2004,1094,3265696]
[2005,3828,13247234]
[2006,3772,13670416]
[2007,4885,16711974]
[2008,4861,14670698]
[2009,2619,6322137]
[2010,94,210924]
```



