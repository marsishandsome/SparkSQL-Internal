CREATE DATABASE SALEDATA;
use SALEDATA;

//Date.txt文件定义了日期的分类，将每天分别赋予所属的月份、星期、季度等属性
//日期，年月，年，月，日，周几，第几周，季度，旬、半月

CREATE TABLE tblDate(
dateID string,
theyearmonth string,
theyear string,
themonth string,
thedate string,
theweek string,
theweeks string,
thequot string,
thetenday string,
thehalfmonth string
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' stored as textfile;

//Stock.txt文件定义了订单表头
//订单号，交易位置，交易日期
CREATE TABLE tblStock(
ordernumber string,
locationid string,
dateID string
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' stored as textfile;

//StockDetail.txt文件定义了订单明细
//订单号，行号，货品，数量，金额
CREATE TABLE tblStockDetail(
ordernumber STRING,
rownum int,
itemid string,
qty int,
price int,
amount int
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' stored as textfile;

//装载数据

LOAD DATA LOCAL INPATH '/home/guliangliang/sparksql/Date.txt' INTO TABLE tblDate;

LOAD DATA LOCAL INPATH '/home/guliangliang/sparksql/Stock.txt' INTO TABLE tblStock;

LOAD DATA LOCAL INPATH '/home/guliangliang/sparksql/StockDetail.txt' INTO TABLE
tblStockDetail;


