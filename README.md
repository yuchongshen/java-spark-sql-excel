# java-spark-sql-excel
spark-sql query excel data program in java

&emsp;eclipse下用java调用非集群Spark-sql查询excel，支持xlsx格式和xls格式，支持多Sheet查询
## Spark官网下载Spark
&emsp;<a  href ="http://spark.apache.org/downloads.html">Spark下载</a>   版本随意
![!\[spark下载](https://img-blog.csdnimg.cn/2019011616472791.png)
&emsp;下载后解压放入bigdata下(目录可以更改)
## 下载Windows下Hadoop所需文件winutils.exe
&emsp;同学们自己网上找找吧，这里就不上传了，其实该文件可有可无，报错也不影响Spark运行，强迫症可以下载，本人就有强迫症~~,文件下载后放入bigdata\hadoop\bin目录下。
&emsp;不用创建环境变量，再Java最开始出定义系统变量即可
```java
		System.setProperty("hadoop.home.dir", HADOOP_HOME);
```
### 创建Java Maven项目java-spark-sql-excel
#### &emsp;建立相关目录层次如下：
&emsp;父级目录(项目所在目录)<br/>
&emsp;&emsp;- java-spark-sql-excel<br/>
&emsp;&emsp;- bigdata<br/>
&emsp;&emsp;&emsp;- spark<br/>
&emsp;&emsp;&emsp;- hadoop<br/>
&emsp;&emsp;&emsp;&emsp;- bin<br/>
&emsp;&emsp;&emsp;&emsp;&emsp;- winutils.exe<br/>

##### &emsp;&emsp;运行结果
![在这里插入图片描述](https://img-blog.csdnimg.cn/20190116170316632.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3FxXzIwNjc3NjM1,size_16,color_FFFFFF,t_70)

