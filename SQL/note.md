pyspark 連線 mySQL server 
下載java驅動程式放入spark/jars 裡(每個叢機都要)：

https://mvnrepository.com/artifact/mysql/mysql-connector-java/8.0.20

```
# 輸入參數檔啟動
pyspark --master spark://master:7077 --jars {驅動程式位置}

# 建立連線參數
prop = {'user': 'user',
       'password': '1234',
       'driver': 'com.mysql.cj.jdbc.Driver'}

# 資料庫連線位置(?以後的一長串是設定時區與utf8)
url = 'jdbc:mysql://<server_hostIP>:<port>/<database_name>?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC'

# 實例化連線的表格
df = spark.read.jdbc(url=url, table='table_name', properties=prop)
```

宣告SQL類型實例
```
spark = SparkSession.builder.getOrCreate()
```

創建dataframe
```
# 讀取現有檔案為dataframe
df = spark.read.csv("hdfs://{PATH}/{data}.csv", header=True, inferSchema=True)
df = spark.read.json("hdfs://{PATH}{data}.json", schema="col_name col_type")

# Row-Based: Csv, Json, Avro, Sequencefile 一次要讀取一整個ROW
# Column-Based: Parquetfile, ROC, RC 可讀取指定欄位(佔記憶體空間較小，大型資料較常用)

# 使用parallelize 創建
data = sc.parallelize([("Michel",29),("Andy",30)])
schema = "name string, age int"
df = spark.createDataFrame(data, schema)

# parallelize 轉化
data = [('2345', 'Checked by John'),
        ('2398', 'Verified by Stacy'),]
df = sc.parallelize(data).toDF(['ID', 'Notes'])
```

alias更改欄位名稱(非創造欄位，但可以賦予變數)
```
df.withColumnRenamed("column", "new_column")   
df.select(df.symbol, (df.volume/10000).alias('123'))
df.select(df['column']).alias("new_column").show()

# 兩段式更名
df_as1 = df.alias("df_as1")
df_as2 = df.alias("df_as2")
joined_df = df_as1.join(df_as2, col("df_as1.name") == col("df_as2.name"), 'inner')
joined_df.select("df_as1.name", "df_as2.name", "df_as2.age")
```

新增去除欄位
```
# 使用表格合併的方式
unknown_list = [[‘0’, ‘Unknown’]]
unknown_df = spark.createDataFrame(unknown_list)
df = df.union(unknown_df)

# 使用withcolumn同時新增與更名
df.withColumn("new column", df["int_type_column1"] - df["int_type_column2"]).show()

# translate() 可以用於更改文字類欄位的值(col, old_v, new_v)
df = df.withColumn('d_id', translate('symbol', 'AAPL', '0'))  

# 移除欄位
df.drop("column")
```

df.select = select {column} from {table} 
```
# 計算unique指定欄位數
df.select('symbol').distinct().count()
df.agg(countDistinct("column")).show()

# 配合正規表達式查詢( column, regex, index )
# index 為需要擷取的範圍，0表示擷取的全部，0+為第幾個( )。
df.select(regexp_extract("column", r'.by\s+(\w+)', 0)).show() 
df.select(regexp_replace("column", r'(.)(by)(\s+)(\w+)', 4)).show()

# when & between (需要從funtions import *)
df.filter(df.column.between(50, 100))
df.select('column', when(df.open > 50, 1).when(df.close < 50, -1).otherwise(0))
```

df.filter = where  {column}  
```
# 原始用法
df.filter(df["column"] == "something")
df.filter((df["column1"] <= 200) & ~(df["column2"] > 30))

# 應用類pandas的方式取用
df[(df['symbol'] == "AAPL") & (df['open']>100)]

# 直接當作where子句使用，內部以字串表示
df.filter('symbol = "AAPL" and open >100')
df.filter("close <= 200 and not(open > 30)")

# where 可完全替代filter
df.where(df["column"] == "something").show()    # where的方法

# 運用contains()取出特定欄位中有指定字元的列
df.filter(df.symbol.contains('AAPL'))
```

df.groupby (必須搭配 agg( ) 或 pivot( ) )
```
# 一次運算多個欄位(改名比較麻煩，需要用到withColumnRenamed)
df.groupby("col").agg({'col':'max','col':min}).withColumnRenamed(max(col),'new_clo')).show()
df.filter(df['close']>50).groupby('symbol').agg({'close':'avg'}).show()

# 一次將所有欄位都做同一種運算(除了groupby的欄位)
df.groupby(df["column"]).mean().show()

# 必須import funtions 中所有函式才可以使用
from pyspark.sql.functions import *
df.groupBy(df["column"]).agg(avg("column2").alias('new_name'), stddev("column3"), sum("column6")).show()
```

null & duplicate
```
# 查閱缺失值，根據指定欄位，打印出符合條件的ROW
df.filter/where(df["column"].isNull()/isNotNull()).show()

# 刪除缺失值(all:所有欄位中都有NA則刪除列，any:只要有其一為NA刪除row)
df.dropna(how="any/all",subset=["column1","column2"]) 
df.dropDuplicates(['col1','col2'])

# 填補缺失值(填補每個欄位要分開寫)
df.fillna("col1",subset = ["column1"]).fillna("fillthing",subset = ["column2"]) 
df.na.fill()
```

order by排序
```
# 一般排序
color_df.sort('col1','col2',ascending=False)

# 客製化排序
color_df.sort(color_df.length.desc(), color_df.color.asc())

# 使用RDD函式
color_df.orderBy('col1,'col2').take()/show()
```

join 合併
```
# join在使用的時候，簡單對應欄位的寫法會造成合併後欄位重複。
重複：df = df1.join(df2, df1["column"] == df2["column"])  
非重：df1.join(df2,["col1",col2])

# 除了inner join，其他 join 都有可能會產生null值，只要合併的條件欄位不是完全一樣。
# 利用 left 與 right 是可以避免null值，但就會產生一對多值。
df1.join(df2, df1["column"] == df2["column"],"inner").show() 

# 在join後直接接select，是最好確保新表格欄位是正確的
df1.join(df2, df1.name == df2.name, ‘outer’).select(df1.name, df1.weight, df2.height).sort(desc(“name”))
```

輸出
```
# 存成不同檔案類型
df.write.parquet("hdfs:/path/filename")
df.write.json("hdfs://PATH/filename")

# 寫入JDBC
df.write.option("driver", "com.mysql.jdbc.Driver").jdbc("jdbc:mysql://localhost:3306", 
                                                        "crime.convictions_by_borough_with_percentage",
                                                        properties={"user": "root", "password": "1234"})
```

其餘常用函式
```
df.printSchema()      # 查看資料型態
df.dtypes             # 以list顯示欄位與型態
df.columns            # 查看欄位名
df.show(n)            # 查看資料欄位
df.limit(10)          # 指定顯示列數
df.toJSON()           # 將df轉化為json
df.toPandas()         # 將df轉化為pandas 

# 將df以RDD語法操作，需先轉化為RDD型態
rdd = df.rdd
list = rdd.collect() 

# 將df以SQL語法操作，須先建立SQL_view
df.createOrReplaceTempView("viewtable")
result = spark.sql("SQL語法 FROM viewtable")
```

