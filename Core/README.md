## RDD操作函式

  ### 常用函數
		* map : 以單一RDD為主去map，函數return需要打印完整的key&value，可以依照不同位置設置指針；ex. lambda x : (x, x[0])
		* mapValues : 只針對RDD的value做map，會完全保留key不用，因此函數不需打印key，只針對每個value做處理，如果value有新增項，需用tuple與value包裹。
		* flatMap：將原有形式拆解，只以單一RDD形式呈現，如果是map則會將物件依照原有形式輸出至RDD呈現
		* reduce：以整體RDD為單位用函數在彼此之間做操作，例如lambda x,y: x+y
		* reduceByKey：只針對value行動，目的以跨RDD之間的運算為主；會先在单台机器中计算，再将结果进行shuffle，I/O开销比groupByKey要小。
		* groupByKey：只針對RDD中的value行動，根据不同的Key映射到不同的partition中再进行aggregate，如數值運算行為多，不建議使用。
		* sampleByKey：針對RDD的key指定抽樣機率，指定參數為fraction={A:0.5}。ex.key為A的有0.5的機率被抽中。
		* sortBy：依據給定的函數決定排列的標準，需增加布林值決定順序或倒序。
		* count：計算RDD中的內一層中有幾個單位，不帶unique功能。
		* distinct：對輸入的所有RDD去重，考量到檢查排列組合，計算時間長，建議自己用filter操作函數
		* join：依據key做join，如果是outer_join，則分為左右或全。
		* union：將兩個裝有RDD的變數做聯集。
		* subtractByKey：針對兩組RDD做差集相減。
		* filter：根據函數篩選RDD，條件true者返回。
		* toDF: 將具備schema的RDD轉化為dataframe

 ####高階函數
	  * persist：使RDD暫存在記憶體，可以快取。
		* Accumulator：計數器，用於了解在單一函數行動中每個RDD操作是否確實。
		* foldByKey：
		* aggregateByKey：是先对每个分區中的RDD根据不同的Key进行aggregat，再完成各个分區之间的aggregate。因此，和groupByKey()相比，运算量小了很多。
		* combineByKey：以單一RDD為單位，用類似於mapValue的功能為每個RDD創建一個組合；再對每個組合作之間以指定函數運算(只有相同key時才行動)，最後依據指定條件將組合做組合。

參考1：https://www.linkedin.com/pulse/spark-pyspark-combinebykey-simplified-swadeep-mishra-1/
參考2：https://blog.csdn.net/zhuzuwei/article/details/104446388
