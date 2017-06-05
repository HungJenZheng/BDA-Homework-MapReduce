###巨量資料分析 HW3 MapReduce###

1. 組員
    四資三 103590010 施椀翎
    四資三 103590046 鄭鴻仁

2. 使用環境
    Linux mint + Spark + Python + Jupyter Notebook

    安裝Spark
	a. 到Spark官網下載，並且解壓縮，並移動到/opt底下
	b. 加入環境變數至~/.profile裡
	    SPARK_HOME=/opt/spark
	    PATH=$PATH:$SPARK_HOME
    安裝Python、Python-dev、pip、Jupyter notebook、JDK、ipython、ipython notebook

3. 使用說明
    在BDA_MapReduce_HW裡，執行spark-submit hw3.py