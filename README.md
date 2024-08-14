# spark

Данные в кафке
![image](https://github.com/user-attachments/assets/291de304-616f-4db1-9f07-6a894cb88da1)

Заливаю в локальный клик таблицу dict_StoragePlace для обогащения office_id по place_cod

Запускаю задание:
> spark-submit --master spark://spark-master:7077
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0
--executor-cores 1
--conf spark.driver.extraJavaOptions="-Divy.cache.dir=/tmp -Divy.home=/tmp"
/opt/spark/Streams/shkDefect_verdict_sync.py


