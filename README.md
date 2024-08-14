# spark

загружаю данные в кафку
![image](https://github.com/user-attachments/assets/291de304-616f-4db1-9f07-6a894cb88da1)

Заливаю в локальный клик таблицу dict_StoragePlace для обогащения office_id по place_cod

Захожу в контейнер:
> docker exec -u root -it spark-master /bin/bash

Устанавливаю недостающее:
> pip install clickhouse_driver clickhouse_cityhash lz4 pandas

> pip install py4j

Запускаю задание:
> spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 --executor-cores 1 --conf spark.driver.extraJavaOptions="-Divy.cache.dir=/tmp -Divy.home=/tmp" /opt/spark/Streams/shkDefect_verdict_sync.py

Терминал
![image](https://github.com/user-attachments/assets/45257bf0-ec67-41cd-baa6-03f167f8571d)
![image](https://github.com/user-attachments/assets/6516b67d-866c-498e-b2f4-c8e62fc221af)

Проверяю Спарк
![image](https://github.com/user-attachments/assets/353658b9-0b3a-46c8-a8ff-0efd4daeca94)

Проверяю итоговую таблицу
![image](https://github.com/user-attachments/assets/82c9c492-33b4-4f90-a866-9c25ca82c688)
