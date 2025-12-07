Пект: Аналитика COVID-19 на стеке HDFS + Spark + Docker

## Проект демонстрирует полный цикл работы с данными медицинских изображений и метаданных COVID‑19 в среде Big Data:

- хранение данных в HDFS в формате Parquet;
- предобработку метаданных в Python/pandas;
- аналитические запросы и агрегации в Apache Spark (SQL + PySpark);
- построение базовых визуализаций (диагнозы, возраст) в Jupyter Notebook.

## Основной объект анализа — таблица метаданных 
covid_metadata
, содержащая информацию о пациентах, диагнозах и исследованиях.

Датасет взят с публичного репозитория: https://github.com/ieee8023/covid-chestxray-dataset/tree/master

Структура проекта

Примерная структура каталогов:

project_root/
├─ data/
│  ├─ raw/
│  │  └─ metadata.csv                  # исходные метаданные
│  └─ processed/
│     ├─ metadata_cleaned.csv          # очищенные метаданные (для анализа в Python)
│     └─ metadata_cleaned_spark.parquet# очищенные данные для Spark/HDFS
├─ notebooks/
│  └─ covid_analysis.ipynb             # Jupyter Notebook с SQL, PySpark и визуализацией
├─ scripts/

text


Фактические пути могут отличаться, но логика остаётся такой.



Используемый стек

Apache Hadoop HDFS — распределённое файловое хранилище.
Apache Spark (3.x) — вычислительный движок для SQL и PySpark.
Python 3.x, pandas, pyarrow — предобработка и экспорт в Parquet.
Jupyter Notebook — демонстрация запросов и визуализации.
Docker / docker-compose — развёртывание кластера (namenode, datanode, spark‑master, spark‑worker).


Подготовка данных

Поместить исходный файл метаданных:

data/raw/metadata.csv
Запустить скрипт предобработки:

cd scripts
python clean_metadata.py
bash

Скрипт выполняет:
очистку и нормализацию диагнозов (
finding_clean
);
построение возрастных признаков (
age_group
,
age_category
);
приведение типов (дата, числовые поля);
сохранение результатов в:
data/processed/metadata_cleaned.csv
data/processed/metadata_cleaned_spark.parquet
.


Запуск кластера в Docker

В корне проекта выполнить:

docker-compose up -d
Будут подняты контейнеры HDFS и Spark (namenode, datanode, spark-master, spark-worker и т.п.).

Загрузить очищенные данные в HDFS (пример команды внутри namenode):

docker exec -it namenode bash

hdfs dfs -mkdir -p /covid_dataset/metadata
hdfs dfs -put /path/inside/container/metadata_cleaned_spark.parquet \
    /covid_dataset/metadata/
bash

(При необходимости скорректировать путь к файлу внутри контейнера.)



Аналитика в Spark

Консольный PySpark

Пример подключения к данным внутри 
spark-master
:

docker exec -it spark-master bash
/opt/spark/bin/pyspark --master local[*]
bash


В консоли PySpark:

from pyspark.sql import functions as F

df = spark.read.parquet("hdfs://namenode:8020/covid_dataset/metadata/metadata_cleaned_spark.parquet")
df.createOrReplaceTempView("covid_metadata")

spark.sql("""
    SELECT finding_clean, COUNT(*) AS cnt
    FROM covid_metadata
    GROUP BY finding_clean
    ORDER BY cnt DESC

python



Jupyter Notebook

Для демонстрации SQL, PySpark и визуализаций используется ноутбук:

notebooks/covid_analysis.ipynb

В нём показаны:

чтение данных из HDFS (Parquet);
SQL‑запросы по таблице
covid_metadata
;
анализ с помощью PySpark DataFrame API;
визуализации:
круговая диаграмма распределения диагнозов (
finding_clean
);
гистограмма возраста пациентов с COVID‑19.

Именно этот ноутбук предоставляется как часть отчётных материалов.



Основные выводы (кратко)

В выборке доминирует диагноз COVID‑19 (584 записей), на втором месте — пневмония.
Случаи COVID‑19 в основном приходятся на средний и старший возраст, средний возраст пациентов с COVID‑19 ≈ 56 лет.
Качество данных ограничено:
большое количество пропусков по датам и клиническим показателям;
часть диагнозов не может быть однозначно нормализована (
Unknown
);
один пациент может фигурировать несколько раз (серии исследований).


Рекомендации по развитию

Улучшить полноту и качество заполнения ключевых полей (дата, клинические параметры, диагноз).
Внедрить постоянный метastore (Hive/Glue) и партиционирование по дате/диагнозу для ускорения запросов.
Расширить набор аналитики и визуализаций (дашборды, модели ML) на основе уже подготовленных агрегатов.
