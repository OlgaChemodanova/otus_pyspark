# otus_pyspark

Код вызывается через spark-submit и имеет следующие аргументы:
--inputpath	Путь до файла с данными crime.csv
--inputpathdict	Путь до справочника кодов преступлений offense_codes.csv
--outputpath	Путь до собранной витрины crimes_datamart.parquet

Пример вызова: spark-submit pyspark_crimes.py --inputpath '/inputpath/crime.csv' 
				 --inputpathdict '/inputpath/offense_codes.csv' 
				 --outputpath '/outputpath/crimes_datamart.parquet'
