
# Пример вызова: spark-submit pyspark_crimes.py --inputpath '/Users/olya/Documents/курсы/otus/pyspark/crime.csv' 
#												--inputpathdict '/Users/olya/Documents/курсы/otus/pyspark/offense_codes.csv' 
#												--outputpath '/Users/olya/Documents/курсы/otus/pyspark/crimes_datamart.parquet'


from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.window import Window
import requests
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--inputpath", help="path to input data crimes.csv")
parser.add_argument("--inputpathdict", help="path to input dictionary offence_codes.csv")
parser.add_argument("--outputpath", help="path to output data crimes_datamart.parquet")
args = parser.parse_args()
if args.inputpath:
    inputpath = args.inputpath
if args.inputpathdict:
    inputpathdict = args.inputpathdict
if args.outputpath:
    outputpath = args.outputpath


spark = SparkSession.builder\
        .master("local[*]")\
        .appName('https://www.kaggle.com/AnalyzeBoston/crimes-in-boston')\
        .getOrCreate()


# Чтение CSV файла https://www.kaggle.com/AnalyzeBoston/crimes-in-boston

#csv_file_url = 'https://www.kaggle.com/AnalyzeBoston/crimes-in-boston?select=crime.csv'
#csv_dict_url = 'https://www.kaggle.com/AnalyzeBoston/crimes-in-boston?select=offence_codes.csv'

               
#response = requests.request(
#        method="GET", url=csv_file_url
#)

#print(response)
#exit()


# Чтение CSV файлов

#csv_file = '/Users/olya/Documents/курсы/otus/pyspark/crime.csv'
#csv_dict = '/Users/olya/Documents/курсы/otus/pyspark/offense_codes.csv'


csv_file = inputpath
csv_dict = inputpathdict


data = spark.read.csv(
    csv_file,
    sep=',',
    header=True,
).filter((f.col('district').isNotNull()))\
.withColumn('offense_code', f.col('offense_code').cast('int'))

data.printSchema()


# Чтение справочника
dict_data = spark.read.csv(
    csv_dict,
    sep=',',
    header=True,
).withColumn('code', f.col('code').cast('int'))
dict_data.printSchema()


# Очистка справочника
dict_data_clear = dict_data.select(['code', 'name'])\
    .groupBy('code')\
    .agg (f.max('name').alias("name"))


# Разбивка по разделителю
split_col = f.split(dict_data_clear['name'], '-')
df_dict = dict_data_clear.withColumn('name', split_col.getItem(0))


#  Общее количество преступлений в районе, средние широта и долгота
crimes_total_lat_lng = data.groupBy('district')\
    .agg (
    	f.count('offense_code').alias('crimes_total'),
    	f.avg('lat').alias('lat'),
    	f.avg('long').alias('lng'),
    )


# Медиана числа преступлений в месяц в этом районе
crimes_monthly = data.groupBy('district', 'year', 'month')\
    .agg (f.count('*').alias('crimes_total_month'))\
    .groupBy('district')\
    .agg(f.expr('percentile_approx(crimes_total_month, 0.5)').alias('crimes_monthly'))


top3_crimes = (
	data
	.groupby('district', 'offense_code')
	.agg(
		f.count('*').alias('crimes_frequently')
	)
	.withColumn('rn', 
	            f.row_number().over(Window.partitionBy('district').orderBy(f.desc('crimes_frequently')))
	)
	.filter(f.col('rn') <= 3)
)


top3_crimes_concat = top3_crimes.join(df_dict, top3_crimes["offense_code"] == df_dict["code"], "left_outer")\
	.groupby('district')\
	.agg(f.concat_ws(", ", f.collect_list('name')).alias('frequent_crime_types'))


# Сборка витрины
datamart = crimes_total_lat_lng.join(crimes_monthly, on='district', how='left')\
	.join(top3_crimes_concat, on='district', how='left')


datamart.write.parquet(outputpath, mode='overwrite')



