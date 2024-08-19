from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, size

def process_json_to_parquet(json_file_path, output_parquet_path):
    spark = SparkSession.builder.appName('json_processing').getOrCreate()

    # JSON 파일 읽기
    jdf = spark.read.option("multiline", "true").json(json_file_path)

    # companys 배열 "펼치기"
    edf = jdf.withColumn("company", explode_outer("companys"))

    # directors 배열 추가로 "펼치기"
    eedf = edf.withColumn("director", explode_outer("directors"))

    # 결과를 Parquet 파일로 저장
    eedf.write.mode("append").parquet(output_parquet_path)

    # Spark 세션 종료
    spark.stop()
