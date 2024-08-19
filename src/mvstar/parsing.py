from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, size

def process_json_to_parquet(json_file_path, output_parquet_path):
    # Spark 세션 초기화
    spark = SparkSession.builder.appName('json_processing').getOrCreate()

    # JSON 파일 읽기
    jdf = spark.read.option("multiline", "true").json(json_file_path)

    # 배열 크기 계산하여 새로운 컬럼 추가
    ccdf = jdf.withColumn("company_count", size("companys")).withColumn("directors_count", size("directors"))

    # 조건에 맞는 데이터 필터링 (company_count > 1 또는 directors_count > 1)
    fdf = ccdf.filter((ccdf.company_count > 1) | (ccdf.directors_count > 1))

    # companys 배열 "펼치기"
    edf = fdf.withColumn("company", explode("companys"))

    # directors 배열 추가로 "펼치기"
    eedf = edf.withColumn("director", explode("directors"))

    # 결과를 Parquet 파일로 저장
    eedf.write.mode("overwrite").parquet(output_parquet_path)

    # Spark 세션 종료
    spark.stop()
