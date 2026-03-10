docker exec -it spark spark-sql \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.demo=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.demo.type=rest \
    --conf spark.sql.catalog.demo.uri=http://iceberg-rest:8181 \
    --conf spark.sql.catalog.demo.warehouse=s3://iceberg/
