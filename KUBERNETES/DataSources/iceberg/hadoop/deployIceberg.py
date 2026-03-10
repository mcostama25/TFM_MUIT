import os
import shutil
import tempfile
from datetime import datetime

# PySpark Imports
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

class IcebergETLDataPipeline:
    """
    Clase que encapsula un pipeline ETL robusto desde CSV crudo hasta Apache Iceberg.
    Diseño orientado a objetos para facilitar la integración en orquestadores (Airflow/Dagster).
    """

    def __init__(self, catalog_name="local_iceberg", warehouse_dir="./iceberg_warehouse"):
        self.catalog_name = catalog_name
        self.warehouse_dir = warehouse_dir
        self.spark = self._init_spark_session()

    def _init_spark_session(self) -> SparkSession:
        # El paquet que ja sabem que Spark troba i descarrega bé
        iceberg_pkg = (
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,"
            "org.projectnessie.nessie-integration:nessie-spark-extensions-3.5_2.12:0.79.0"
        )

        conf = SparkConf()
        conf.set("spark.jars.packages", iceberg_pkg)
        
        # 1. Extensions obligatòries d'Iceberg
        conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        
        # 2. Configuració del Catàleg (Segons documentació oficial de Nessie)
        conf.set(f"spark.sql.catalog.{self.catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
        
        # IMPORTANT: Useu NessieCatalog en lloc de RESTCatalog
        conf.set(f"spark.sql.catalog.{self.catalog_name}.catalog-impl", 
                "org.apache.iceberg.rest.RESTCatalog")
        
        # La URI apunta a l'API base de Nessie, NO a la d'Iceberg
        conf.set(f"spark.sql.catalog.{self.catalog_name}.uri", 
                "http://localhost:19120/iceberg/v1")
        
        # Referència a la branca (main per defecte)
        conf.set(f"spark.sql.catalog.{self.catalog_name}.ref", "main")
        
        # Ubicació física de les dades (com que Spark corre fora de K8s, usem la ruta d'Ubuntu)
        conf.set(f"spark.sql.catalog.{self.catalog_name}.warehouse", f"file://{self.warehouse_dir}")
        
        return SparkSession.builder \
            .config(conf=conf) \
            .appName("Nessie_Iceberg_Pipeline") \
            .getOrCreate()
    # def _init_spark_session(self) -> SparkSession:
    #     """
    #     Inicializa SparkSession con el Runtime de Iceberg y el Catálogo Hadoop.
    #     """
    #     print(f"[{datetime.now()}] Inicializando Spark Session con soporte Iceberg...")
        
    #     # Coordenadas Maven (Spark 3.5 / Scala 2.12 / Iceberg 1.5.2)
    #     iceberg_pkg = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2"

    #     conf = SparkConf()
    #     conf.set("spark.jars.packages", iceberg_pkg)
        
    #     # Extensiones SQL para soportar comandos DDL/DML de Iceberg (MERGE, CALL, etc.)
    #     conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        
    #     # Configuración del Catálogo (Backend: Hadoop/FileSystem)
    #     conf.set(f"spark.sql.catalog.{self.catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
    #     conf.set(f"spark.sql.catalog.{self.catalog_name}.type", "hadoop")
    #     conf.set(f"spark.sql.catalog.{self.catalog_name}.warehouse", self.warehouse_dir)
        
    #     # Optimizaciones de Escritura
    #     # check-nullability=false permite mejor rendimiento si garantizamos datos limpios antes
    #     conf.set("spark.sql.iceberg.check-nullability", "false") 
        
    #     return SparkSession.builder \
    #         .config(conf=conf) \
    #         .appName("Production_CSV_to_Iceberg") \
    #         .getOrCreate()

    def define_schema(self) -> StructType:
        """
        Define un esquema fuertemente tipado para evitar inferSchema (costoso y frágil).
        """
        return StructType([
            StructField("tx_id", StringType(), False),
            StructField("client_id", IntegerType(), True),
            StructField("amount", DoubleType(), True),
            StructField("category", StringType(), True),
            StructField("tx_ts", TimestampType(), True) # Timestamp ISO-8601
        ])

    def generate_dummy_csv(self, file_path: str):
        """
        Genera un CSV temporal para que el ejemplo sea autocontenido.
        """
        print(f"[{datetime.now()}] Generando datos dummy en {file_path}...")
        data = """tx_id,client_id,amount,category,tx_ts
TX001,101,450.50,Electronics,2023-10-21T10:00:00
TX002,102,20.00,Food,2023-10-21T11:30:00
TX003,101,999.99,Electronics,2023-10-21T12:00:00
TX004,103,5.50,Transport,2023-10-22T08:15:00
TX005,102,120.00,Food,2023-10-22T20:45:00"""
        
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "w") as f:
            f.write(data)

    def run_pipeline(self, input_csv_path: str, target_table: str):
        """
        Ejecuta el flujo principal.
        """
        # 1. Lectura (Extract)
        print(f"[{datetime.now()}] Leyendo CSV con esquema estricto...")
        df_raw = self.spark.read \
            .format("csv") \
            .option("header", "true") \
            .option("mode", "FAILFAST") \
            .schema(self.define_schema()) \
            .load(input_csv_path)

        # 2. Transformación (Transform)
        # Añadimos particionamiento lógico y limpiamos datos
        print(f"[{datetime.now()}] Aplicando transformaciones...")
        df_transformed = df_raw \
            .withColumn("ingestion_ts", F.current_timestamp()) \
            .filter(F.col("amount") > 0) # Regla de negocio simple

        # 3. Escritura (Load) en Iceberg
        # full_table_name = f"{self.catalog_name}.default.{target_table}"
        full_table_name = f"{self.catalog_name}.default.{target_table}"

        print(f"Catalogs disponibles: {self.spark.catalog.listDatabases()}")

        print(f"[{datetime.now()}] Escribiendo en Iceberg: {full_table_name}...")

        # Usamos la API DataFrameWriterV2
        # PartitionedBy: Usamos transformación 'days' para Hidden Partitioning
        try:
            # Crear el namespace 'default' a Nessie si no existeix
            self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {self.catalog_name}.default")
            df_transformed.writeTo(full_table_name) \
                .partitionedBy(F.days("tx_ts")) \
                .createOrReplace()
            print(">>> Escritura exitosa: Tabla creada/reemplazada.")
        except Exception as e:
            print(f"Error en escritura: {e}")

    def verify_data(self, target_table: str):
        """
        Verificación de lectura y metadatos.
        """
        full_table_name = f"{self.catalog_name}.default.{target_table}"
        print(f"\n[{datetime.now()}] --- VERIFICACIÓN DE DATOS ---")
        
        # Leer datos
        self.spark.read.table(full_table_name).show()

        print(f"[{datetime.now()}] --- HISTORIAL DE SNAPSHOTS (Metadatos) ---")
        # Inspeccionar la tabla de metadatos oculta .history
        self.spark.read \
            .format("iceberg") \
            .load(f"{full_table_name}.history") \
            .select("made_current_at", "snapshot_id", "is_current_ancestor") \
            .show(truncate=False)

    def cleanup(self):
        """Limpia la sesión."""
        self.spark.stop()

# --- ENTRY POINT ---
if __name__ == "__main__":
    # Configuración de rutas
    BASE_DIR = os.getcwd()
    CSV_PATH = os.path.join(BASE_DIR, "data_input", "transactions.csv")
    WAREHOUSE_PATH = os.path.join(BASE_DIR, "iceberg_warehouse")
    TABLE_NAME = "sales_transactions"

    # Instancia del Pipeline
    etl = IcebergETLDataPipeline(warehouse_dir=WAREHOUSE_PATH)

    try:
        # 1. Generar CSV
        etl.generate_dummy_csv(CSV_PATH)

        # 2. Ejecutar Pipeline
        etl.run_pipeline(CSV_PATH, TABLE_NAME)

        # 3. Validar
        etl.verify_data(TABLE_NAME)

    finally:
        # Limpieza opcional de la sesión (no borra los datos en disco)
        etl.cleanup()
        print("Proceso finalizado.")