# Employee Data Pipeline 
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import logging
import traceback

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("EmployeePipeline")

# Spark Session
spark = SparkSession.builder \
    .appName("Employee Data Pipeline") \
    .config("spark.jars", "/home/jovyan/work/jars/postgresql-42.7.3.jar") \
    .getOrCreate()

logger.info("Spark Session Started")

try:
    # Load CSV
    df = spark.read.csv(
        "/home/jovyan/work/data/employees_raw.csv",
        header=True,
        inferSchema=True
    )

    logger.info(f"Raw Count: {df.count()}")

    # Cleaning
    df = df.dropna(subset=["employee_id", "email"])
    df = df.dropDuplicates(["employee_id"])
    df = df.filter(col("email").rlike("^[A-Za-z0-9+_.-]+@[A-Za-z0-9.-]+$"))

    df = df.withColumn("hire_date", to_date("hire_date"))
    df = df.withColumn("birth_date", to_date("birth_date"))

    # Transform
    df = df.withColumn("first_name", initcap(col("first_name")))
    df = df.withColumn("last_name", initcap(col("last_name")))
    df = df.withColumn("email", lower(col("email")))

    df = df.withColumn(
        "salary",
        regexp_replace(col("salary"), "[$,]", "").cast("double")
    )

    df = df.withColumn(
        "age",
        (datediff(current_date(), col("birth_date")) / 365).cast("int")
    )

    df = df.withColumn(
        "tenure_years",
        (datediff(current_date(), col("hire_date")) / 365)
    )

    df = df.withColumn(
        "salary_band",
        when(col("salary") < 50000, "Junior")
        .when(col("salary") <= 80000, "Mid")
        .otherwise("Senior")
    )

    # Enrichment
    df = df.withColumn("full_name", concat_ws(" ", col("first_name"), col("last_name")))
    df = df.withColumn("email_domain", split(col("email"), "@")[1])

    # Add load timestamp
    df = df.withColumn("loaded_at", current_timestamp())

    # Final Columns
    df = df.select(
        "employee_id","first_name","last_name","full_name",
        "email","email_domain","hire_date","job_title","department",
        "salary","salary_band","manager_id","address","city","state",
        "zip_code","birth_date","age","tenure_years","status","loaded_at"
    )

    logger.info(f"Clean Count: {df.count()}")

    # Write section
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/employees_db") \
        .option("dbtable", "employees_clean") \
        .option("user", "admin") \
        .option("password", "admin") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

    logger.info("Data Appended to PostgreSQL!")

except Exception as e:
    logger.error("Pipeline Failed!")
    traceback.print_exc()

spark.stop()
logger.info("Spark Stopped")