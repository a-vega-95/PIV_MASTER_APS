# _targets.R
library(targets)
library(tarchetypes)

# Cargar funciones
# Cargar funciones modulares
source("R/etl_extract.R")
source("R/etl_transform.R")
source("R/etl_gold.R")
source("R/etl_audit.R")

# Opciones globales
tar_option_set(
    packages = c("arrow", "dplyr", "duckdb", "stringr", "here", "fs", "glue", "readr", "purrr"),
    format = "file" # Default format for file tracking
)

# Definición del Pipeline
list(
    # 1. Track Input Directory (Batch level)
    # Si cambia algo en DATOS_ENTRADA, se invalida 'raw_files'
    tar_target(
        raw_dir,
        here::here("DATOS", "DATOS_ENTRADA"),
        format = "file"
    ),

    # 2. Extract -> Bronze
    # Depende de raw_dir. Retorna dir_bronze.
    tar_target(
        bronze_dir,
        etl_extract(raw_dir, here::here("DATOS", "DATOS_BRONCE")),
        format = "file"
    ),

    # 3. Bronze -> Silver
    # Depende de bronze_dir. Retorna silver_dir.
    tar_target(
        silver_dir,
        etl_transform(bronze_dir, here::here("DATOS", "DATOS_SILVER")),
        format = "file"
    ),

    # 4. Silver -> Gold
    # Depende de silver_dir. Retorna gold_dir.
    tar_target(
        gold_dir,
        etl_gold(silver_dir, here::here("DATOS", "DATOS_GOLD")),
        format = "file"
    ),

    # 5. Audit
    # Depende de todos los directorios. Retorna path del log.
    tar_target(
        audit_log,
        etl_audit(bronze_dir, silver_dir, gold_dir, here::here("ETL", "AUDIT", "LOGS_EVIDENCIA")),
        format = "file"
    )
)
