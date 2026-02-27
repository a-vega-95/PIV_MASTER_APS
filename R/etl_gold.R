# R/etl_gold.R
# R/etl_gold.R
etl_gold <- function(input_dir_silver, output_dir_gold) {
  require(duckdb)
  require(dplyr)
  require(arrow)
  require(glue)
  require(openssl)

  if (!dir.exists(input_dir_silver)) stop("No Silver")

  # Directorios de salida
  output_clean <- file.path(output_dir_gold, "DATASET_FINAL")
  output_quarantine <- file.path(output_dir_gold, "QUARANTINE_DUPLICATOS")

  if (dir.exists(output_dir_gold)) {
    files <- list.files(output_dir_gold, full.names = TRUE, recursive = TRUE)
    unlink(files, force = TRUE)
  }
  dir.create(output_clean, recursive = TRUE)
  dir.create(output_quarantine, recursive = TRUE)

  con <- dbConnect(duckdb::duckdb())
  on.exit(dbDisconnect(con, shutdown = TRUE))

  # 1. Cargar Silver
  ds_silver <- arrow::open_dataset(input_dir_silver)
  arrow::to_duckdb(ds_silver, table_name = "silver_view", con = con)

  # 2. Centros DSM Mapping
  centros_dsm <- c(
    "CENTRO DE SALUD FAMILIAR AMANECER", "CENTRO COMUNITARIO DE SALUD FAMILIAR ARQUENCO",
    "CENTRO DE SALUD FAMILIAR EL CARMEN", "CENTRO COMUNITARIO DE SALUD FAMILIAR VILLA EL SALAR",
    "CENTRO DE SALUD FAMILIAR LABRANZA", "CENTRO COMUNITARIO DE SALUD FAMILIAR LAS QUILAS",
    "CENTRO DE SALUD DOCENTE ASISTENCIAL MONSEÑOR SERGIO VALECH", "CENTRO DE SALUD FAMILIAR PEDRO DE VALDIVIA (TEMUCO)",
    "POSTA DE SALUD RURAL COLLIMALLÍN", "POSTA DE SALUD RURAL CONOCO", "CENTRO DE SALUD FAMILIAR PUEBLO NUEVO",
    "CENTRO DE SALUD FAMILIAR SANTA ROSA", "CENTRO DE SALUD FAMILIAR VILLA ALEGRE (TEMUCO)"
  )
  centros_sql <- paste(sprintf("'%s'", centros_dsm), collapse = ", ")

  cat("\n[ELT-GOLD] Generando RAW con Hash SHA256...\n")

  # 3. Preparar Base + Hash SHA256
  # Definimos la llave única de negocio: RUN-DV-FECHA_CORTE-COD_CENTRO-ESTADO
  # Si hay múltiples registros iguales en la misma fecha corte para el mismo centro y estado, son duplicados.

  sql_staging <- paste0("
  CREATE OR REPLACE TABLE gold_staging AS
  SELECT *,
    -- Generar Hash SHA256 sobre TODA la fila (excluyendo metadatos logísticos si los hubiera, pero aquí queremos validación extensa)
    sha256(concat(
      RUN, DV,
      coalesce(cast(FECHA_NACIMIENTO as VARCHAR), ''),
      coalesce(cast(FECHA_CORTE as VARCHAR), ''),
      coalesce(NOMBRE_CENTRO, ''),
      coalesce(COD_CENTRO, ''),
      coalesce(ACEPTADO_RECHAZADO, ''),
      coalesce(GENERO, ''),
      coalesce(TRAMO, ''),
      anio, mes
    )) AS ROW_HASH,

    -- Logica de Negocio
    regexp_replace(RUN || DV, '[^0-9Kk]', '', 'g') AS ID_PCTE,
    CASE WHEN NOMBRE_CENTRO IN (", centros_sql, ") THEN 'SI' ELSE 'NO' END AS DSM_TCO,
    CASE
        WHEN GENERO IN ('F', 'MUJER') THEN 'FEMENINO'
        WHEN GENERO IN ('M', 'HOMBRE') THEN 'MASCULINO'
        ELSE 'NO DEFINIDO'
    END AS GENERO_NORMALIZADO,
    date_diff('year', FECHA_NACIMIENTO, FECHA_CORTE) AS EDAD_EN_FECHA_CORTE,
    CASE
      WHEN date_diff('year', FECHA_NACIMIENTO, FECHA_CORTE) < 1 THEN 'Menos de 1 año'
      WHEN date_diff('year', FECHA_NACIMIENTO, FECHA_CORTE) BETWEEN 1 AND 4 THEN '1 - 4 años'
      WHEN date_diff('year', FECHA_NACIMIENTO, FECHA_CORTE) BETWEEN 5 AND 9 THEN '5 - 9 años'
      WHEN date_diff('year', FECHA_NACIMIENTO, FECHA_CORTE) BETWEEN 10 AND 14 THEN '10 - 14 años'
      WHEN date_diff('year', FECHA_NACIMIENTO, FECHA_CORTE) BETWEEN 15 AND 19 THEN '15 - 19 años'
      WHEN date_diff('year', FECHA_NACIMIENTO, FECHA_CORTE) BETWEEN 20 AND 24 THEN '20 - 24 años'
      WHEN date_diff('year', FECHA_NACIMIENTO, FECHA_CORTE) BETWEEN 25 AND 29 THEN '25 - 29 años'
      WHEN date_diff('year', FECHA_NACIMIENTO, FECHA_CORTE) BETWEEN 30 AND 34 THEN '30 - 34 años'
      WHEN date_diff('year', FECHA_NACIMIENTO, FECHA_CORTE) BETWEEN 35 AND 39 THEN '35 - 39 años'
      WHEN date_diff('year', FECHA_NACIMIENTO, FECHA_CORTE) BETWEEN 40 AND 44 THEN '40 - 44 años'
      WHEN date_diff('year', FECHA_NACIMIENTO, FECHA_CORTE) BETWEEN 45 AND 49 THEN '45 - 49 años'
      WHEN date_diff('year', FECHA_NACIMIENTO, FECHA_CORTE) BETWEEN 50 AND 54 THEN '50 - 54 años'
      WHEN date_diff('year', FECHA_NACIMIENTO, FECHA_CORTE) BETWEEN 55 AND 59 THEN '55 - 59 años'
      WHEN date_diff('year', FECHA_NACIMIENTO, FECHA_CORTE) BETWEEN 60 AND 64 THEN '60 - 64 años'
      WHEN date_diff('year', FECHA_NACIMIENTO, FECHA_CORTE) BETWEEN 65 AND 69 THEN '65 - 69 años'
      WHEN date_diff('year', FECHA_NACIMIENTO, FECHA_CORTE) BETWEEN 70 AND 74 THEN '70 - 74 años'
      WHEN date_diff('year', FECHA_NACIMIENTO, FECHA_CORTE) BETWEEN 75 AND 79 THEN '75 - 79 años'
      WHEN date_diff('year', FECHA_NACIMIENTO, FECHA_CORTE) >= 80 THEN '80 y mas años'
      ELSE 'Sin Información'
    END AS GRUPO_ETARIO
  FROM silver_view
  ")
  dbExecute(con, sql_staging)

  cat("   -> Identificando Duplicados...\n")

  # 4. Separar Duplicados usando Window Function
  # ROW_NUMBER() particionado por el Hash. Si > 1, es duplicado.
  sql_dedup <- "
  CREATE OR REPLACE TABLE gold_final_logic AS
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY ROW_HASH ORDER BY source_file DESC) as rn
  FROM gold_staging
  "
  dbExecute(con, sql_dedup)

  # 5. Exportar CLEAN (rn = 1) -> Monolítico
  # 5. Exportar GOLD CLEAN (Optimización Senior)
  cat("   -> Exportando GOLD CLEAN (Monolítico, Optimizado y Sin Basura)...\n")

  # Limpieza de archivos previos
  old_files <- list.files(output_clean, pattern = "PIV_MASTER_GOLD_.*\\.parquet", full.names = TRUE)
  if (length(old_files) > 0) file.remove(old_files)

  timestamp <- format(Sys.time(), "%y%m%d_%H%M")
  final_parquet_file <- file.path(output_clean, glue("PIV_MASTER_GOLD_{timestamp}.parquet"))

  # --- LA MAGIA ESTÁ AQUÍ ---
  sql_export <- glue("
      COPY (
        SELECT
            * EXCLUDE(rn, ROW_HASH) -- 1. ELIMINAMOS COLUMNAS DE PROCESO (Basura en Gold)
        FROM gold_final_logic
        WHERE rn = 1
          AND DSM_TCO = 'SI'
        ORDER BY
            NOMBRE_CENTRO ASC,      -- 2. CLUSTERING: Agrupa datos por centro (Mejor compresión)
            FECHA_CORTE ASC,        -- Orden Cronológico
            GENERO_NORMALIZADO,     -- Agrupa Strings repetidos
            GRUPO_ETARIO            -- Agrupa Strings repetidos
      ) TO '{final_parquet_file}'
      (
        FORMAT PARQUET,
        COMPRESSION 'ZSTD',         -- 3. ALGORITMO: Estándar moderno (mejor que Snappy)
        COMPRESSION_LEVEL 10,       -- NIVEL: 10 es agresivo pero seguro para lectura
        ROW_GROUP_SIZE 1000000      -- BLOQUES: 1M filas por bloque optimiza metadatos para 22M regs
      )
  ")

  dbExecute(con, sql_export)

  # Validación Post-Exportación (Opcional pero recomendado)
  cat(paste0("   [EXITO] Archivo generado: ", basename(final_parquet_file), "\n"))

  # 6. Exportar QUARANTINE (rn > 1)
  count_duplicates <- dbGetQuery(con, "SELECT COUNT(*) as n FROM gold_final_logic WHERE rn > 1")$n

  if (count_duplicates > 0) {
    cat(sprintf("   [ALERTA] Se encontraron %d registros duplicados. Moviendo a Cuarentena...\n", count_duplicates))
    quarantine_file <- file.path(output_quarantine, "QUARANTINE_DUPLICATES.parquet")
    dbExecute(con, glue("COPY (SELECT * FROM gold_final_logic WHERE rn > 1) TO '{quarantine_file}' (FORMAT PARQUET)"))
  } else {
    cat("   [OK] No se encontraron duplicados.\n")
  }

  # --- NUEVO: EXPORTACIÓN CORTE SEPTIEMBRE ---
  cat("\n[ELT-GOLD] Generando Productos Especiales: Corte Septiembre...\n")
  require(writexl)

  output_sept <- file.path(output_dir_gold, "DATASET_FINAL", "PIVs_CORTE_SEPT")
  if (!dir.exists(output_sept)) dir.create(output_sept, recursive = TRUE)

  # Obtener años disponibles en el corte de Septiembre
  anios_sept <- dbGetQuery(con, "SELECT DISTINCT anio FROM gold_final_logic WHERE mes = 9 AND rn = 1 AND DSM_TCO = 'SI' ORDER BY anio")$anio

  if (length(anios_sept) > 0) {
    for (a in anios_sept) {
      cat(glue("   -> Procesando Año {a} [Corte Septiembre]...\n"))

      # Carpeta por año
      path_anio <- file.path(output_sept, as.character(a))
      if (!dir.exists(path_anio)) dir.create(path_anio, recursive = TRUE)

      # Nombres de archivos
      base_name <- glue("{a}_CORT_SEPT_PIV")
      file_parquet <- file.path(path_anio, paste0(base_name, ".parquet"))
      file_excel <- file.path(path_anio, paste0(base_name, ".xlsx"))

      # 1. Exportar Parquet (vía DuckDB para velocidad)
      sql_sept_path <- glue("
        COPY (
          SELECT * EXCLUDE(rn, ROW_HASH)
          FROM gold_final_logic
          WHERE mes = 9 AND anio = {a} AND rn = 1 AND DSM_TCO = 'SI'
        ) TO '{file_parquet}' (FORMAT PARQUET, COMPRESSION 'ZSTD')
      ")
      dbExecute(con, sql_sept_path)

      # 2. Exportar Excel (vía R/writexl para compatibilidad)
      df_sept <- dbGetQuery(con, glue("
          SELECT * EXCLUDE(rn, ROW_HASH)
          FROM gold_final_logic
          WHERE mes = 9 AND anio = {a} AND rn = 1 AND DSM_TCO = 'SI'
      "))
      writexl::write_xlsx(df_sept, path = file_excel)

      cat(glue("      [OK] Guardados: {basename(file_parquet)} y {basename(file_excel)}\n"))
    }
  } else {
    cat("   [!] No se encontraron datos para el mes de Septiembre (mes = 9).\n")
  }

  return(output_clean)
}
