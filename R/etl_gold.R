# R/etl_gold.R
etl_gold <- function(input_dir_silver, output_dir_gold) {
    require(duckdb)
    require(dplyr)
    require(arrow)
    require(glue)

    if (!dir.exists(input_dir_silver)) stop("No Silver")
    if (!dir.exists(output_dir_gold)) dir.create(output_dir_gold, recursive = TRUE)

    con <- dbConnect(duckdb::duckdb())
    on.exit(dbDisconnect(con, shutdown = TRUE))

    ds_silver <- arrow::open_dataset(input_dir_silver)
    arrow::to_duckdb(ds_silver, table_name = "silver_view", con = con)

    centros_dsm <- c(
        "CENTRO DE SALUD FAMILIAR AMANECER", "CENTRO COMUNITARIO DE SALUD FAMILIAR ARQUENCO",
        "CENTRO DE SALUD FAMILIAR EL CARMEN", "CENTRO COMUNITARIO DE SALUD FAMILIAR VILLA EL SALAR",
        "CENTRO DE SALUD FAMILIAR LABRANZA", "CENTRO COMUNITARIO DE SALUD FAMILIAR LAS QUILAS",
        "CENTRO DE SALUD DOCENTE ASISTENCIAL MONSEÑOR SERGIO VALECH", "CENTRO DE SALUD FAMILIAR PEDRO DE VALDIVIA (TEMUCO)",
        "POSTA DE SALUD RURAL COLLIMALLÍN", "POSTA DE SALUD RURAL CONOCO", "CENTRO DE SALUD FAMILIAR PUEBLO NUEVO",
        "CENTRO DE SALUD FAMILIAR SANTA ROSA", "CENTRO DE SALUD FAMILIAR VILLA ALEGRE (TEMUCO)"
    )
    centros_sql <- paste(sprintf("'%s'", centros_dsm), collapse = ", ")

    sql_gold <- paste0("
  CREATE OR REPLACE TABLE gold_temp AS
  SELECT *,
    regexp_replace(RUN || DV, '[^0-9Kk]', '', 'g') AS ID_PCTE,
    CASE WHEN NOMBRE_CENTRO IN (", centros_sql, ") THEN 'SI' ELSE 'NO' END AS DSM_TCO,
    date_diff('year', FECHA_NACIMIENTO, FECHA_CORTE) AS EDAD,
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

    dbExecute(con, sql_gold)
    dbExecute(con, paste0("COPY (SELECT * FROM gold_temp) TO '", output_dir_gold, "' (FORMAT PARQUET, PARTITION_BY (anio, mes), OVERWRITE_OR_IGNORE)"))

    return(output_dir_gold)
}
