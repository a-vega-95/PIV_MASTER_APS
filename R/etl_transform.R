# R/etl_transform.R
etl_transform <- function(input_dir_bronze, output_dir_silver) {
    require(duckdb)
    require(dplyr)
    require(arrow)
    require(glue)
    require(fs)

    if (!dir.exists(input_dir_bronze)) stop("No Bronze")

    # Idempotency cleanup
    if (dir.exists(output_dir_silver)) fs::dir_delete(output_dir_silver)
    dir.create(output_dir_silver, recursive = TRUE)

    con <- dbConnect(duckdb::duckdb())
    on.exit(dbDisconnect(con, shutdown = TRUE))

    ds_bronce <- arrow::open_dataset(input_dir_bronze)
    cols_bronce <- names(ds_bronce)
    arrow::to_duckdb(ds_bronce, table_name = "bronze_view", con = con)

    # Dynamic SQL Builder Helpers
    get_coalesce <- function(candidates, default = "''") {
        valid_cols <- intersect(candidates, cols_bronce)
        if (length(valid_cols) == 0) {
            return(default)
        }
        exprs <- paste0("try_cast(", valid_cols, " AS VARCHAR)")
        if (length(exprs) > 1) {
            paste0("COALESCE(", paste(exprs, collapse = ", "), ", ", default, ")")
        } else {
            paste0("COALESCE(", exprs, ", ", default, ")")
        }
    }

    col_run <- get_coalesce(c("RUN", "RUT"))
    col_dv <- get_coalesce(c("DV", "DIGITO_VERIFICADOR"))
    col_nac <- get_coalesce(c("FECHA_NACIMIENTO", "FECHA_NAC"), "NULL")
    col_cor <- get_coalesce(c("FECHA_CORTE"), "NULL")
    col_cen <- get_coalesce(c("NOMBRE_CENTRO", "CENTRO_SALUD", "ESTABLECIMIENTO"))
    col_cod <- get_coalesce(c("COD_CENTRO", "CODIGO_CENTRO", "CODIGO_CENTRO_PROCEDENCIA"))
    col_ace <- get_coalesce(c("ACEPTADO_RECHAZADO", "SITUACION", "ESTADO"))
    col_pre <- get_coalesce(c("PREVISION"))
    col_gen <- get_coalesce(c("GENERO", "SEXO"))
    col_tra <- get_coalesce(c("TRAMO", "TRAMO_PREVISION", "NIVEL"))

    # Source file handling
    col_src <- "source_file"
    if (!("source_file" %in% cols_bronce)) col_src <- "'Unknown'"

    # Mejorar parsing de fechas: Intentar varios formatos conocidos
    # Formatos posibles: DD/MM/YYYY, YYYY-MM-DD, DD-MM-YYYY
    sql_nac <- glue("COALESCE(
        try_strptime(TRIM({col_nac}), '%d/%m/%Y'),
        try_strptime(TRIM({col_nac}), '%Y-%m-%d'),
        try_strptime(TRIM({col_nac}), '%d-%m-%Y'),
        NULL
    )")

    sql_cor <- glue("COALESCE(
        try_strptime(TRIM({col_cor}), '%d/%m/%Y'),
        try_strptime(TRIM({col_cor}), '%Y-%m-%d'),
        try_strptime(TRIM({col_cor}), '%d-%m-%Y'),
        NULL
    )")

    sql_transform <- glue("
  CREATE OR REPLACE TABLE silver_temp AS
  SELECT
    TRIM(UPPER({col_run})) AS RUN,
    TRIM(UPPER({col_dv})) AS DV,
    {sql_nac} AS FECHA_NACIMIENTO,
    {sql_cor} AS FECHA_CORTE,
    TRIM(UPPER({col_cen})) AS NOMBRE_CENTRO,
    TRIM(UPPER({col_cod})) AS COD_CENTRO,
    TRIM(UPPER({col_ace})) AS ACEPTADO_RECHAZADO,
    TRIM(UPPER({col_pre})) AS PREVISION,
    TRIM(UPPER({col_gen})) AS GENERO,
    TRIM(UPPER({col_tra})) AS TRAMO,
    anio, mes,
    {col_src} AS source_file
  FROM bronze_view
  WHERE TRIM(UPPER({col_run})) <> ''
  ")

    dbExecute(con, sql_transform)
    dbExecute(con, glue("COPY (SELECT * FROM silver_temp) TO '{output_dir_silver}' (FORMAT PARQUET, PARTITION_BY (anio, mes))"))

    return(output_dir_silver)
}
