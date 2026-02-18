# R/etl_audit.R
etl_audit <- function(dir_bronce, dir_silver, dir_gold, output_dir_report) {
    require(duckdb)
    require(dplyr)
    require(arrow)
    require(glue)
    require(readr)
    require(stringr)

    if (!dir.exists(output_dir_report)) dir.create(output_dir_report, recursive = TRUE)

    con <- dbConnect(duckdb::duckdb())
    on.exit(dbDisconnect(con, shutdown = TRUE))

    # Register Views Globally for all periods (simpler) or iterate?
    # Iterating is better to produce granular counts per period.

    dirs_anio <- dir(dir_silver, pattern = "anio=", full.names = TRUE)

    # Generate timestamped filename for this run
    timestamp_str <- format(Sys.time(), "%Y%m%d_%H%M%S")
    file_name <- paste0("AUDIT_LOG_", timestamp_str, ".csv")
    file_consolidado <- file.path(output_dir_report, file_name)

    for (d_a in dirs_anio) {
        anio <- str_extract(d_a, "\\d{4}")
        dirs_mes <- dir(d_a, pattern = "mes=", full.names = TRUE)

        for (d_m in dirs_mes) {
            mes <- str_extract(d_m, "\\d{2}$")
            periodo_str <- paste0(anio, "-", mes)

            path_bronce <- file.path(dir_bronce, paste0("anio=", anio), paste0("mes=", mes))
            path_silver <- file.path(dir_silver, paste0("anio=", anio), paste0("mes=", mes))
            path_gold <- file.path(dir_gold, paste0("anio=", anio), paste0("mes=", mes))

            if (!dir.exists(path_bronce)) next

            # Define views for this period
            dbExecute(con, glue("CREATE OR REPLACE VIEW v_bronce AS SELECT * FROM read_parquet('{path_bronce}/*.parquet')"))

            has_silver <- dir.exists(path_silver)
            if (has_silver) dbExecute(con, glue("CREATE OR REPLACE VIEW v_silver AS SELECT * FROM read_parquet('{path_silver}/*.parquet')"))

            has_gold <- dir.exists(path_gold)
            if (has_gold) dbExecute(con, glue("CREATE OR REPLACE VIEW v_gold AS SELECT * FROM read_parquet('{path_gold}/*.parquet')"))

            resumen <- data.frame(ETAPA = character(), IN = integer(), OUT = integer(), DIFF = integer(), PCT = numeric())

            # BRONZE VS SILVER
            if (has_silver) {
                n_bronce <- dbGetQuery(con, "SELECT COUNT(*) as n FROM v_bronce")$n
                n_silver <- dbGetQuery(con, "SELECT COUNT(*) as n FROM v_silver")$n

                resumen <- rbind(resumen, data.frame(
                    ETAPA = "BRONZE_VS_SILVER", IN = n_bronce, OUT = n_silver, DIFF = n_bronce - n_silver, PCT = (n_silver / n_bronce) * 100
                ))
            }

            # SILVER VS GOLD
            if (has_silver && has_gold) {
                n_gold <- dbGetQuery(con, "SELECT COUNT(*) as n FROM v_gold")$n

                sql_missing <- "
        WITH s AS (SELECT md5(concat(RUN, DV, FECHA_NACIMIENTO, FECHA_CORTE, COD_CENTRO, ACEPTADO_RECHAZADO)) as h_id FROM v_silver),
             g AS (SELECT md5(concat(RUN, DV, FECHA_NACIMIENTO, FECHA_CORTE, COD_CENTRO, ACEPTADO_RECHAZADO)) as h_id FROM v_gold)
        SELECT count(*) as missing FROM s WHERE h_id NOT IN (SELECT h_id FROM g)
        "
                n_missing <- dbGetQuery(con, sql_missing)$missing

                resumen <- rbind(resumen, data.frame(
                    ETAPA = "SILVER_VS_GOLD", IN = n_silver, OUT = n_gold, DIFF = n_missing, PCT = ((n_silver - n_missing) / n_silver) * 100
                ))
            }

            if (nrow(resumen) > 0) {
                resumen$Periodo_Auditado <- periodo_str
                resumen$Timestamp_Ejecucion <- Sys.time()
                resumen$Usuario_Ejecutor <- Sys.info()[["user"]]
                resumen$Maquina <- Sys.info()[["nodename"]]

                resumen <- resumen %>% select(Timestamp_Ejecucion, Usuario_Ejecutor, Maquina, Periodo_Auditado, ETAPA, IN, OUT, DIFF, PCT)

                if (!file.exists(file_consolidado)) {
                    write_excel_csv(resumen, file_consolidado)
                } else {
                    write_excel_csv(resumen, file_consolidado, append = TRUE)
                }
            }
        }
    }
    return(file_consolidado)
}
