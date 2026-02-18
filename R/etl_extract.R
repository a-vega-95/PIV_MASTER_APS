# R/etl_extract.R
etl_extract <- function(input_dir, output_dir) {
    require(arrow)
    require(dplyr)
    require(stringr)
    require(fs)
    require(readr)
    require(here)

    if (!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE)

    files <- fs::dir_ls(input_dir, glob = "*.txt", recurse = TRUE)

    if (length(files) == 0) {
        warning("No hay archivos en ", input_dir)
        return(output_dir)
    }

    ingestar_archivo <- function(f) {
        # Inferencia de periodo
        parent_dir <- path_file(path_dir(f))
        anio <- NA
        mes <- NA

        if (str_detect(parent_dir, "^\\d{6}$")) { # MMYYYY
            mes <- str_sub(parent_dir, 1, 2)
            anio <- str_sub(parent_dir, 3, 6)
        } else if (str_detect(parent_dir, "^\\d{4}-\\d{2}$")) { # YYYY-MM
            parts_date <- str_split(parent_dir, "-")[[1]]
            anio <- parts_date[1]
            mes <- parts_date[2]
        }

        if (is.na(anio) | is.na(mes)) {
            anio <- format(Sys.Date(), "%Y")
            mes <- "00"
        }

        # Lectura
        tryCatch(
            {
                ds <- arrow::read_delim_arrow(
                    f,
                    delim = ",", quote = "\"", col_types = arrow::schema(), as_data_frame = FALSE
                )
            },
            error = function(e) stop(e)
        )

        num_rows <- nrow(ds)

        # Mutate partitions
        ds <- ds %>% mutate(anio = !!anio, mes = !!mes, source_file = !!path_file(f))

        # Write
        arrow::write_dataset(
            ds,
            path = output_dir, format = "parquet",
            partitioning = c("anio", "mes"),
            existing_data_behavior = "overwrite",
            basename_template = paste0("part-{i}-", fs::path_ext_remove(path_file(f)), ".parquet")
        )

        # Log
        log_entry <- data.frame(
            Archivo = path_file(f), Periodo = paste0(anio, "-", mes),
            Filas_Leidas_Arrow = num_rows, Estado = "OK", Timestamp = Sys.time()
        )
        # Log path hardcoded or param? For simplicity let's keep consistent location
        log_file <- here::here("ETL", "AUDIT", "LOGS_EVIDENCIA", "LOG_EXTRACT.csv")
        if (!dir.exists(dirname(log_file))) dir.create(dirname(log_file), recursive = TRUE)
        if (!file.exists(log_file)) write_excel_csv(data.frame(Archivo = character(), Periodo = character(), Filas_Leidas_Arrow = integer(), Estado = character(), Timestamp = as.POSIXct(character())), log_file)

        write_excel_csv(log_entry, log_file, append = TRUE)
        return(TRUE)
    }

    purrr::map_lgl(files, ingestar_archivo)
    return(output_dir) # Return output path for targets dependency
}
