# ==============================================================================
# ORQUESTADOR ELT CON TARGETS (Modo Perezoso)
# ==============================================================================

if (!require("pacman")) install.packages("pacman")
pacman::p_load(targets, tarchetypes, here)

cat("\n[TARGETS] Iniciando Pipeline ELT...\n")

# --- CONFIGURACIÓN LOG MAESTRO ---
log_dir <- here::here("ETL", "LOG_ETL")
if (!dir.exists(log_dir)) dir.create(log_dir, recursive = TRUE)
log_file <- file.path(log_dir, "ETL_EXECUTION.log")

log_msg <- function(msg, status = "INFO") {
  timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  user_info <- paste0(Sys.info()[["user"]], "@", Sys.info()[["nodename"]])
  entry <- sprintf("[%s] [%s] [%s] %s", timestamp, status, user_info, msg)

  # Imprimir en consola y archivo
  cat(entry, "\n")
  write(entry, file = log_file, append = TRUE)
}

log_msg("--------------- INICIO EJECUCIÓN ELT ---------------", "START")

# --- EJECUCIÓN PIPELINE ---
tryCatch(
  {
    # targets verificará dependencias y solo correrá lo necesario.
    targets::tar_make()

    log_msg("Pipeline completado exitosamente.", "SUCCESS")
    cat("\n[TARGETS] OK.\n")

    # --- REPORTING TOTALES (Solo si éxito) ---
    try({
      require(arrow)
      require(readr)
      require(dplyr)

      # 1. Entrada
      log_extract_path <- here::here("ETL", "AUDIT", "LOGS_EVIDENCIA", "LOG_EXTRACT.csv")
      total_entrada <- 0
      if (file.exists(log_extract_path)) {
        df_log <- read_csv(log_extract_path, show_col_types = FALSE)
        total_entrada <- sum(df_log$Filas_Leidas_Arrow, na.rm = TRUE)
      }

      # 2. Bronze
      dir_bronce <- here::here("DATOS", "DATOS_BRONCE")
      total_bronce <- 0
      if (dir.exists(dir_bronce) && length(list.files(dir_bronce, recursive = TRUE)) > 0) {
        total_bronce <- nrow(arrow::open_dataset(dir_bronce))
      }

      # 3. Silver
      dir_silver <- here::here("DATOS", "DATOS_SILVER")
      total_silver <- 0
      if (dir.exists(dir_silver) && length(list.files(dir_silver, recursive = TRUE)) > 0) {
        total_silver <- nrow(arrow::open_dataset(dir_silver))
      }

      # 4. Gold (Monolithic + Quarantine)
      dir_gold <- here::here("DATOS", "DATOS_GOLD")

      # Buscar el último archivo generado por timestamp
      path_gold_dir <- file.path(dir_gold, "DATASET_FINAL")
      gold_files <- list.files(path_gold_dir, pattern = "PIV_MASTER_GOLD_.*\\.parquet", full.names = TRUE)

      file_gold <- if (length(gold_files) > 0) tail(sort(gold_files), 1) else ""

      file_quarantine <- file.path(dir_gold, "QUARANTINE_DUPLICATOS", "QUARANTINE_DUPLICATES.parquet")

      total_gold <- 0
      total_quarantine <- 0

      if (file.exists(file_gold)) {
        total_gold <- nrow(arrow::read_parquet(file_gold))
      }

      if (file.exists(file_quarantine)) {
        total_quarantine <- nrow(arrow::read_parquet(file_quarantine))
      }

      log_msg(sprintf(
        "RESUMEN TOTALES: Entrada=%d | Bronze=%d | Silver=%d | Gold=%d | Cuarentena=%d",
        total_entrada, total_bronce, total_silver, total_gold, total_quarantine
      ), "METRICS")
    })
  },
  error = function(e) {
    log_msg(paste("Error crítico en el pipeline:", e$message), "ERROR")
    cat("\n[TARGETS] Error.\n")
    message(e)
  }
)

log_msg("--------------- FIN EJECUCIÓN ELT ---------------\n", "END")
