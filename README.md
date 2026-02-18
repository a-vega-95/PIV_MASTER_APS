# PIV - ETL Moderno (Targets + DuckDB + Arrow)

Proyecto de ingeniería de datos para la ingesta, transformación y auditoría de datos PIV.
Esta versión implementa una arquitectura **ELT (Extract, Load, Transform)** orquestada por el framework `targets`, utilizando `Arrow` para lectura eficiente y `DuckDB` para procesamiento SQL de alto rendimiento.

## Como Ejecutar

El proyecto cuenta con un **unico punto de entrada** para el usuario:

```r
source("ETL/00_run_etl.R")
```

Este script se encarga de:
1.  Cargar librerias necesarias.
2.  Invocar a `targets::tar_make()` para ejecutar el pipeline de forma inteligente.
3.  Generar y actualizar el **Log Maestro de Ejecucion**.

## Estructura del Proyecto

### Nucleo del Pipeline
*   **`ETL/00_run_etl.R`**: Orquestador principal. **(Ejecutar este script)**.
*   **`_targets.R`**: Definicion del pipeline y sus dependencias (Grafo de ejecucion).
*   **`R/` (Modulos de Logica)**:
    *   `etl_extract.R`: Ingesta TXT -> Bronze.
    *   `etl_transform.R`: Limpieza Bronze -> Silver.
    *   `etl_gold.R`: Reglas Negocio Silver -> Gold.
    *   `etl_audit.R`: Generacion de Reportes.

### Directorios de Datos
*   **`DATOS/DATOS_ENTRADA/`**: Archivos TXT crudos de origen.
*   **`DATOS/DATOS_BRONCE/`**: Copia exacta en formato Parquet (con particionado Hive).
*   **`DATOS/DATOS_SILVER/`**: Datos limpios y tipados (Parquet).
*   **`DATOS/DATOS_GOLD/`**:
    *   `DATASET_FINAL/PIV_MASTER_GOLD_YYMMDD_HHMM.parquet`: **Monolitico**. Dataset final limpio y validado (Sin duplicados, nombre con timestamp dinámico).
    *   `QUARANTINE_DUPLICATOS/`: Registros duplicados detectados por SHA256.

### Auditoria y Logs
*   **`ETL/LOG_ETL/ETL_EXECUTION.log`**: **Log Maestro**. Historial acumulativo.
*   **`ETL/AUDIT/LOGS_EVIDENCIA/`**: Reportes de integridad (CSV con BOM).

## Tecnologias

*   **R & Targets**: Orquestacion modular.
*   **Apache Arrow**: Lectura veloz.
*   **DuckDB**: Motor SQL embebido.

## Autor
Anghello Vega
Técnico en laboratorio clínico, banco de sangre e imagenología.
Estudiante de Universidad Mayor, Ingeniería en Informática y Computación.
Gestor de datos del Departamento de Salud Municipal de Temuco, Chile.
Actualizado: 17-02-2026
