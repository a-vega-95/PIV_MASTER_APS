# PIV - ETL Moderno (Targets + DuckDB + Arrow)

Proyecto de ingeniería de datos para la ingesta, transformación y auditoría de datos PIV.
Esta versión implementa una arquitectura **ELT (Extract, Load, Transform)** orquestada por el framework `targets`, utilizando `Arrow` para lectura eficiente y `DuckDB` para procesamiento SQL de alto rendimiento.

## 🚀 Cómo Ejecutar

El proyecto cuenta con un **único punto de entrada** para el usuario:

```r
source("ETL/00_run_etl.R")
```

Este script se encarga de:
1.  Cargar librerías necesarias.
2.  Invocar a `targets::tar_make()` para ejecutar el pipeline de forma inteligente.
3.  Generar y actualizar el **Log Maestro de Ejecución**.

## 📂 Estructura del Proyecto

### Núcleo del Pipeline
*   **`ETL/00_run_etl.R`**: Orquestador principal. **(Ejecutar este script)**.
*   **`_targets.R`**: Definición del pipeline y sus dependencias (Grafo de ejecución).
*   **`R/` (Módulos de Lógica)**:
    *   `etl_extract.R`: Ingesta TXT -> Bronze.
    *   `etl_transform.R`: Limpieza Bronze -> Silver.
    *   `etl_gold.R`: Reglas Negocio Silver -> Gold.
    *   `etl_audit.R`: Generación de Reportes.

### Directorios de Datos
*   **`DATOS/DATOS_ENTRADA/`**: Archivos TXT crudos de origen.
*   **`DATOS/DATOS_BRONCE/`**: Copia exacta en formato Parquet (con particionado Hive).
*   **`DATOS/DATOS_SILVER/`**: Datos limpios y tipados (Parquet).
*   **`DATOS/DATOS_GOLD/`**: Datos enriquecidos con reglas de negocio.

### Auditoría y Logs
*   **`ETL/LOG_ETL/ETL_EXECUTION.log`**: **Log Maestro**. Historial acumulativo.
*   **`ETL/AUDIT/LOGS_EVIDENCIA/`**: Reportes de integridad (CSV con BOM).

## 🛠️ Tecnologías

*   **R & Targets**: Orquestación modular.
*   **Apache Arrow**: Lectura veloz.
*   **DuckDB**: Motor SQL embebido.

## 👤 Autor
Anghello Vega
Técnico en laboratorio clínico, banco de sangre e imagenología.
Estudiante de Universidad Mayor, Ingeniería en Informática y Computación.
Gestor de datos del Departamento de Salud Municipal de Temuco, Chile.
Actualizado: 17-02-2026
