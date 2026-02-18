# Estructura del Proyecto ELT (Targets)

## 🧹 Limpieza y Modularización
Se ha migrado el antiguo script monolítico `functions_etl.R` a módulos independientes para mejorar la mantenibilidad y seguir el patrón Medallion.

## 📂 Archivos Activos y Su Función

### 1. `ETL/00_run_etl.R` (El Orquestador) 🟢
- **Función:** Es el punto de entrada único.
- **Qué hace:** Carga las librerías, ejecuta el pipeline con `targets::tar_make()`, maneja errores y escribe en el Log Maestro (`ETL/LOG_ETL/ETL_EXECUTION.log`).
- **Uso:** `source("ETL/00_run_etl.R")`

### 2. `_targets.R` (El Plano) 🗺️
- **Función:** Define el grafo de dependencias del pipeline.
- **Qué hace:** Carga los módulos de funciones (`R/*.R`) y especifica el flujo de trabajo (Extract -> Bronze -> Silver -> Gold -> Audit).

### 3. `R/*.R` (El Motor Modular) ⚙️
La lógica de negocio ahora está separada por capas:

*   **`R/etl_extract.R`** (Capa Bronze): Ingesta de archivos TXT a Parquet crudo.
*   **`R/etl_transform.R`** (Capa Silver): Limpieza, tipado y estandarización de datos.
*   **`R/etl_gold.R`** (Capa Gold): Genera **Dataset Monolítico Final** (sin duplicados) y mueve registros repetidos (hash SHA256) a **Cuarentena**.
*   **R/etl_audit.R** (Auditoría): Generación de reportes de calidad e integridad.

## 🚀 Orden de Ejecución
1. Usted ejecuta `ETL/00_run_etl.R`.
2. Este script llama a `targets`.
3. `targets` lee `_targets.R`, carga las funciones de la carpeta `R/` y verifica cambios.
4. Se ejecutan solo los pasos necesarios.
5. Se registra todo en el Log Maestro.
