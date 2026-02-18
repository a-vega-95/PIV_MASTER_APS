# Estructura del Proyecto ELT (Targets)

## Limpieza y Modularizacion
Se ha migrado el antiguo script monolitico `functions_etl.R` a modulos independientes para mejorar la mantenibilidad y seguir el patron Medallion.

## Archivos Activos y Su Funcion

### 1. `ETL/00_run_etl.R` (El Orquestador)
- **Funcion:** Es el punto de entrada unico.
- **Que hace:** Carga las librerias, ejecuta el pipeline con `targets::tar_make()`, maneja errores y escribe en el Log Maestro (`ETL/LOG_ETL/ETL_EXECUTION.log`).
- **Uso:** `source("ETL/00_run_etl.R")`

### 2. `_targets.R` (El Plano)
- **Funcion:** Define el grafo de dependencias del pipeline.
- **Que hace:** Carga los modulos de funciones (`R/*.R`) y especifica el flujo de trabajo (Extract -> Bronze -> Silver -> Gold -> Audit).

### 3. `R/*.R` (El Motor Modular)
La logica de negocio ahora esta separada por capas:

*   **`R/etl_extract.R`** (Capa Bronze): Ingesta de archivos TXT a Parquet crudo.
*   **`R/etl_transform.R`** (Capa Silver): Limpieza, tipado y estandarizacion de datos.
*   **`R/etl_gold.R`** (Capa Gold): Genera **Dataset Monolitico Final** (sin duplicados) y mueve registros repetidos (hash SHA256) a **Cuarentena**.
*   **R/etl_audit.R** (Auditoria): Generacion de reportes de calidad e integridad.

## Orden de Ejecucion
1. Usted ejecuta `ETL/00_run_etl.R`.
2. Este script llama a `targets`.
3. `targets` lee `_targets.R`, carga las funciones de la carpeta `R/` y verifica cambios.
4. Se ejecutan solo los pasos necesarios.
5. Se registra todo en el Log Maestro.
