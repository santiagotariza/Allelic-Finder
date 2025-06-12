library(dplyr)

# --- Configuración de las carpetas y archivos ---
output_dir <- "output"
cnvs_file <- "cnvs/cnvs.csv" # Asumiendo que cnvs.csv está en una carpeta 'cnvs'
output_processed_dir <- "output_processed" # Nueva carpeta para guardar los resultados procesados

# --- Crear la carpeta de salida procesada si no existe ---
if (!dir.exists(output_processed_dir)) {
  dir.create(output_processed_dir)
}

# --- Cargar el archivo cnvs.csv ---
cnvs_db <- read.csv(cnvs_file, stringsAsFactors = FALSE)

# Renombrar la primera columna de cnvs_db para facilitar la comparación
names(cnvs_db)[1] <- "Sample_ID"

# --- Obtener la lista de archivos de salida en la carpeta 'output' ---
output_files <- list.files(output_dir, pattern = "_matches\\.csv$", full.names = TRUE)

cat("Iniciando procesamiento de archivos en:", output_dir, "\n")

# --- Iterar sobre cada archivo de coincidencias ---
for (file_path in output_files) {
  cat("\nProcesando archivo:", basename(file_path), "\n")
  
  # Leer el archivo de coincidencias
  matches_df <- read.csv(file_path, stringsAsFactors = FALSE)
  
  # --- Paso 1: Clasificar filas según si su Sample_ID existe en cnvs.csv ---
  # Filas cuyo Sample_ID NO está en cnvs.csv (estas se conservan tal cual)
  rows_not_in_cnvs <- matches_df %>%
    anti_join(cnvs_db, by = "Sample_ID")
  
  # Filas cuyo Sample_ID SÍ está en cnvs.csv (a estas se les aplica el filtro)
  rows_in_cnvs <- matches_df %>%
    semi_join(cnvs_db, by = "Sample_ID")
  
  cat("  Filas con 'Sample_ID' no encontrado en cnvs.csv (se conservan):", nrow(rows_not_in_cnvs), "\n")
  cat("  Filas con 'Sample_ID' encontrado en cnvs.csv (se filtrarán):", nrow(rows_in_cnvs), "\n")
  
  # --- Paso 2: Aplicar el filtro a las filas que SÍ están en cnvs.csv ---
  final_filtered_rows_in_cnvs <- data.frame()
  
  if (nrow(rows_in_cnvs) > 0) {
    for (i in 1:nrow(rows_in_cnvs)) {
      current_row <- rows_in_cnvs[i, ]
      sample_id_to_check <- current_row$Sample_ID
      
      cnv_row <- cnvs_db %>%
        filter(Sample_ID == sample_id_to_check)
      
      if (nrow(cnv_row) > 1) {
        cat("  Advertencia: Múltiples entradas para Sample_ID", sample_id_to_check, "en cnvs.csv. Usando la primera.\n")
        cnv_row <- cnv_row[1, ]
      }
      
      cnv_reference_value <- cnv_row[[3]]
      
      val_col4 <- current_row[[4]]
      val_col5 <- current_row[[5]]
      val_col6 <- current_row[[6]]
      
      # --- Lógica de filtrado: mantener si alguna es NA o si alguna coincide con referencia ---
      is_na_in_any_col <- is.na(val_col4) || is.na(val_col5) || is.na(val_col6)
      
      matches_reference <- FALSE
      if (!is.na(val_col4) && val_col4 == cnv_reference_value) matches_reference <- TRUE
      if (!is.na(val_col5) && val_col5 == cnv_reference_value) matches_reference <- TRUE
      if (!is.na(val_col6) && val_col6 == cnv_reference_value) matches_reference <- TRUE
      
      if (is_na_in_any_col || matches_reference) {
        final_filtered_rows_in_cnvs <- bind_rows(final_filtered_rows_in_cnvs, current_row)
      }
    }
    cat("  Filas de 'Sample_ID's encontrados en cnvs.csv que cumplen el filtro:", nrow(final_filtered_rows_in_cnvs), "\n")
  } else {
    cat("  No hay filas con 'Sample_ID's encontrados en cnvs.csv para filtrar.\n")
  }
  
  
  # --- Paso 3: Combinar las filas que se conservan de ambos grupos ---
  # Combinar las filas que no estaban en cnvs.csv con las filas que sí estaban y pasaron el filtro.
  final_result_df <- bind_rows(rows_not_in_cnvs, final_filtered_rows_in_cnvs)
  
  # --- Guardar el archivo procesado si hay resultados ---
  if (nrow(final_result_df) > 0) {
    output_file_name <- basename(file_path)
    output_file_name_filtered <- paste0(tools::file_path_sans_ext(output_file_name), "_filtered.csv")
    
    write.csv(final_result_df, file.path(output_processed_dir, output_file_name_filtered), row.names = FALSE)
    cat("  Archivo procesado generado:", output_file_name_filtered, "con", nrow(final_result_df), "filas totales.\n")
  } else {
    cat("  No se encontraron filas que cumplan los criterios de filtrado para", basename(file_path), "\n")
  }
}

cat("\nProcesamiento de todos los archivos de salida completado.\n")