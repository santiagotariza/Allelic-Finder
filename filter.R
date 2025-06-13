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
# Usar check.names = FALSE para asegurar que los nombres de las columnas 'HsXXXX_cn' se mantengan intactos
cnvs_db <- read.csv(cnvs_file, stringsAsFactors = FALSE, check.names = FALSE)

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
  
  # Columnas 'Hs' a considerar en matches_df
  hs_cols_to_check <- grep("^Hs\\d+_cn$", names(matches_df), value = TRUE)
  
  # Verificar si se encontraron columnas Hs válidas
  if (length(hs_cols_to_check) == 0) {
    cat("  Advertencia: No se encontraron columnas 'HsXXXX_cn' en", basename(file_path), ". Saltando este archivo.\n")
    next
  }
  
  # Dataframe para almacenar las filas que pasan el filtro
  filtered_matches_df <- data.frame()
  
  # Iterar sobre cada fila de matches_df
  for (i in 1:nrow(matches_df)) {
    current_row <- matches_df[i, ]
    sample_id_to_check <- current_row$Sample_ID
    
    # Extraer los valores de las columnas 'Hs' de la fila actual
    values_from_matches <- as.numeric(current_row[hs_cols_to_check]) # Asegurarse que son numéricos
    
    # --- Lógica de filtrado ---
    # Paso 1: Verificar si alguna de las columnas 'Hs' es NA en matches_df
    if (any(is.na(values_from_matches))) {
      # Si hay algún NA, la fila se conserva tal cual
      filtered_matches_df <- bind_rows(filtered_matches_df, current_row)
      next # Pasar a la siguiente fila
    }
    
    # Paso 2: Si no hay NAs, buscar el Sample_ID en cnvs_db
    cnv_row_reference <- cnvs_db %>%
      filter(Sample_ID == sample_id_to_check)
    
    # Si el Sample_ID no se encuentra en cnvs_db, la fila se conserva (similar a anti_join)
    if (nrow(cnv_row_reference) == 0) {
      filtered_matches_df <- bind_rows(filtered_matches_df, current_row)
      next # Pasar a la siguiente fila
    }
    
    # Si se encuentra, asegurar que solo hay una fila de referencia
    if (nrow(cnv_row_reference) > 1) {
      cat("  Advertencia: Múltiples entradas para Sample_ID", sample_id_to_check, "en cnvs.csv. Usando la primera.\n")
      cnv_row_reference <- cnv_row_reference[1, ]
    }
    
    # Obtener los valores de referencia de cnvs_db para las mismas columnas Hs
    # Debemos asegurarnos de que las columnas Hs de cnvs_db también existen y tienen el mismo nombre
    common_hs_cols <- intersect(hs_cols_to_check, names(cnv_row_reference))
    
    if (length(common_hs_cols) == 0) {
      cat("  Advertencia: No se encontraron columnas 'HsXXXX_cn' coincidentes entre matches_df y cnvs.csv para Sample_ID", sample_id_to_check, ". Fila conservada por precaución.\n")
      filtered_matches_df <- bind_rows(filtered_matches_df, current_row)
      next
    }
    
    # Verificar si TODOS los valores de 'matches_df' coinciden con sus homólogos en 'cnvs_db'
    all_match <- TRUE # Asumimos que todos coinciden hasta que se demuestre lo contrario
    for (j in 1:length(common_hs_cols)) {
      col_name <- common_hs_cols[j]
      val_matches <- current_row[[col_name]]
      val_cnvs <- cnv_row_reference[[col_name]]
      
      # Si hay NA en cualquiera de los valores (aunque ya filtramos NAs de matches_df,
      # podría haber en cnvs_db, o un tipo de dato diferente), o si no coinciden,
      # entonces 'all_match' es FALSE y podemos salir del bucle.
      if (is.na(val_matches) || is.na(val_cnvs) || val_matches != val_cnvs) {
        all_match <- FALSE
        break
      }
    }
    
    if (all_match) {
      # Si todas las columnas 'Hs' coinciden, la fila se conserva
      filtered_matches_df <- bind_rows(filtered_matches_df, current_row)
    }
  }
  
  # --- Guardar el archivo procesado si hay resultados ---
  if (nrow(filtered_matches_df) > 0) {
    output_file_name <- basename(file_path)
    output_file_name_filtered <- paste0(tools::file_path_sans_ext(output_file_name), "_filtered.csv")
    
    write.csv(filtered_matches_df, file.path(output_processed_dir, output_file_name_filtered), row.names = FALSE)
    cat("  Archivo procesado generado:", output_file_name_filtered, "con", nrow(filtered_matches_df), "filas totales.\n")
  } else {
    cat("  No se encontraron filas que cumplan los criterios de filtrado para", basename(file_path), "\n")
  }
}

cat("\nProcesamiento de todos los archivos de salida completado.\n")