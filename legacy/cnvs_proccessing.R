library(dplyr)
library(tidyr)    # Needed for pivot_wider
library(magrittr) # Explicitly load for the pipe operator

# --- CONFIGURACIÓN ---
input_file <- "input_cnvs/Results_CNV.csv"
output_file <- "cnvs/cnvs.csv"
header_pattern <- "^Sample Name,Target,Reference" # Patrón para identificar el inicio de una tabla

# --- PROCESAMIENTO ---
tryCatch({
  
  # 1. LEER TODO EL ARCHIVO COMO LÍNEAS DE TEXTO
  cat("Paso 1: Leyendo el archivo completo...\n")
  all_lines <- readLines(input_file, warn = FALSE)
  
  # 2. ENCONTRAR LAS LÍNEAS DE INICIO DE CADA SECCIÓN
  start_indices <- grep(header_pattern, all_lines)
  
  if (length(start_indices) == 0) {
    stop(paste("No se encontró ninguna sección que comenzara con el encabezado:", header_pattern))
  }
  cat(sprintf("Paso 2: Se encontraron %d secciones de datos.\n", length(start_indices)))
  
  # 3. EXTRAER CADA SECCIÓN Y CONVERTIRLA A UN DATAFRAME
  list_of_dataframes <- list() # Crearemos una lista para guardar cada tabla extraída
  
  for (i in 1:length(start_indices)) {
    
    current_start <- start_indices[i]
    cat(sprintf("  - Procesando sección %d (comienza en la línea %d)...\n", i, current_start))
    
    # Definir el rango de búsqueda para el final de esta sección.
    # Comienza justo después del encabezado actual y va hasta el final del archivo.
    search_range_start <- current_start + 1
    
    # Encontrar el final de la sección actual (3 líneas vacías)
    section_end_line <- length(all_lines) # Por defecto, el final del archivo
    
    for (j in search_range_start:(length(all_lines) - 2)) {
      # Comprobar si esta línea y las dos siguientes están vacías.
      # trimws() elimina espacios en blanco para que "  " también cuente como vacía.
      is_line1_empty <- trimws(all_lines[j]) == ""
      is_line2_empty <- trimws(all_lines[j + 1]) == ""
      is_line3_empty <- trimws(all_lines[j + 2]) == ""
      
      if (is_line1_empty && is_line2_empty && is_line3_empty) {
        # El final de los datos es la línea ANTERIOR a la primera de las 3 vacías.
        section_end_line <- j - 1
        cat(sprintf("      ... final de la sección encontrado en la línea %d.\n", section_end_line))
        break # Salir del bucle de búsqueda de final
      }
    }
    
    # Extraer las líneas que componen esta tabla (desde su encabezado hasta su final)
    section_lines <- all_lines[current_start:section_end_line]
    
    # Convertir este bloque de texto en un dataframe.
    # read.csv() puede leer directamente desde un vector de texto usando el argumento 'text'.
    # Usamos suppressWarnings para ignorar advertencias sobre "línea final incompleta" si ocurre.
    temp_df <- suppressWarnings(
      read.csv(text = section_lines, header = TRUE, stringsAsFactors = FALSE)
    )
    
    # Añadir el dataframe extraído a nuestra lista
    list_of_dataframes[[i]] <- temp_df
  }
  
  # 4. COMBINAR TODOS LOS DATAFRAMES DE LA LISTA EN UNO SOLO
  cat("Paso 3: Combinando todas las secciones en una sola tabla...\n")
  combined_data <- dplyr::bind_rows(list_of_dataframes)
  
  # 5. Seleccionar solo las columnas relevantes Y renombrar 'Sample.Name'
  cat("Paso 4: Seleccionando las columnas 'Sample.Name', 'Target', 'CN.Predicted' y renombrando 'Sample.Name' a 'Sample ID'...\n")
  selected_data <- combined_data %>%
    select(
      `Sample.Name`,
      Target,
      `CN.Predicted` # Ajusta este nombre si es diferente en tu CSV
    ) %>%
    rename(`Sample ID` = `Sample.Name`) # Renombra la columna aquí
  
  # --- NUEVO ENFOQUE: Transformar para lograr el formato deseado ---
  # 1. Crear un identificador de grupo para cada set de Targets por Sample ID
  cat("Paso 5: Creando un identificador de grupo para cada bloque de datos por Sample ID...\n")
  temp_data <- selected_data %>%
    group_by(`Sample ID`) %>% # Usar el nuevo nombre de columna
    mutate(group_id = ceiling(row_number() / length(unique(Target)))) %>%
    ungroup()
  
  # 2. Ahora sí, pivotar usando Sample ID y group_id como id_cols
  cat("Paso 6: Pivotando 'Target' a nuevas columnas y rellenando con 'CN.Predicted'...\n")
  pivoted_data <- temp_data %>%
    pivot_wider(
      names_from = Target,
      values_from = `CN.Predicted`,
      id_cols = c(`Sample ID`, group_id) # Usar el nuevo nombre de columna
    ) %>%
    select(-group_id) # Eliminar la columna group_id que solo sirvió para el pivoteo
  
  # 6. APLICAR EL FILTRO ORIGINAL
  cat("Paso 7: Filtrando filas con datos insuficientes...\n")
  filtered_data <- pivoted_data %>%
    filter(rowSums(!is.na(select(., -`Sample ID`))) > 0) # Usar el nuevo nombre de columna
  
  # 7. GUARDAR EL RESULTADO FINAL
  write.csv(filtered_data, output_file, row.names = FALSE, na = "")
  
  cat(sprintf("\n¡PROCESO COMPLETADO!\n"))
  cat(sprintf("Se extrajeron y combinaron datos de %d secciones.\n", length(list_of_dataframes)))
  cat(sprintf("La tabla final contiene %d filas y se ha guardado en '%s'.\n", nrow(filtered_data), output_file))
  
}, error = function(e) {
  cat(sprintf("\nERROR: %s\n", e$message))
})