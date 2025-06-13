library(dplyr)
library(tidyr)    # Necesario para pivot_wider
library(magrittr) # Carga explícita para el operador pipe

# --- CONFIGURACIÓN ---
input_dir <- "input_cnvs" # Carpeta que contiene los archivos .csv de entrada
output_file <- "cnvs/cnvs.csv" # Archivo de salida consolidado
# Patrón para identificar el inicio de una tabla (usamos 'Sample Name' por seguridad)
header_pattern <- "^Sample Name,Target,Reference"

# --- CREAR CARPETAS SI NO EXISTEN ---
if (!dir.exists("cnvs")) {
  dir.create("cnvs")
}
if (!dir.exists(input_dir)) {
  stop(paste("La carpeta de entrada '", input_dir, "' no existe. Asegúrate de que los archivos .csv estén ahí.", sep = ""))
}

# --- PROCESAMIENTO GENERAL ---
# Obtener la lista de todos los archivos .csv en la carpeta input_dir
input_files <- list.files(input_dir, pattern = "\\.csv$", full.names = TRUE)

if (length(input_files) == 0) {
  stop(paste("No se encontraron archivos .csv en la carpeta:", input_dir))
}

cat("Iniciando procesamiento de archivos en:", input_dir, "\n")

# Lista para almacenar los dataframes procesados de cada archivo en formato largo
# Incluirá Sample ID, Target, CN.Value, SourceFile y OriginalLineNumber
all_long_data_frames <- list()

# Iterar sobre cada archivo de entrada
for (current_input_file in input_files) {
  current_file_name <- basename(current_input_file)
  cat(sprintf("\n--- Procesando archivo: %s ---\n", current_file_name))
  
  # CRITICAL: If any error occurs during this file's processing, the script will stop immediately.
  tryCatch({
    # 1. LEER TODO EL ARCHIVO COMO LÍNEAS DE TEXTO
    cat("  Paso 1: Leyendo el archivo completo...\n")
    all_lines <- readLines(current_input_file, warn = FALSE)
    
    # 2. ENCONTRAR LAS LÍNEAS DE INICIO DE CADA SECCIÓN
    start_indices <- grep(header_pattern, all_lines)
    
    if (length(start_indices) == 0) {
      stop(sprintf("ERROR: No se encontró ninguna sección que comenzara con el encabezado '%s' en el archivo '%s'. Revise el formato del archivo.", header_pattern, current_file_name))
    }
    cat(sprintf("  Paso 2: Se encontraron %d secciones de datos.\n", length(start_indices)))
    
    # 3. EXTRAER CADA SECCIÓN Y CONVERTIRLA A UN DATAFRAME
    list_of_dataframes_for_current_file <- list()
    
    for (i in 1:length(start_indices)) {
      
      current_start <- start_indices[i]
      cat(sprintf("    - Procesando sección %d (comienza en la línea %d)...\n", i, current_start))
      
      section_end_line <- length(all_lines)
      # Encontrar el final de la sección actual (3 líneas vacías)
      if (length(all_lines) >= 3) {
        for (j in (current_start + 1):(length(all_lines) - 2)) { # Empezar a buscar DESPUÉS del encabezado
          is_line1_empty <- trimws(all_lines[j]) == ""
          is_line2_empty <- trimws(all_lines[j + 1]) == ""
          is_line3_empty <- trimws(all_lines[j + 2]) == ""
          
          if (is_line1_empty && is_line2_empty && is_line3_empty) {
            section_end_line <- j - 1
            cat(sprintf("      ... final de la sección encontrado en la línea %d.\n", section_end_line))
            break
          }
        }
      } else {
        section_end_line <- length(all_lines)
      }
      
      # --- Separar el encabezado de las filas de datos ---
      header_line_text <- all_lines[current_start]
      data_lines_text <- if (section_end_line >= (current_start + 1)) {
        all_lines[(current_start + 1):section_end_line]
      } else {
        NULL # No hay filas de datos en esta sección
      }
      
      # --- Determinar colClasses dinámicamente y obtener nombres de columna ---
      # Leer SOLO el encabezado para obtener los nombres de columna procesados por R (ej. 'CN Predicted' -> 'CN.Predicted')
      header_df_temp <- read.csv(text = header_line_text, header = TRUE, stringsAsFactors = FALSE)
      col_names_in_r <- names(header_df_temp)
      
      # Preparar colClasses: inicialmente todo como character, luego forzar CN.Predicted/Calculated
      col_types_list <- rep("character", length(col_names_in_r))
      names(col_types_list) <- col_names_in_r
      
      cn_col_name_in_r <- NULL # Para almacenar el nombre real de la columna CN en R (ej., CN.Predicted)
      if ("CN.Predicted" %in% col_names_in_r) {
        col_types_list["CN.Predicted"] <- "numeric"
        cn_col_name_in_r <- "CN.Predicted"
      } else if ("CN.Calculated" %in% col_names_in_r) {
        col_types_list["CN.Calculated"] <- "numeric"
        cn_col_name_in_r <- "CN.Calculated"
      } else {
        stop(sprintf("ERROR: Ni 'CN Predicted' ni 'CN Calculated' encontrados en el encabezado de la sección %d del archivo '%s' (línea de encabezado: %d). Revise el encabezado.", i, current_file_name, current_start))
      }
      
      # Si no hay filas de datos para esta sección, añadir un dataframe vacío y saltar
      if (is.null(data_lines_text) || length(data_lines_text) == 0) {
        cat(sprintf("      Advertencia: No se encontraron filas de datos en la sección %d del archivo '%s' (línea de encabezado: %d). Saltando esta sección.\n", i, current_file_name, current_start))
        # Crear un dataframe vacío con las columnas correctas pero 0 filas
        empty_df <- data.frame(matrix(ncol = length(col_names_in_r), nrow = 0))
        names(empty_df) <- col_names_in_r
        empty_df$OriginalLineNumber <- integer(0) # Asegurar tipo numérico para el número de línea
        list_of_dataframes_for_current_file[[i]] <- empty_df
        next # Saltar a la siguiente sección
      }
      
      # --- Leer las filas de datos con header = FALSE, usando los colClasses determinados ---
      temp_df <- suppressWarnings(
        read.csv(text = data_lines_text, header = FALSE, stringsAsFactors = FALSE, colClasses = col_types_list)
      )
      
      # Asignar los nombres de columna correctos (procesados por R) al dataframe de datos
      names(temp_df) <- col_names_in_r
      
      # Verificar explícitamente si la columna CN se leyó correctamente como numérica.
      # `!is.numeric()` captura si la coerción falló completamente.
      # `any(!is.na(temp_df[[cn_col_name_in_r]]))` asegura que no sea una columna completamente NA.
      if (!is.null(cn_col_name_in_r) && !is.numeric(temp_df[[cn_col_name_in_r]]) && any(!is.na(temp_df[[cn_col_name_in_r]]))) {
        # Capturar y mostrar algunos de los valores problemáticos
        problematic_values <- paste(head(temp_df[[cn_col_name_in_r]][!is.numeric(temp_df[[cn_col_name_in_r]]) & !is.na(temp_df[[cn_col_name_in_r]])], 5), collapse = ", ")
        stop(sprintf("ERROR: La columna '%s' en la sección %d del archivo '%s' contiene valores no numéricos que no pudieron ser convertidos a NA. Revise las líneas %d a %d. Valores problemáticos detectados (primeros 5): %s",
                     cn_col_name_in_r, i, current_file_name, current_start + 1, section_end_line, problematic_values))
      }
      
      # Calcular el número de línea original para cada fila de datos.
      # El encabezado está en current_start. Los datos comienzan en current_start + 1.
      temp_df$OriginalLineNumber <- (current_start + 1) + (0:(nrow(temp_df) - 1))
      
      list_of_dataframes_for_current_file[[i]] <- temp_df
    } # Fin del bucle for para secciones
    
    # 4. COMBINAR TODOS LOS DATAFRAMES DE LA LISTA TEMPORAL EN UNO SOLO
    cat("  Paso 3: Combinando todas las secciones de este archivo en una sola tabla...\n")
    combined_data <- dplyr::bind_rows(list_of_dataframes_for_current_file)
    
    # 5. Seleccionar solo las columnas relevantes Y renombrar 'Sample.Name'
    cat("  Paso 4: Seleccionando las columnas 'Sample.Name', 'Target', 'CN.Value' (CN.Predicted/Calculated), 'OriginalLineNumber' y renombrando 'Sample.Name' a 'Sample ID'...\n")
    
    # Re-identificar la columna de valores CN en combined_data (debería ser consistente ahora)
    cn_value_col <- NULL
    if ("CN.Predicted" %in% names(combined_data)) {
      cn_value_col <- "CN.Predicted"
    } else if ("CN.Calculated" %in% names(combined_data)) {
      cn_value_col <- "CN.Calculated"
    } else {
      stop(sprintf("ERROR: Ni 'CN.Predicted' ni 'CN.Calculated' encontrados después de la combinación de secciones del archivo '%s'. Esto es una inconsistencia interna o un error previo no detectado.", current_file_name))
    }
    
    selected_data <- combined_data %>%
      select(
        `Sample.Name`,
        Target,
        !!sym(cn_value_col), # Usar !!sym() para la selección dinámica de columnas con dplyr
        OriginalLineNumber
      ) %>%
      rename(`Sample ID` = `Sample.Name`, `CN.Value` = !!sym(cn_value_col)) # Renombrar a CN.Value genérico
    
    # Añadir el nombre del archivo de origen como una columna
    selected_data$SourceFile <- current_file_name
    
    # Si todos los pasos anteriores pasaron sin errores, añadir los datos procesados a la lista global
    all_long_data_frames[[current_file_name]] <- selected_data
    
    cat(sprintf("  Archivo %s procesado con éxito. Se obtuvieron %d filas en formato largo.\n", current_file_name, nrow(selected_data)))
    
  }, error = function(e) {
    # CRÍTICO: Detener el script inmediatamente y reportar el error con el nombre del archivo
    stop(sprintf("ERROR CRÍTICO DETECTADO EN EL ARCHIVO '%s': %s", current_input_file, e$message))
  })
}

# --- Consolidación y Validación Final ---
# Este bloque solo se ejecutará si *todos* los archivos individuales se procesaron sin errores.

# Verificar si hay algún dato para consolidar después de procesar todos los archivos
if (length(all_long_data_frames) == 0) {
  stop("ERROR: No se pudo procesar ningún archivo exitosamente. No se generó 'cnvs.csv'. Revise los errores críticos reportados anteriormente.")
}

# Combinar todos los dataframes largos de todos los archivos
consolidated_long_data <- bind_rows(all_long_data_frames)

# --- CONTROL DE DUPLICADOS DE SAMPLE ID + TARGET + CN.Value ---
cat("\n--- Validando inconsistencias en 'CN.Value' para 'Sample ID' y 'Target' duplicados ---\n")

# Identificar duplicados con valores de CN.Value diferentes
inconsistent_duplicates <- consolidated_long_data %>%
  group_by(`Sample ID`, Target) %>%
  filter(n_distinct(`CN.Value`) > 1) %>% # Hay más de un valor único de CN.Value para esta combinación
  ungroup()

if (nrow(inconsistent_duplicates) > 0) {
  # Preparar el informe de inconsistencias incluyendo los números de línea
  inconsistency_report <- inconsistent_duplicates %>%
    select(`Sample ID`, Target, `CN.Value`, SourceFile, OriginalLineNumber) %>%
    arrange(`Sample ID`, Target, SourceFile, OriginalLineNumber)
  
  # Imprimir un resumen de las inconsistencias y detener el script
  stop(sprintf("\nERROR CRÍTICO: Se encontraron %d filas con inconsistencias (Sample ID y Target con CN.Value diferentes) entre los archivos procesados. Por favor, revise los datos fuente.\nDetalles de las inconsistencias:\n%s",
               nrow(inconsistency_report), paste(capture.output(as.data.frame(inconsistency_report)), collapse = "\n")))
}
cat("Paso de validación: No se encontraron inconsistencias en 'CN.Value' para 'Sample ID' y 'Target' duplicados. Eliminando redundancias...\n")

# Si no hay inconsistencias, eliminar las entradas redundantes (Sample ID + Target + CN.Value idénticos)
processed_long_data <- consolidated_long_data %>%
  distinct(`Sample ID`, Target, `CN.Value`, .keep_all = FALSE)

# 1. Pivotar a formato ancho
cat("Paso consolidado 1: Pivotando 'Target' a nuevas columnas y rellenando con 'CN.Value'...\n")
final_cnvs_db <- processed_long_data %>%
  pivot_wider(
    names_from = Target,
    values_from = `CN.Value`,
    id_cols = `Sample ID`,
    names_repair = "minimal"
  )

# 2. Aplicar el filtro final (si alguna columna de CN tiene un valor)
cat("Paso consolidado 2: Filtrando filas con datos insuficientes en la tabla final...\n")
final_cnvs_db <- final_cnvs_db %>%
  filter(rowSums(!is.na(select(., -`Sample ID`))) > 0)

# 3. Guardar el resultado final consolidado
write.csv(final_cnvs_db, output_file, row.names = FALSE, na = "")

cat(sprintf("\n¡PROCESO DE CONSOLIDACIÓN COMPLETADO CON ÉXITO!\n"))
cat(sprintf("Se procesaron %d archivos CSV sin errores críticos.\n", length(input_files)))
cat(sprintf("La tabla final consolidada contiene %d filas y se ha guardado en '%s'.\n", nrow(final_cnvs_db), output_file))