library(dplyr)

# Configuración de las carpetas
samples_dir <- "samples"
db_file <- "db/db.csv"
output_dir <- "output"

# Crear la carpeta de salida si no existe
if (!dir.exists(output_dir)) {
  dir.create(output_dir)
}

# Cargar el archivo db.csv
# Asegúrate de que las columnas relevantes se lean como caracteres
db <- read.csv(db_file, stringsAsFactors = FALSE)

# Convertir todas las columnas de genotipo en db a caracteres para consistencia
# Y TRATAR CELDAS VACÍAS COMO NA AQUÍ TAMBIÉN
for (col_name in names(db)[-c(1:5)]) { # Asumo que las primeras 5 columnas no son genotipos
  db[[col_name]] <- as.character(db[[col_name]])
  # Reemplazar celdas vacías por NA en db.csv
  db[[col_name]][db[[col_name]] == ""] <- NA
}

# Obtener la lista de archivos en la carpeta samples
sample_files <- list.files(samples_dir, pattern = "\\.csv$", full.names = TRUE)

# Iterar sobre cada archivo en la carpeta samples
for (sample_file in sample_files) {
  # Leer el archivo de muestra
  # Asegúrate de que las columnas se lean como caracteres
  sample <- read.csv(sample_file, stringsAsFactors = FALSE)
  
  # Convertir todas las columnas de genotipo en sample a caracteres para consistencia
  for (col_name in names(sample)[-1]) { # Asumo que la primera columna no es genotipo
    sample[[col_name]] <- as.character(sample[[col_name]])
  }
  
  # Extraer el nombre del archivo sin la ruta ni la extensión
  sample_name <- tools::file_path_sans_ext(basename(sample_file))
  
  # Identificar los rsIDs comunes
  sample_rsIDs <- names(sample)[-1]     # Columnas después de la primera en samples
  db_rsIDs <- names(db)[-c(1:5)]        # Columnas después de la quinta en db
  common_rsIDs <- intersect(sample_rsIDs, db_rsIDs)
  
  # Crear un marco de datos para almacenar los resultados
  matches <- data.frame()
  
  # Reemplazar todos los 'NOAMP' y 'UND' en sample por NA
  for (rsID_col in sample_rsIDs) {
    sample[[rsID_col]][sample[[rsID_col]] %in% c("NOAMP", "UND")] <- NA
  }
  
  # Comparar cada fila del archivo sample contra cada fila de db
  for (i in 1:nrow(sample)) {
    for (j in 1:nrow(db)) {
      # Inicializar una lista para almacenar los rsIDs que coinciden en esta iteración
      matched_rsIDs_for_this_comparison <- c()
      
      # Contador para los rsIDs no-NA en db que necesitamos que coincidan
      non_na_db_rsIDs_to_match <- 0
      # Contador para los rsIDs que realmente coinciden
      actual_matches_count <- 0
      
      # Iterar sobre los rsIDs comunes para comparar valores
      for (rsID in common_rsIDs) {
        sample_val <- sample[i, rsID]
        db_val <- db[j, rsID]
        
        # Considerar solo los rsIDs en db que NO sean NA para el match
        if (!is.na(db_val)) {
          non_na_db_rsIDs_to_match <- non_na_db_rsIDs_to_match + 1
          
          # Si los valores coinciden (ambos no NA y son iguales)
          # O si ambos son NA (significa que ambos estaban no informados/vacíos)
          if (identical(sample_val, db_val) || (is.na(sample_val) && is.na(db_val))) {
            actual_matches_count <- actual_matches_count + 1
            matched_rsIDs_for_this_comparison <- c(matched_rsIDs_for_this_comparison, rsID)
          }
        }
      }
      
      # Si el número de rsIDs que realmente coinciden es igual al número
      # de rsIDs no-NA en db que se necesitaban para el match, entonces es un match completo.
      # Y además, si hay al menos un rsID no-NA en db para comparar.
      if (non_na_db_rsIDs_to_match > 0 && actual_matches_count == non_na_db_rsIDs_to_match) {
        matches <- bind_rows(matches, data.frame(
          Sample_ID = sample[i, 1],
          db[j, c("Gene", "Genotype", "CYP2D6_INT2_Hs04083572_cn", 
                  "CYP2D6_INT6_Hs04502391_cn", "CYP2D6_ex9_Hs00010001_cn")],
          rsIDs_matched = paste(matched_rsIDs_for_this_comparison, collapse = ", "),
          stringsAsFactors = FALSE
        ))
      }
    }
  }
  
  # Guardar el archivo de coincidencias si hay resultados
  if (nrow(matches) > 0) {
    write.csv(matches, file.path(output_dir, paste0(sample_name, "_matches.csv")), row.names = FALSE)
    cat("Archivo generado:", paste0(sample_name, "_matches.csv"), "con", nrow(matches), "coincidencias.\n")
  } else {
    cat("No se encontraron coincidencias para", sample_name, "\n")
  }
}

cat("Procesamiento de todos los archivos de muestra completado.\n")