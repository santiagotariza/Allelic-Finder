library(tidyverse)
library(fs)
library(data.table)  # Para usar fwrite

# Ruta a la carpeta 'samples'
samples_dir <- "samples"

# Crear una lista de todos los archivos .zip en la carpeta 'samples'
zip_files <- dir_ls(samples_dir, glob = "*.zip")

# Iterar sobre cada archivo .zip
for (zip_file in zip_files) {
  
  # Obtener el nombre del archivo sin extensión
  zip_name <- path_file(zip_file)
  folder_name <- path_ext_remove(zip_name)
  folder_path <- file.path(samples_dir, folder_name)
  
  # Crear una carpeta para el contenido descomprimido si no existe
  if (!dir.exists(folder_path)) {
    dir.create(folder_path)
  }
  
  # Descomprimir el contenido del .zip en la carpeta creada
  unzip(zip_file, exdir = folder_path)
  
  # Construir la ruta al archivo 'Genotype Matrix.csv'
  csv_file <- file.path(folder_path, "Genotype Matrix.csv")
  
  # Verificar si el archivo existe
  if (file.exists(csv_file)) {
    
    # Leer el archivo CSV sin encabezado
    genotype_data <- read.csv(csv_file, comment.char = "#", stringsAsFactors = FALSE)
    
    # Verificar si el dataframe no está vacío
    if (nrow(genotype_data) > 0) {
      
      # Identificar la fila donde aparece "Empty" y eliminar desde esa fila en adelante
      empty_row <- which(genotype_data == "Empty", arr.ind = TRUE)[1]
      if (!is.na(empty_row)) {
        genotype_data <- genotype_data[1:(empty_row - 1), ]
      }
      
      # Convertir todas las columnas a tipo caracter para evitar problemas con factores
      genotype_data <- data.frame(lapply(genotype_data, as.character), stringsAsFactors = FALSE)
      
      # Reemplazar el encabezado del dataframe original con nombres "limpios"
      # Esto genera los .1 automáticamente si hay duplicados
      colnames(genotype_data) <- sub("_.*", "", colnames(genotype_data))
      
      # Modificar la primera fila de `genotype_data` (para los encabezados secundarios)
      # Nota: Esta línea no parece tener efecto en el resultado final si ya los _.* se eliminaron.
      # Si esta línea es importante por algún motivo, por favor házmelo saber.
      # genotype_data[1, ] <- sub("^[^_]*_", "", genotype_data[1, ])
      
      # Ordenar las columnas desde la segunda en adelante (alfabéticamente)
      genotype_data <- genotype_data[, c(1, order(colnames(genotype_data)[-1]) + 1)]
      
      # Identificar todas las columnas base (ej. rs1234) que tienen su contraparte .1 (ej. rs1234.1)
      base_rsids_with_variants <- unique(
        sub("\\.1$", "", # Quitar el .1 para obtener la base
            grep("^rs[0-9]+\\.1$", colnames(genotype_data), value = TRUE) # Buscar solo las columnas que terminan en .1
        )
      )
      
      # Dataframe para almacenar los resultados de la fusión (rsID_temp) y las columnas originales
      # Esto actúa como tu 'data_temp' de forma más directa y eficiente
      merged_genotypes <- genotype_data[, 1, drop = FALSE] # Mantener la primera columna (Sample.Assay)
      cols_to_remove_from_genotype_data <- c() # Para guardar las columnas .1 a eliminar al final
      
      # --- Función para aplicar la lógica de fusión a cada par de valores ---
      merge_genotypes <- function(val_rsid, val_rsid_dot1) {
        # Función auxiliar para verificar si un valor es un genotipo válido
        is_valid_genotype <- function(val) {
          !(val %in% c("NOAMP", "UND", "") || is.na(val))
        }
        
        val_rsid_is_valid <- is_valid_genotype(val_rsid)
        val_rsid_dot1_is_valid <- is_valid_genotype(val_rsid_dot1)
        
        if (val_rsid_is_valid && val_rsid_dot1_is_valid) {
          # Ambos son genotipos válidos
          if (val_rsid == val_rsid_dot1) {
            return(val_rsid) # Son iguales, devolver ese valor
          } else {
            return(val_rsid) # Son válidos pero diferentes, devolver el de rsID
          }
        } else if (val_rsid_is_valid) {
          return(val_rsid) # Solo rsID es válido
        } else if (val_rsid_dot1_is_valid) {
          return(val_rsid_dot1) # Solo rsID.1 es válido
        } else {
          return(val_rsid) # Ambos son inválidos (NOAMP/UND/NA), devolver el de rsID
        }
      }
      
      # Iterar sobre cada rsID base que tiene una variante .1
      for (base_col in base_rsids_with_variants) {
        variant_col <- paste0(base_col, ".1")
        
        # Verificar que ambas columnas existen antes de intentar fusionar
        if (base_col %in% colnames(genotype_data) && variant_col %in% colnames(genotype_data)) {
          # Aplicar la función de fusión fila por fila
          # Usamos mapply para aplicar la función a vectores completos de las columnas
          merged_result <- mapply(merge_genotypes, 
                                  genotype_data[[base_col]], 
                                  genotype_data[[variant_col]], 
                                  SIMPLIFY = TRUE, USE.NAMES = FALSE)
          
          # Asignar el resultado de la fusión a la columna base en el dataframe de resultados
          merged_genotypes[[base_col]] <- merged_result
          
          # Marcar la columna .1 para su eliminación posterior del dataframe original
          cols_to_remove_from_genotype_data <- c(cols_to_remove_from_genotype_data, variant_col)
          
        } else if (base_col %in% colnames(genotype_data)) {
          # Si solo existe la columna base y no hay variante .1, simplemente copiarla
          merged_genotypes[[base_col]] <- genotype_data[[base_col]]
        }
        # Si solo existiera la variante .1 y no la base, no la estamos manejando aquí
        # porque 'base_rsids_with_variants' solo busca bases que *tienen* una .1
      }
      
      # Añadir columnas que no son rsID y que no se han procesado ya (ej. Sample.Assay ya está, pero otras no-rsID?)
      # Y también añadir los rsID base que no tuvieron una variante .1 para fusionar
      
      # Columnas rsID base que no tienen una variante .1
      all_rs_base_cols <- grep("^rs[0-9]+$", colnames(genotype_data), value = TRUE)
      rs_cols_without_variants <- setdiff(all_rs_base_cols, base_rsids_with_variants)
      
      for(col in rs_cols_without_variants) {
        merged_genotypes[[col]] <- genotype_data[[col]]
      }
      
      # Otras columnas (no rsID) que no son Sample.Assay
      other_non_rs_cols <- setdiff(colnames(genotype_data), c(all_rs_base_cols, cols_to_remove_from_genotype_data, colnames(merged_genotypes)))
      for(col in other_non_rs_cols) {
        merged_genotypes[[col]] <- genotype_data[[col]]
      }
      
      # --- Reemplazar el dataframe original con el fusionado ---
      # Primero, eliminar todas las columnas rsID y rsID.1 del original
      # Esto es para asegurar que solo queden las columnas no-rsID para el merge final
      # Y luego fusionar con 'merged_genotypes'
      
      # Obtener solo las columnas que NO son rsID
      non_rs_columns_final <- grep("^rs[0-9]+", colnames(genotype_data), invert = TRUE, value = TRUE)
      final_data <- genotype_data[, non_rs_columns_final, drop = FALSE]
      
      # Ahora, fusionar con las columnas rsID procesadas
      # Asegurarse de que Sample.Assay se maneje correctamente y no se duplique si ya está en final_data
      if ("Sample.Assay" %in% colnames(final_data) && "Sample.Assay" %in% colnames(merged_genotypes)) {
        merged_genotypes <- merged_genotypes %>% select(-`Sample.Assay`) # Eliminar de merged_genotypes para evitar duplicado en bind_cols
      }
      
      genotype_data <- bind_cols(final_data, merged_genotypes)
      
      # Ordenar las columnas finales (excluyendo la primera si es Sample.Assay o similar)
      # Identifica la primera columna (probablemente Sample.Assay)
      first_col_name <- colnames(genotype_data)[1]
      
      # Obtiene el resto de las columnas
      other_cols <- setdiff(colnames(genotype_data), first_col_name)
      
      # Reordena alfabéticamente las demás columnas y combina
      genotype_data <- genotype_data[, c(first_col_name, sort(other_cols))]
      
      # Crear el nuevo nombre del archivo agregando el nombre del ZIP al inicio
      output_csv <- file.path(samples_dir, paste0(folder_name, "_Genotype_Matrix_short.csv"))
      
      # Guardar los archivos procesados
      fwrite(genotype_data, output_csv)
      
      message("Archivos procesados y guardados:")
      message(" - Genotype Data: ", output_csv)
    } else {
      message("El archivo está vacío o no contiene datos: ", csv_file)
    }
    
  } else {
    message("No se encontró el archivo 'Genotype Matrix.csv' en ", folder_path)
    # Listar archivos en la carpeta para depurar
    print(dir_ls(folder_path))
  }
  
  # Eliminar la carpeta descomprimida
  unlink(folder_path, recursive = TRUE)
  message("Carpeta eliminada: ", folder_path)
}

message("Procesamiento completado.")