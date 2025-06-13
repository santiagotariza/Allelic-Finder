# Cargar librerías necesarias
library(readr)
library(dplyr)
library(stringr)

# --- 1. Configuración de Rutas y Carpetas ---
input_folder <- "input_db"
output_folder <- "db"
output_file_name <- "db.csv"

# Crear la carpeta de salida si no existe
if (!dir.exists(output_folder)) {
  dir.create(output_folder)
}

# Encontrar el archivo de entrada (busca el primer archivo que coincida con "AT*.csv")
input_file_path <- list.files(path = input_folder, pattern = "^AT.*\\.csv$", full.names = TRUE)

if (length(input_file_path) == 0) {
  stop(paste0("Error: No se encontró ningún archivo que coincida con 'AT*.csv' en la carpeta '", input_folder, "'."))
} else if (length(input_file_path) > 1) {
  warning(paste0("Advertencia: Se encontraron varios archivos que coinciden con 'AT*.csv'. Usando el primero: ", input_file_path[1]))
  input_file_path <- input_file_path[1]
}

output_file_path <- file.path(output_folder, output_file_name)

# --- 2. Leer Todas las Líneas y Preprocesar ---

# Leer todas las líneas del archivo de entrada como un vector de caracteres
all_lines <- readLines(input_file_path)

if (length(all_lines) < 18) {
  stop("Error: El archivo de entrada no tiene suficientes líneas para las operaciones de eliminación especificadas y para contener un encabezado.")
}

# Determinar el número máximo de columnas esperado (ajústalo si es diferente a 79)
# num_expected_cols <- 79

# Leer el archivo completo como un dataframe inicial, asegurando que se leen todas las columnas como caracteres
raw_df <- read_csv(
  input_file_path,
  col_names = FALSE,
  col_types = cols(.default = "c"),
  skip = 0,
  n_max = -1
)
# 
# # Rellenar con columnas vacías si raw_df tiene menos de las columnas esperadas
# if (ncol(raw_df) < num_expected_cols) {
#   warning(paste0("Advertencia: El archivo se leyó con ", ncol(raw_df), " columnas, pero se esperaban ", num_expected_cols, ". Añadiendo columnas vacías si es necesario."))
#   missing_cols <- num_expected_cols - ncol(raw_df)
#   for (i in 1:missing_cols) {
#     raw_df[[paste0("V", ncol(raw_df) + 1)]] <- NA_character_
#   }
# }

# Reemplazar el valor de la celda de la fila 10, columna 1, por 'Gene'
raw_df[10, 1] <- "Gene"

# Reemplazar los valores de las columnas 2 a 5 de la fila 9 por los de la fila 10 de las mismas columnas
raw_df[9, 2:5] <- raw_df[10, 2:5]

# Identificar el nuevo encabezado (fila 9) y los datos (desde la fila 11 en adelante)
new_header_row_vec <- as.character(raw_df[9, ])
data_rows <- raw_df[c(11:nrow(raw_df)), ]

# Aplicar la limpieza de nombres al vector de encabezados
clean_header_names <- str_replace_all(new_header_row_vec, "_.*$", "")

# Manejar nombres de columna duplicados y conservar solo la primera
unique_col_indices <- !duplicated(clean_header_names)
final_header_names <- clean_header_names[unique_col_indices]
final_df <- data_rows[, unique_col_indices]

# Asignar los nombres de columna limpios y únicos al dataframe final
colnames(final_df) <- final_header_names

# --- 3. Normalizar valores de la tabla (después de la columna 5, desde la fila 1) ---

# Función para normalizar un valor (ej. "G/A" a "A/G", "TTC/CTT" a "CTT/TTC")
normalize_allele <- function(value) {
  if (is.na(value) || !str_detect(value, "/")) {
    return(value) # Retorna el valor original si es NA o no tiene '/'
  }
  parts <- str_split(value, "/", simplify = TRUE)
  sorted_parts <- sort(parts)
  return(paste(sorted_parts, collapse = "/"))
}

# Aplicar la normalización a las columnas a partir de la columna 6
# Usamos `mutate(across())` de dplyr para aplicar la función a múltiples columnas
final_df <- final_df %>%
  mutate(across(6:ncol(.), ~sapply(., normalize_allele)))

# --- 4. Escribir el Archivo CSV Final ---
write_csv(final_df, output_file_path)

cat(paste0("\nProcesamiento completado y archivo guardado en '", output_file_path, "'\n"))
#print(head(final_df))
print(paste0("El dataframe final tiene ", ncol(final_df), " columnas."))