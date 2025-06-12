# Install and load necessary packages if you haven't already
if (!requireNamespace("digest", quietly = TRUE)) {
  install.packages("digest")
}
library(digest) # For MD5 hashing

# --- Configuration ---
# Set the paths to the two CSV files you want to compare
# Replace "path/to/your_script_output.csv" and "path/to/my_script_output.csv"
# with the actual paths to your generated CSV files.


library(here)
file_path_1 <- here("samples/sample1_Genotype_Matrix_short.csv")
file_path_2 <- here("samples/sample1_Genotype_Matrix.csv")

# --- Script Logic ---

message("--- Comparing CSV File Contents Using Hashes ---")

# Check if the first file exists
if (!file.exists(file_path_1)) {
  stop("Error: File 1 not found at ", file_path_1)
} else {
  message(paste0("Found File 1: ", file_path_1))
}

# Check if the second file exists
if (!file.exists(file_path_2)) {
  stop("Error: File 2 not found at ", file_path_2)
} else {
  message(paste0("Found File 2: ", file_path_2))
}

# Generate MD5 hash for the content of the first file
message("\nGenerating hash for File 1...")
hash_1 <- digest(readLines(file_path_1), algo = "md5")
message(paste0("Hash for File 1: ", hash_1))

# Generate MD5 hash for the content of the second file
message("\nGenerating hash for File 2...")
hash_2 <- digest(readLines(file_path_2), algo = "md5")
message(paste0("Hash for File 2: ", hash_2))

# Compare the hashes
message("\n--- Comparison Result ---")
if (hash_1 == hash_2) {
  message("The contents of both CSV files are IDENTICAL.")
} else {
  message("The contents of the CSV files are DIFFERENT.")
  message("This indicates a discrepancy in processing or output formatting.")
}

message("\nComparison complete.")