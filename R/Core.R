library(R6)
library(DBI)
library(duckdb)

#' ===========================================================================
#' GLOBAL PREVIEW CONFIGURATION
#' ==========================================================================
#' Get the current preview limit from global options
#' @return Integer preview limit (default: 10)
get_preview_limit <- function() {
  list(
    rows = getOption("mydb.preview.rows", default = 30),
    cols = getOption("mydb.preview.cols", default = 20)
  )
}

#' Set the global preview limit for all MyDB objects
#' @param n Integer, number of rows to show in previews
set_preview_limit <- function(rows = NULL, cols = NULL) {
  # Get current limits
  current_limits <- get_preview_limit()

  # Update rows if provided
  if (!is.null(rows)) {
    if (!is.numeric(rows) || length(rows) != 1 || rows < 1) {
      stop("Row preview limit must be a single positive integer")
    }
    options(mydb.preview.rows = as.integer(rows))
    message("Global MyDB row preview limit set to: ", rows, " rows")
  }

  # Update cols if provided
  if (!is.null(cols)) {
    if (!is.numeric(cols) || length(cols) != 1 || cols < 1) {
      stop("Column preview limit must be a single positive integer")
    }
    options(mydb.preview.cols = as.integer(cols))
    message("Global MyDB column preview limit set to: ", cols, " columns")
  }

  # If neither provided, show current settings
  if (is.null(rows) && is.null(cols)) {
    current_limits <- get_preview_limit()
    message("Current preview limits - Rows: ", current_limits$rows, ", Columns: ", current_limits$cols)
    return(invisible(current_limits))
  }

  # Return updated limits
  invisible(get_preview_limit())
}

#' ===========================================================================
#' GENERIC MyDB CLASS DEFINITION
#' ===========================================================================
MyDB <- R6Class("MyDB",
                public = list(
                  file_path = NULL,
                  con = NULL,
                  table = NULL,

                  initialize = function(file_path, table = NULL) {
                    # Validate required parameters
                    if (missing(file_path) || is.null(file_path)) {
                      stop("file_path is required for DuckDB backend")
                    }

                    self$file_path <- file_path
                    self$connect()

                    # Auto-detect table if not specified
                    if (is.null(table)) {
                      available_tables <- DBI::dbListTables(self$con)
                      if (length(available_tables) > 0) {
                        self$table <- available_tables[1]
                        message("Auto-selected table: ", self$table)
                      } else {
                        stop("No tables found in database")
                      }
                    } else {
                      self$table <- table
                    }
                  },

                  connect = function() {
                    self$con <- DBI::dbConnect(duckdb::duckdb(), dbdir = self$file_path)
                    message("Successfully connected to DuckDB")
                  },

                  disconnect = function() {
                    if (!is.null(self$con)) {
                      DBI::dbDisconnect(self$con, shutdown = TRUE)
                      message("Disconnected from DuckDB")
                      self$con <- NULL
                    }
                  },

                  # Utility methods for common operations
                  list_tables = function() {
                    if (is.null(self$con)) stop("Not connected to database")
                    DBI::dbListTables(self$con)
                  },

                  query = function(sql) {
                    if (is.null(self$con)) stop("Not connected to database")
                    DBI::dbGetQuery(self$con, sql)
                  },

                  set_table = function(table_name) {
                    self$table <- table_name
                    message("Active table set to: ", table_name)
                  },

                  get_table_info = function(table_name = NULL) {
                    if (is.null(self$con)) stop("Not connected to database")
                    table_to_check <- table_name %||% self$table
                    if (is.null(table_to_check)) stop("No table specified")

                    DBI::dbListFields(self$con, table_to_check)
                  },

                  # Auto-detect and set first table if none specified
                  ensure_table = function() {
                    if (is.null(self$table)) {
                      available_tables <- self$list_tables()
                      if (length(available_tables) > 0) {
                        self$table <- available_tables[1]
                        message("Auto-selected table: ", self$table)
                      } else {
                        stop("No tables found in database")
                      }
                    }
                  }
                )
)


# Helper operator for null coalescing
`%||%` <- function(x, y) if (is.null(x)) y else x

#' Helper function to apply column limiting to a query
#' @param query The base query
#' @param columns Available column names
#' @param col_limit Maximum number of columns to show
#' @return List with limited query and column names
apply_column_preview_limit <- function(query, columns, col_limit) {
  if (length(columns) <= col_limit) {
    # No limiting needed
    return(list(query = query, columns = columns, truncated = FALSE))
  }

  # Limit columns
  limited_columns <- columns[1:col_limit]
  col_str <- paste(limited_columns, collapse = ", ")

  # Apply column limiting to query
  if (grepl("LIMIT", query, ignore.case = TRUE)) {
    # Query already has LIMIT - wrap it in a subquery for column selection
    limited_query <- paste0("SELECT ", col_str, " FROM (", query, ") col_preview_subq")
  } else {
    # No LIMIT - can select directly
    limited_query <- paste0("SELECT ", col_str, " FROM (", query, ") col_preview_subq")
  }

  return(list(
    query = limited_query,
    columns = limited_columns,
    truncated = TRUE,
    total_cols = length(columns),
    shown_cols = length(limited_columns)
  ))
}


#' ===========================================================================
#' HELPER FUNCTIONS FOR INDEXING AND QUERIES
#' ===========================================================================
# Helper function to handle negative indices
handle_negative_indices <- function(indices, total_length) {
  if (is.numeric(indices) && any(indices < 0)) {
    # Convert negative indices to positive (exclude those rows/cols)
    positive_indices <- setdiff(1:total_length, abs(indices[indices < 0]))
    return(positive_indices)
  }
  return(indices)
}

# Fixed helper function to extract expression from db.vector query
extract_expression_from_query <- function(db_vec) {
  if (grepl("SELECT .+ as result FROM", db_vec$query)) {
    # This is a computed expression
    # Extract the expression part between SELECT and as result
    pattern <- "SELECT\\s+(.+?)\\s+as\\s+result\\s+FROM"
    match <- regmatches(db_vec$query, regexpr(pattern, db_vec$query, ignore.case = TRUE))
    if (length(match) > 0) {
      expr <- gsub(pattern, "\\1", match, ignore.case = TRUE)

      # FIXED: For nested subqueries, use the result column name instead of trying to extract
      # the original expression, which may reference columns not available in the current context
      if (grepl("FROM \\(.*SELECT.*as result.*\\) subq", db_vec$query)) {
        # This is a nested expression - use "result" as the column reference
        return("result")
      }

      # Clean up subquery references - be more specific about what to replace
      expr <- gsub("\\bsubq\\.", "", expr)
      return(expr)
    }
  }

  # Simple column reference - check if we need to qualify it
  if (grepl("FROM \\(.*\\) subq", db_vec$query)) {
    # Query has subquery structure, use unqualified column name
    return(db_vec$column)
  } else {
    # Direct table query, use unqualified column name
    return(db_vec$column)
  }
}


# Helper function to build row filtering query with logical conditions - FIXED VERSION
build_row_query <- function(base_query, i, table_name, con) {
  if (missing(i)) return(base_query)

  if (inherits(i, "db.vector")) {
    # Extract the expression
    expr <- extract_expression_from_query(i)

    if (expr != "result") {
      # Simple expression - inline in WHERE for efficiency
      return(paste0("SELECT * FROM (", base_query, ") subq WHERE ", expr))
    } else {
      # Complex/nested - use fixed JOIN with ROW_NUMBER before WHERE
      query <- paste0(
        "WITH base AS (SELECT *, ROW_NUMBER() OVER() AS rn FROM (", base_query, ") base_subq),\n",
        "cond AS (SELECT rn FROM (SELECT ROW_NUMBER() OVER() AS rn, * FROM (", i$query, ") cond_subq) where_subq WHERE ", i$column, ")\n",
        "SELECT base.* FROM base JOIN cond ON base.rn = cond.rn ORDER BY base.rn"
      )
      return(query)
    }
  }

  if (is.numeric(i)) {
    # FIXED: Add validation to ensure i contains valid numeric values
    if (any(is.na(i)) || any(!is.finite(i))) {
      stop("Invalid row indices: must be finite numeric values")
    }

    # Handle negative indices
    if (any(i < 0)) {
      # Get total row count for negative indexing
      count_query <- paste0("SELECT COUNT(*) as count FROM ", table_name)
      total_rows <- DBI::dbGetQuery(con, count_query)$count
      i <- handle_negative_indices(i, total_rows)
    }

    if (length(i) == 0) stop("No rows selected after negative indexing")
    if (any(i < 1)) stop("Invalid row indices")

    # FIXED: Ensure i contains integers only and sort for diff calculation
    i <- as.integer(i)
    i <- i[!is.na(i)]  # Remove any NAs that might have been introduced

    if (length(i) == 0) stop("No valid row indices after cleaning")

    # Create row number filtering
    min_row <- min(i)
    max_row <- max(i)

    # FIXED: Add proper validation before using diff()
    if (length(i) > 1) {
      sorted_i <- sort(i)
      # Check if contiguous by comparing length with range
      is_contiguous <- length(sorted_i) == (max_row - min_row + 1) && all(diff(sorted_i) == 1)
    } else {
      is_contiguous <- TRUE  # Single element is always "contiguous"
    }

    if (is_contiguous) {
      # Contiguous range - use LIMIT/OFFSET
      limit <- max_row - min_row + 1
      offset <- min_row - 1

      # FIXED: Wrap the base query in a subquery to ensure LIMIT is applied correctly
      if (grepl("SELECT \\* FROM", base_query) && !grepl("LIMIT|OFFSET", base_query)) {
        # Simple table query - can apply LIMIT directly
        query <- paste0(base_query, " LIMIT ", limit, " OFFSET ", offset)
      } else {
        # Complex query - wrap in subquery
        query <- paste0("SELECT * FROM (", base_query, ") row_limit_subq LIMIT ", limit, " OFFSET ", offset)
      }
    } else {
      # Non-contiguous - use ROW_NUMBER()
      row_list <- paste(i, collapse = ", ")
      query <- paste0("SELECT * FROM (SELECT *, ROW_NUMBER() OVER() as rn FROM (",
                      base_query, ") base_subq) numbered_subq WHERE rn IN (", row_list, ")")
    }
    return(query)
  }

  return(base_query)
}

# Helper function to build column filtering query - FIXED VERSION
build_column_query <- function(base_query, j, col_names) {
  if (missing(j)) return(base_query)

  if (is.numeric(j)) {
    # Handle negative indices
    if (any(j < 0)) {
      j <- handle_negative_indices(j, length(col_names))
    }

    if (length(j) == 0) stop("No columns selected after negative indexing")
    # Changed error message to match base R
    if (any(j < 1 | j > length(col_names))) stop("undefined columns selected")
    j <- col_names[j]
  }

  if (is.character(j)) {
    # Changed error message to match base R
    if (!all(j %in% col_names)) stop("undefined columns selected")
    selected_cols <- paste(j, collapse = ", ")

    # FIXED: Preserve any existing LIMIT/OFFSET clauses when selecting columns
    if (grepl("LIMIT", base_query, ignore.case = TRUE)) {
      # Query already has LIMIT - wrap it in a subquery for column selection
      query <- paste0("SELECT ", selected_cols, " FROM (", base_query, ") col_subq")
    } else {
      # No LIMIT - can select directly
      query <- paste0("SELECT ", selected_cols, " FROM (", base_query, ") col_subq")
    }
    return(query)
  }

  return(base_query)
}
#' ===========================================================================
#' MAIN SUBSETTING OPERATORS FOR MyDB
#' ===========================================================================

# Define subsetting operators OUTSIDE the R6 class
`[.MyDB` <- function(x, i, j, drop = TRUE) {
  col_names <- DBI::dbListFields(x$con, x$table)

  # Detect if this is single-bracket subsetting with one or two indices
  # In R's data frames: df[j] selects columns, df[i,j] selects rows and columns
  n_args <- nargs()

  # Single index subsetting: data[1] or data[1:5] - this is COLUMN subsetting in R
  if (n_args == 2) {
    # This is column-only subsetting (like dat[1] or dat[1:5])
    if (missing(i)) {
      # x[] - return everything as data frame
      query <- paste0("SELECT * FROM ", x$table)
      return(structure(list(con = x$con, table = x$table, query = query, columns = col_names),
                       class = "db.dataframe"))
    }

    # Column selection
    if (is.numeric(i)) {
      # Handle negative indices
      if (any(i < 0)) {
        i <- handle_negative_indices(i, length(col_names))
      }

      if (length(i) == 0) stop("No columns selected after negative indexing")
      # Check bounds and give appropriate error message like base R
      if (any(i < 1 | i > length(col_names))) stop("undefined columns selected")

      selected_cols <- col_names[i]
    } else if (is.character(i)) {
      if (!all(i %in% col_names)) stop("undefined columns selected")
      selected_cols <- i
    } else {
      stop("invalid subscript type")
    }

    # Build query
    cols_str <- paste(selected_cols, collapse = ", ")
    query <- paste0("SELECT ", cols_str, " FROM ", x$table)

    # Return db.vector for single column with drop=TRUE, otherwise db.dataframe
    if (length(selected_cols) == 1 && drop) {
      return(structure(list(con = x$con, table = x$table, column = selected_cols[1], query = query),
                       class = "db.vector"))
    } else {
      return(structure(list(con = x$con, table = x$table, query = query, columns = selected_cols),
                       class = "db.dataframe"))
    }
  }

  # Two-argument subsetting: data[i,j] - this is row and column subsetting
  # Build base query
  base_query <- paste0("SELECT * FROM ", x$table)

  # Apply row filtering (including logical conditions)
  query <- build_row_query(base_query, i, x$table, x$con)

  # Apply column filtering
  query <- build_column_query(query, j, col_names)

  # Determine final column names after filtering
  final_cols <- if (!missing(j)) {
    if (is.numeric(j)) {
      if (any(j < 0)) {
        j <- handle_negative_indices(j, length(col_names))
      }
      if (length(j) == 0) stop("No columns selected after negative indexing")
      # Match base R error message
      if (any(j < 1 | j > length(col_names))) stop("undefined columns selected")
      col_names[j]
    } else if (is.character(j)) {
      if (!all(j %in% col_names)) stop("undefined columns selected")
      j
    } else {
      stop("invalid subscript type")
    }
  } else {
    col_names
  }

  # Return db.vector for single column
  if (length(final_cols) == 1 && drop) {
    return(structure(list(con = x$con, table = x$table, column = final_cols[1], query = query),
                     class = "db.vector"))
  }

  # Return db.dataframe for all other cases
  return(structure(list(con = x$con, table = x$table, query = query, columns = final_cols),
                   class = "db.dataframe"))
}

`[[.MyDB` <- function(x, i) {
  col_names <- DBI::dbListFields(x$con, x$table)
  if (is.numeric(i)) {
    if (i < 1 || i > length(col_names)) stop("Column index out of bounds")
    i <- col_names[i]
  }
  if (!is.character(i) || !i %in% col_names) stop("Invalid or non-existent column name")
  query <- paste0("SELECT ", i, " FROM ", x$table)
  structure(list(con = x$con, table = x$table, column = i, query = query), class = "db.vector")
}


`$.MyDB` <- function(x, name) {
  # Check if name is an internal field or method
  if (name %in% ls(x)) {
    return(.subset2(x, name))
  }

  # Otherwise, treat as potential column name
  col_names <- DBI::dbListFields(.subset2(x, "con"), .subset2(x, "table"))
  if (!name %in% col_names) {
    return(NULL)
  }
  query <- paste0("SELECT ", name, " FROM ", .subset2(x, "table"))
  structure(list(con = .subset2(x, "con"), table = .subset2(x, "table"), column = name, query = query), class = "db.vector")
}
#' ===========================================================================
#' UPDATED PRINT METHODS FOR DB OBJECTS WITH CONFIGURABLE PREVIEW
#' ===========================================================================

# Updated print methods with configurable preview limit
print.db.vector <- function(x) {
  preview_limits <- get_preview_limit()

  # Get total count first to determine if we should show preview
  count_query <- paste0("SELECT COUNT(*) as total FROM (", x$query, ") count_subq")
  total_count <- DBI::dbGetQuery(x$con, count_query)$total

  # Only show preview message and limit if total count > 10
  if (total_count > 10) {
    cat("(showing first ", preview_limits$rows, " elements)\n")

    # Check if query already has LIMIT/OFFSET
    if (grepl("LIMIT", x$query, ignore.case = TRUE)) {
      # If query already has LIMIT, create subquery for preview
      preview_query <- paste0("SELECT * FROM (", x$query, ") preview_subq LIMIT ", preview_limits$rows)
    } else {
      # If no LIMIT, add it directly
      preview_query <- paste0(x$query, " LIMIT ", preview_limits$rows)
    }
    result <- DBI::dbGetQuery(x$con, preview_query)
  } else {
    # For small datasets (<=10), show all without any message
    result <- DBI::dbGetQuery(x$con, x$query)
  }

  print(as.vector(result[[x$column]]))
}

print.db.dataframe <- function(x) {
  preview_limits <- get_preview_limit()

  # Get total count first to determine if we should show preview
  count_query <- paste0("SELECT COUNT(*) as total FROM (", x$query, ") count_subq")
  total_count <- DBI::dbGetQuery(x$con, count_query)$total

  # Apply column limiting
  col_info <- apply_column_preview_limit(x$query, x$columns, preview_limits$cols)

  # Only show messages and limit if total count > 10
  if (total_count > 10) {
    # Show header info
    if (col_info$truncated) {
      cat("(showing first ", preview_limits$rows, " rows, ",
          col_info$shown_cols, " of ", col_info$total_cols, " columns)\n")
    } else {
      cat("(showing first ", preview_limits$rows, " rows)\n")
    }

    # Apply row limiting
    if (grepl("LIMIT", col_info$query, ignore.case = TRUE)) {
      # If query already has LIMIT, create subquery for preview
      preview_query <- paste0("SELECT * FROM (", col_info$query, ") preview_subq LIMIT ", preview_limits$rows)
    } else {
      # If no LIMIT, add it directly
      preview_query <- paste0(col_info$query, " LIMIT ", preview_limits$rows)
    }
  } else {
    # For small datasets (<=10), show all without any message
    preview_query <- col_info$query
  }

  result <- DBI::dbGetQuery(x$con, preview_query)
  print(as.data.frame(result))

  # Show truncation info only for large datasets with truncated columns
  if (total_count > 10 && col_info$truncated) {
    hidden_cols <- col_info$total_cols - col_info$shown_cols
    cat("... (", hidden_cols, " more columns not shown. Use as.ram() to see all data)\n")
  }
}

print.db.matrix <- function(x) {
  preview_limits <- get_preview_limit()

  # Get total count to determine if we need preview message
  count_query <- paste0("SELECT COUNT(*) as total FROM (", x$query, ") count_subq")
  total_count <- DBI::dbGetQuery(x$con, count_query)$total

  nrow <- x$nrow
  ncol <- x$ncol

  # Determine rows to show
  rows_to_show <- if (nrow > 10) {
    cat("(showing first ", preview_limits$rows, " rows)\n")
    min(preview_limits$rows, nrow)
  } else {
    nrow
  }

  # Determine columns to show
  cols_to_show <- min(preview_limits$cols, ncol)
  col_truncated <- cols_to_show < ncol

  if (col_truncated) {
    cat("(showing ", cols_to_show, " of ", ncol, " columns)\n")
  }

  # Calculate how many elements to fetch (column-major order)
  elements_needed <- rows_to_show * cols_to_show

  # Fetch data in column-major order
  positions <- c()
  for (col_idx in 1:cols_to_show) {
    for (row_idx in 1:rows_to_show) {
      pos <- (col_idx - 1) * nrow + row_idx
      positions <- c(positions, pos)
    }
  }

  positions_str <- paste(positions, collapse = ", ")
  preview_query <- paste0(
    "SELECT value FROM (",
    "  SELECT value, ROW_NUMBER() OVER() as rn FROM (", x$query, ") numbered_subq",
    ") positioned WHERE rn IN (", positions_str, ")"
  )

  result <- DBI::dbGetQuery(x$con, preview_query)

  if (nrow(result) > 0) {
    # Reconstruct matrix in column-major order
    result_matrix <- matrix(result$value, nrow = rows_to_show, ncol = cols_to_show)

    # Add column names
    if (!is.null(x$colnames)) {
      colnames(result_matrix) <- x$colnames[1:cols_to_show]
    } else {
      colnames(result_matrix) <- paste0("[,", 1:cols_to_show, "]")
    }

    # Add row names if available
    if (!is.null(x$rownames)) {
      rownames(result_matrix) <- x$rownames[1:rows_to_show]
    }

    print(result_matrix)

    # Show truncation info
    if (col_truncated) {
      hidden_cols <- ncol - cols_to_show
      cat("... (", hidden_cols, " more columns not shown. Use as.ram() to see all data)\n")
    }
  } else {
    cat("No data available for preview\n")
  }
}
print.db.array <- function(x) {
  preview_limits <- get_preview_limit()

  dims <- x$dim
  n_dims <- length(dims)

  if (n_dims == 1) {
    # 1D array (vector-like)
    count_query <- paste0("SELECT COUNT(*) as total FROM (", x$query, ") count_subq")
    total_count <- DBI::dbGetQuery(x$con, count_query)$total

    if (total_count > 10) {
      cat("(showing first ", preview_limits$rows, " elements)\n")
      preview_query <- paste0("SELECT value FROM (", x$query, ") preview_subq LIMIT ", preview_limits$rows)
    } else {
      preview_query <- paste0("SELECT value FROM (", x$query, ") preview_subq")
    }

    result <- DBI::dbGetQuery(x$con, preview_query)
    data_vector <- result$value

    preview_array <- array(data_vector, dim = length(data_vector))
    print(preview_array)

  } else if (n_dims == 2) {
    # 2D array (matrix-like)
    rows_to_show <- if (dims[1] > 10) {
      cat("(showing first ", preview_limits$rows, " of ", dims[1], " rows)\n")
      min(preview_limits$rows, dims[1])
    } else {
      dims[1]
    }

    # Calculate how many elements to fetch (column-major order)
    elements_needed <- rows_to_show * dims[2]

    preview_query <- paste0("SELECT value FROM (", x$query, ") preview_subq LIMIT ", elements_needed)
    result <- DBI::dbGetQuery(x$con, preview_query)
    data_vector <- result$value

    if (length(data_vector) > 0) {
      # Reconstruct array in column-major order
      preview_array <- array(data_vector, dim = c(rows_to_show, dims[2]))
      print(preview_array)
    } else {
      cat("No data available for preview\n")
    }

  } else {
    # 3D and higher arrays
    slice_size <- dims[1] * dims[2]
    nslices <- prod(dims[3:n_dims])

    rows_to_show <- min(dims[1], preview_limits$rows)
    max_slices_to_show <- min(nslices, preview_limits$rows)

    if (rows_to_show < dims[1] || max_slices_to_show < nslices) {
      cat("(showing first ", rows_to_show, " rows per slice, ", max_slices_to_show, " of ", nslices, " slices)\n")
    }

    # Calculate elements per slice to display
    elements_per_slice_display <- rows_to_show * dims[2]
    elements_to_fetch <- max_slices_to_show * slice_size  # Fetch full slices

    preview_query <- paste0("SELECT value FROM (", x$query, ") preview_subq LIMIT ", elements_to_fetch)
    result <- DBI::dbGetQuery(x$con, preview_query)
    data_vector <- result$value

    if (length(data_vector) >= slice_size) {
      # Generate slice combinations
      slice_combinations <- generate_slice_combinations(dims[3:n_dims], max_slices_to_show)

      # Show each slice
      for (slice_idx in 1:max_slices_to_show) {
        if (slice_idx <= nrow(slice_combinations)) {
          slice_coords <- slice_combinations[slice_idx, ]

          # Extract this slice's data (column-major order)
          start_pos <- (slice_idx - 1) * slice_size + 1
          end_pos <- min(start_pos + slice_size - 1, length(data_vector))

          if (start_pos <= length(data_vector)) {
            slice_data <- data_vector[start_pos:end_pos]

            # Format slice header
            coord_str <- paste(slice_coords, collapse = ", ")
            cat("\n, , ", coord_str, "\n\n", sep = "")

            # Show limited rows
            if (length(slice_data) >= elements_per_slice_display) {
              display_data <- slice_data[1:elements_per_slice_display]
              slice_matrix <- array(display_data, dim = c(rows_to_show, dims[2]))
              print(slice_matrix)

              if (rows_to_show < dims[1]) {
                remaining_rows <- dims[1] - rows_to_show
                cat("... (", remaining_rows, " more rows in this slice)\n")
              }
            }
          }
        }
      }

      if (max_slices_to_show < nslices) {
        remaining_slices <- nslices - max_slices_to_show
        cat("\n... (", remaining_slices, " more slices)\n")
      }
    } else {
      cat("Insufficient data for preview\n")
    }
  }
}

generate_slice_combinations <- function(higher_dims, max_slices) {
  n_higher_dims <- length(higher_dims)

  if (n_higher_dims == 1) {
    # Simple case: just one higher dimension
    indices <- 1:min(max_slices, higher_dims[1])
    return(matrix(indices, ncol = 1))
  }

  # For multiple higher dimensions, generate combinations
  # Create a list of sequences for each dimension
  dim_sequences <- lapply(higher_dims, function(dim_size) 1:dim_size)

  # Generate combinations using expand.grid
  all_combinations <- expand.grid(dim_sequences, stringsAsFactors = FALSE)

  # Limit to the number of slices we want to show
  max_combinations <- min(max_slices, nrow(all_combinations))

  # Return as matrix with appropriate column order
  result_matrix <- as.matrix(all_combinations[1:max_combinations, , drop = FALSE])

  return(result_matrix)
}

#' ===========================================================================
#' CONVERSION FUNCTIONS
#' ===========================================================================
as.ram <- function(x) {
  if (inherits(x, "db.vector")) {
    result <- DBI::dbGetQuery(x$con, x$query)
    as.vector(result[[x$column]])
  } else if (inherits(x, "db.dataframe")) {
    result <- DBI::dbGetQuery(x$con, x$query)
    as.data.frame(result)
  } else if (inherits(x, "db.matrix")) {
    result <- DBI::dbGetQuery(x$con, paste0("SELECT value FROM (", x$query, ") subq"))
    mat <- matrix(result$value, nrow = x$nrow, ncol = x$ncol)

    # Add names if they exist
    if (!is.null(x$rownames)) {
      rownames(mat) <- x$rownames
    }
    if (!is.null(x$colnames)) {
      colnames(mat) <- x$colnames
    }

    mat
  } else if (inherits(x, "db.array")) {
    result <- DBI::dbGetQuery(x$con, paste0("SELECT value FROM (", x$query, ") subq"))
    array(result$value, dim = x$dim)
  } else if (inherits(x, "db.factor")) {
    all_query <- paste0("SELECT ", x$column, " FROM (", x$query, ") subq")
    result <- DBI::dbGetQuery(x$con, all_query)
    values <- result[[x$column]]
    levels_data <- DBI::dbGetQuery(x$con, paste0("SELECT level_value, level_label FROM ", x$levels_table, " ORDER BY level_order"))
    disk_levels <- levels_data$level_value
    disk_labels <- levels_data$level_label
    factor(values, levels = disk_levels, labels = disk_labels, ordered = x$ordered)
  } else if (inherits(x, "db.list")) {
    result <- lapply(x, as.ram)
    names(result) <- names(x)
    result
  } else if (inherits(x, "table") && !is.null(attr(x, "truncated")) && attr(x, "truncated")) {
    stop("Cannot convert truncated table to full version directly. Use as.ram(table(your_factor)) instead.")
  } else if (is.list(x) && length(x) > 0 && all(sapply(x, inherits, "db.vector"))) {
    lapply(x, function(vec) {
      all_query <- paste0("SELECT ", vec$column, " FROM (", vec$query, ") subq")
      result <- DBI::dbGetQuery(vec$con, all_query)
      result[[vec$column]]
    })
  } else {
    stop("Unsupported object type for as.ram")
  }
}
#' ===========================================================================
#' DB-SPECIFIC MATRIX AND ARRAY OPERATIONS (UPDATED)
#' ===========================================================================
as.db.matrix <- function(x, ...) {
  UseMethod("as.db.matrix")
}

as.db.matrix.MyDB <- function(x, nrow = NULL, ncol = NULL, byrow = FALSE, ...) {
  # Get table data info
  col_names <- DBI::dbListFields(x$con, x$table)
  count_query <- paste0("SELECT COUNT(*) as count FROM ", x$table)
  nrows_table <- DBI::dbGetQuery(x$con, count_query)$count
  ncols_table <- length(col_names)
  total_elements <- nrows_table * ncols_table

  # Flatten all columns into single sequential column (column-major order)
  union_parts <- lapply(col_names, function(col) {
    paste0("SELECT ", col, " as value FROM ", x$table)
  })
  query <- paste(union_parts, collapse = " UNION ALL ")

  # Determine dimensions
  if (is.null(nrow) && is.null(ncol)) {
    # Use original table structure
    nrow <- nrows_table
    ncol <- ncols_table
  } else if (is.null(nrow)) {
    nrow <- ceiling(total_elements / ncol)
  } else if (is.null(ncol)) {
    ncol <- ceiling(total_elements / nrow)
  }

  # Check if we need recycling or truncation
  needed_elements <- nrow * ncol
  if (total_elements != needed_elements) {
    warning("data length [", total_elements, "] is not a sub-multiple or multiple of the number of rows [", nrow, "]")

    if (total_elements < needed_elements) {
      # Need to recycle - add recycling logic to query
      recycle_times <- ceiling(needed_elements / total_elements)
      recycled_parts <- rep(list(paste0("(", query, ")")), recycle_times)
      query <- paste(recycled_parts, collapse = " UNION ALL ")
      query <- paste0("SELECT value FROM (", query, ") recycled_subq LIMIT ", needed_elements)
    } else {
      # Truncate
      query <- paste0("SELECT value FROM (", query, ") truncated_subq LIMIT ", needed_elements)
    }
  }

  # Handle byrow conversion
  if (byrow) {
    # Convert from column-major to row-major ordering via SQL
    query <- convert_to_rowmajor_query(query, nrow, ncol, x$con)
  }

  structure(list(
    con = x$con,
    table = x$table,
    query = query,
    nrow = nrow,
    ncol = ncol
  ), class = "db.matrix")
}

as.db.matrix.db.dataframe <- function(x, nrow = NULL, ncol = NULL, byrow = FALSE, ...) {
  # Get current dimensions
  count_query <- paste0("SELECT COUNT(*) as count FROM (", x$query, ") count_subq")
  nrows_current <- DBI::dbGetQuery(x$con, count_query)$count
  ncols_current <- length(x$columns)
  total_elements <- nrows_current * ncols_current

  # Flatten into single column (column-major order)
  union_parts <- lapply(x$columns, function(col) {
    paste0("SELECT ", col, " as value FROM (", x$query, ") subq")
  })
  query <- paste(union_parts, collapse = " UNION ALL ")

  # Determine dimensions
  if (is.null(nrow) && is.null(ncol)) {
    nrow <- nrows_current
    ncol <- ncols_current
  } else if (is.null(nrow)) {
    nrow <- ceiling(total_elements / ncol)
  } else if (is.null(ncol)) {
    ncol <- ceiling(total_elements / nrow)
  }

  # Handle recycling/truncation
  needed_elements <- nrow * ncol
  if (total_elements != needed_elements) {
    warning("data length [", total_elements, "] is not a sub-multiple or multiple of the number of rows [", nrow, "]")

    if (total_elements < needed_elements) {
      recycle_times <- ceiling(needed_elements / total_elements)
      recycled_parts <- rep(list(paste0("(", query, ")")), recycle_times)
      query <- paste(recycled_parts, collapse = " UNION ALL ")
      query <- paste0("SELECT value FROM (", query, ") recycled_subq LIMIT ", needed_elements)
    } else {
      query <- paste0("SELECT value FROM (", query, ") truncated_subq LIMIT ", needed_elements)
    }
  }

  # Handle byrow
  if (byrow) {
    query <- convert_to_rowmajor_query(query, nrow, ncol, x$con)
  }

  structure(list(
    con = x$con,
    table = x$table,
    query = query,
    nrow = nrow,
    ncol = ncol
  ), class = "db.matrix")
}

as.db.matrix.db.vector <- function(x, nrow = NULL, ncol = NULL, byrow = FALSE, ...) {
  # Get vector length
  count_query <- paste0("SELECT COUNT(*) as count FROM (", x$query, ") count_subq")
  total_elements <- DBI::dbGetQuery(x$con, count_query)$count

  # Vector is already sequential - just wrap as 'value'
  query <- paste0("SELECT ", x$column, " as value FROM (", x$query, ") subq")

  # Determine dimensions
  if (is.null(nrow) && is.null(ncol)) {
    # Default: column matrix
    nrow <- total_elements
    ncol <- 1
  } else if (is.null(nrow)) {
    nrow <- ceiling(total_elements / ncol)
  } else if (is.null(ncol)) {
    ncol <- ceiling(total_elements / nrow)
  }

  # Handle recycling/truncation
  needed_elements <- nrow * ncol
  if (total_elements != needed_elements) {
    warning("data length [", total_elements, "] is not a sub-multiple or multiple of the number of rows [", nrow, "]")

    if (total_elements < needed_elements) {
      recycle_times <- ceiling(needed_elements / total_elements)
      recycled_parts <- rep(list(paste0("(", query, ")")), recycle_times)
      query <- paste(recycled_parts, collapse = " UNION ALL ")
      query <- paste0("SELECT value FROM (", query, ") recycled_subq LIMIT ", needed_elements)
    } else {
      query <- paste0("SELECT value FROM (", query, ") truncated_subq LIMIT ", needed_elements)
    }
  }

  # Handle byrow
  if (byrow) {
    query <- convert_to_rowmajor_query(query, nrow, ncol, x$con)
  }

  structure(list(
    con = x$con,
    table = x$table,
    query = query,
    nrow = nrow,
    ncol = ncol
  ), class = "db.matrix")
}

convert_to_rowmajor_query <- function(query, nrow, ncol, con) {
  # SQL to reorder from column-major to row-major
  # For each position in row-major, calculate its column-major position
  # Row-major position p maps to column-major position:
  # p_col = (p_row %% nrow) * ncol + (p_row %/% nrow) + 1

  paste0(
    "SELECT value FROM (",
    "  SELECT value, ROW_NUMBER() OVER() as rn FROM (", query, ") numbered_subq",
    ") reordered ",
    "ORDER BY ((rn - 1) % ", nrow, ") * ", ncol, " + ((rn - 1) / ", nrow, ") + 1"
  )
}

as.db.matrix.matrix <- function(x, table_name = "temp_matrix", con, ...) {
  stop("Converting R matrices to db.matrix requires an existing MyDB connection. Use db[[\"colname\"]] <- as.vector(x) instead.")
}

#' ===========================================================================
#' COLUMN ASSIGNMENT OPERATIONS
#' ===========================================================================
# FIXED: create_table_with_column function
create_table_with_column <- function(x, column_name, value, operation = "add") {
  # Generate a unique temporary table name
  temp_table <- paste0("temp_", x$table, "_", format(Sys.time(), "%Y%m%d_%H%M%S"), "_", sample(1000:9999, 1))

  if (operation == "remove") {
    # Remove column operation
    existing_cols <- DBI::dbListFields(x$con, x$table)
    remaining_cols <- setdiff(existing_cols, column_name)

    if (length(remaining_cols) == 0) {
      stop("Cannot remove all columns from table")
    }

    cols_str <- paste(remaining_cols, collapse = ", ")
    create_query <- paste0("CREATE TABLE ", temp_table, " AS SELECT ", cols_str, " FROM ", x$table)

  } else {
    # Add or update column operation
    existing_cols <- DBI::dbListFields(x$con, x$table)

    if (is.numeric(value) && length(value) == 1) {
      # Constant numeric value
      value_expr <- as.character(value)
    } else if (is.character(value) && length(value) == 1) {
      # Constant string value
      value_expr <- paste0("'", gsub("'", "''", value), "'")
    } else if (inherits(value, "db.vector")) {
      # FIXED: Handle db.vector expressions properly by using the entire subquery
      # Instead of trying to extract expressions, use the db.vector query as a subquery

      # Check if the db.vector query is a simple column reference or a complex expression
      if (grepl("SELECT .+ as result FROM", value$query)) {
        # This is a complex expression - we need to join it with the base table
        # Create a row-numbered version for proper joining
        value_subquery <- paste0("(SELECT result, ROW_NUMBER() OVER() as rn FROM (", value$query, ") value_subq)")
        base_with_rn <- paste0("(SELECT *, ROW_NUMBER() OVER() as rn FROM ", x$table, ") base")

        # Build the final query using JOIN
        if (column_name %in% existing_cols) {
          # Update existing column
          other_cols <- setdiff(existing_cols, column_name)
          if (length(other_cols) > 0) {
            other_cols_str <- paste(paste0("base.", other_cols), collapse = ", ")
            cols_str <- paste0(other_cols_str, ", value_data.result as ", column_name)
          } else {
            cols_str <- paste0("value_data.result as ", column_name)
          }
        } else {
          # Add new column
          existing_cols_str <- paste(paste0("base.", existing_cols), collapse = ", ")
          cols_str <- paste0(existing_cols_str, ", value_data.result as ", column_name)
        }

        # FIXED: Remove the duplicate "base" in the FROM clause
        create_query <- paste0("CREATE TABLE ", temp_table, " AS SELECT ", cols_str,
                               " FROM ", base_with_rn, " JOIN ", value_subquery, " value_data ON base.rn = value_data.rn")

      } else {
        # Simple column reference - can reference directly
        # But we still need to be careful about table context
        if (value$table == x$table) {
          # Same table - can reference column directly
          value_expr <- value$column
        } else {
          # Different table - need to use subquery approach
          value_subquery <- paste0("(SELECT ", value$column, ", ROW_NUMBER() OVER() as rn FROM ", value$table, ")")
          base_with_rn <- paste0("(SELECT *, ROW_NUMBER() OVER() as rn FROM ", x$table, ") base")

          if (column_name %in% existing_cols) {
            other_cols <- setdiff(existing_cols, column_name)
            if (length(other_cols) > 0) {
              other_cols_str <- paste(paste0("base.", other_cols), collapse = ", ")
              cols_str <- paste0(other_cols_str, ", value_data.", value$column, " as ", column_name)
            } else {
              cols_str <- paste0("value_data.", value$column, " as ", column_name)
            }
          } else {
            existing_cols_str <- paste(paste0("base.", existing_cols), collapse = ", ")
            cols_str <- paste0(existing_cols_str, ", value_data.", value$column, " as ", column_name)
          }

          # FIXED: Remove the duplicate "base" in the FROM clause
          create_query <- paste0("CREATE TABLE ", temp_table, " AS SELECT ", cols_str,
                                 " FROM ", base_with_rn, " JOIN ", value_subquery, " value_data ON base.rn = value_data.rn")

          return(list(query = create_query, temp_table = temp_table))
        }
      }
    } else {
      stop("Unsupported value type for column assignment")
    }

    # For simple cases (non-db.vector or same-table db.vector), use the original approach
    if (!inherits(value, "db.vector") || (inherits(value, "db.vector") && value$table == x$table && !grepl("SELECT .+ as result FROM", value$query))) {
      if (column_name %in% existing_cols) {
        # Update existing column
        other_cols <- setdiff(existing_cols, column_name)
        if (length(other_cols) > 0) {
          cols_str <- paste(c(other_cols, paste0(value_expr, " as ", column_name)), collapse = ", ")
        } else {
          cols_str <- paste0(value_expr, " as ", column_name)
        }
      } else {
        # Add new column
        cols_str <- paste(c(existing_cols, paste0(value_expr, " as ", column_name)), collapse = ", ")
      }

      create_query <- paste0("CREATE TABLE ", temp_table, " AS SELECT ", cols_str, " FROM ", x$table)
    }
  }

  return(list(query = create_query, temp_table = temp_table))
}

#' @export
`[[<-.MyDB` <- function(x, i, value) {
  if (!is.character(i) || length(i) != 1) {
    stop("Column name must be a single character string")
  }

  if (is.null(value)) {
    # Remove column
    if (!i %in% DBI::dbListFields(x$con, x$table)) {
      warning("Column '", i, "' does not exist")
      return(x)
    }

    table_info <- create_table_with_column(x, i, NULL, "remove")

    # Execute the query to create temporary table
    DBI::dbExecute(x$con, table_info$query)

    # Drop original table and rename temp table
    DBI::dbExecute(x$con, paste0("DROP TABLE ", x$table))
    DBI::dbExecute(x$con, paste0("ALTER TABLE ", table_info$temp_table, " RENAME TO ", x$table))

    message("Removed column '", i, "' from ", x$table)

  } else {
    # Add or update column
    table_info <- create_table_with_column(x, i, value, "add")

    # Execute the query to create temporary table
    tryCatch({
      DBI::dbExecute(x$con, table_info$query)

      # Drop original table and rename temp table
      DBI::dbExecute(x$con, paste0("DROP TABLE ", x$table))
      DBI::dbExecute(x$con, paste0("ALTER TABLE ", table_info$temp_table, " RENAME TO ", x$table))

      if (i %in% DBI::dbListFields(x$con, x$table)) {
        message("Updated column '", i, "' in ", x$table)
      } else {
        message("Added column '", i, "' to ", x$table)
      }

    }, error = function(e) {
      # Clean up temp table if it was created
      tryCatch(DBI::dbExecute(x$con, paste0("DROP TABLE IF EXISTS ", table_info$temp_table)), error = function(e2) {})
      stop("Failed to add/update column: ", e$message)
    })
  }

  return(x)
}

as.db.array <- function(x, dim = NULL, ...) {
  UseMethod("as.db.array")
}

as.db.array.MyDB <- function(x, dim = NULL, ...) {
  col_names <- DBI::dbListFields(x$con, x$table)

  # Flatten all columns into single sequential column (column-major order)
  # Build UNION ALL query to stack columns
  union_parts <- lapply(col_names, function(col) {
    paste0("SELECT ", col, " as value FROM ", x$table)
  })
  query <- paste(union_parts, collapse = " UNION ALL ")

  # If no dimensions specified, infer from data structure
  if (is.null(dim)) {
    count_query <- paste0("SELECT COUNT(*) as count FROM ", x$table)
    nrows <- DBI::dbGetQuery(x$con, count_query)$count
    ncols <- length(col_names)
    dim <- c(nrows, ncols)
  }

  # Validate dimensions match data length
  total_elements <- nrows * ncols
  expected_elements <- prod(dim)
  if (total_elements != expected_elements) {
    stop("dims [product ", expected_elements, "] do not match the length of object [", total_elements, "]")
  }

  structure(list(
    con = x$con,
    table = x$table,
    query = query,
    dim = dim
  ), class = "db.array")
}

as.db.array.db.dataframe <- function(x, dim = NULL, ...) {
  # Flatten dataframe into single column (column-major order)
  union_parts <- lapply(x$columns, function(col) {
    paste0("SELECT ", col, " as value FROM (", x$query, ") subq")
  })
  query <- paste(union_parts, collapse = " UNION ALL ")

  if (is.null(dim)) {
    count_query <- paste0("SELECT COUNT(*) as count FROM (", x$query, ") count_subq")
    nrows <- DBI::dbGetQuery(x$con, count_query)$count
    ncols <- length(x$columns)
    dim <- c(nrows, ncols)
  }

  # Validate dimensions
  total_elements <- DBI::dbGetQuery(x$con, paste0("SELECT COUNT(*) as count FROM (", query, ") validation_subq"))$count
  expected_elements <- prod(dim)
  if (total_elements != expected_elements) {
    stop("dims [product ", expected_elements, "] do not match the length of object [", total_elements, "]")
  }

  structure(list(
    con = x$con,
    table = x$table,
    query = query,
    dim = dim
  ), class = "db.array")
}

as.db.array.db.matrix <- function(x, dim = NULL, ...) {
  # Matrix is already flattened in column-major order
  union_parts <- lapply(x$columns, function(col) {
    paste0("SELECT ", col, " as value FROM (", x$query, ") subq")
  })
  query <- paste(union_parts, collapse = " UNION ALL ")

  if (is.null(dim)) {
    count_query <- paste0("SELECT COUNT(*) as count FROM (", x$query, ") count_subq")
    nrows <- DBI::dbGetQuery(x$con, count_query)$count
    ncols <- length(x$columns)
    dim <- c(nrows, ncols)
  }

  structure(list(
    con = x$con,
    table = x$table,
    query = query,
    dim = dim
  ), class = "db.array")
}

as.db.array.db.vector <- function(x, dim = NULL, ...) {
  if (is.null(dim)) {
    # Get length of vector for dimension
    count_query <- paste0("SELECT COUNT(*) as count FROM (", x$query, ") subq")
    nrows <- DBI::dbGetQuery(x$con, count_query)$count
    dim <- c(nrows)  # Default to 1D array
  }

  # Validate that dimensions match data length
  count_query <- paste0("SELECT COUNT(*) as count FROM (", x$query, ") subq")
  actual_length <- DBI::dbGetQuery(x$con, count_query)$count
  expected_length <- prod(dim)

  if (actual_length != expected_length) {
    stop("dims [product ", expected_length, "] do not match the length of object [", actual_length, "]")
  }

  # Vector is already in sequential order - just wrap with dimensions
  # Select the column as 'value' for consistency
  query <- paste0("SELECT ", x$column, " as value FROM (", x$query, ") subq")

  structure(list(
    con = x$con,
    table = x$table,
    query = query,
    dim = dim
  ), class = "db.array")
}

#' ===========================================================================
#' SUBSETTING FOR DB OBJECTS (ENHANCED WITH LOGICAL FILTERING)
#' ===========================================================================
# Subsetting for db.dataframe - FIXED: Proper subquery handling with negative indexing and logical filtering
`[.db.dataframe` <- function(x, i, j, drop = TRUE) {
  col_names <- DBI::dbListFields(x$con, x$table)

  # Start with existing query
  query <- x$query

  # Apply row filtering (including logical conditions)
  query <- build_row_query(query, i, x$table, x$con)

  # Apply column filtering
  query <- build_column_query(query, j, col_names)

  # Determine final column names after filtering
  final_cols <- if (!missing(j)) {
    if (is.numeric(j)) {
      if (any(j < 0)) {
        j <- handle_negative_indices(j, length(col_names))
      }
      col_names[j]
    } else {
      j
    }
  } else {
    col_names
  }

  # Return db.vector for single column
  if (length(final_cols) == 1 && drop) {
    return(structure(list(con = x$con, table = x$table, column = final_cols[1], query = query), class = "db.vector"))
  }

  # Return db.dataframe
  structure(list(con = x$con, table = x$table, query = query, columns = final_cols), class = "db.dataframe")
}

`[[.db.dataframe` <- function(x, i) {
  col_names <- DBI::dbListFields(x$con, x$table)
  if (is.numeric(i)) {
    if (i > length(col_names) || i < 1) stop("Column index out of bounds")
    i <- col_names[i]
  }
  if (!is.character(i) || !i %in% col_names) stop("Invalid or non-existent column name")
  # Use subquery alias to avoid conflicts
  query <- paste0("SELECT ", i, " FROM (", x$query, ") subq")
  structure(list(con = x$con, table = x$table, column = i, query = query), class = "db.vector")
}

# Subsetting for db.vector - FIXED: Proper subquery handling
`[.db.vector` <- function(x, i) {
  if (is.numeric(i)) {
    if (any(i < 1)) stop("Negative or zero indices not supported")
    limit <- max(i) - min(i) + 1
    offset <- min(i) - 1
    # Create a clean subquery by wrapping the original query
    query <- paste0("SELECT ", x$column, " FROM (", x$query, ") subq LIMIT ", limit, " OFFSET ", offset)
  } else {
    stop("Only numeric indices supported for db.vector")
  }
  structure(list(con = x$con, table = x$table, column = x$column, query = query), class = "db.vector")
}

`$.db.dataframe` <- function(x, name) {
  if (name %in% ls(x)) {
    return(.subset2(x, name))
  }
  col_names <- .subset2(x, "columns")
  if (!name %in% col_names) {
    return(NULL)
  }
  query <- paste0("SELECT ", name, " FROM (", .subset2(x, "query"), ") subq")
  structure(list(con = .subset2(x, "con"), table = .subset2(x, "table"), column = name, query = query), class = "db.vector")
}

# Subsetting for db.matrix
`[.db.matrix` <- function(x, i, j, drop = TRUE) {
  nrow <- x$nrow
  ncol <- x$ncol

  # Check if this is position indexing (single numeric vector) or array indexing (matrix)
  if (!missing(i) && is.matrix(i)) {
    # ARRAY INDEXING: matrix of coordinates
    return(extract_by_array_matrix(x, i, drop))
  }

  if (!missing(i) && is.numeric(i) && missing(j) && length(i) > 1) {
    # POSITION INDEXING: single vector treated as linear positions
    return(extract_by_position_matrix(x, i, drop))
  }

  # Standard row/column indexing
  row_indices <- if (missing(i)) 1:nrow else i
  col_indices <- if (missing(j)) 1:ncol else j

  # Handle negative indices
  if (any(row_indices < 0)) {
    row_indices <- handle_negative_indices(row_indices, nrow)
  }
  if (any(col_indices < 0)) {
    col_indices <- handle_negative_indices(col_indices, ncol)
  }

  # Validate indices
  if (any(row_indices < 1) || any(row_indices > nrow)) {
    stop("Row indices out of bounds")
  }
  if (any(col_indices < 1) || any(col_indices > ncol)) {
    stop("Column indices out of bounds")
  }

  # Calculate linear positions in column-major order
  positions <- c()
  for (col_idx in col_indices) {
    for (row_idx in row_indices) {
      pos <- (col_idx - 1) * nrow + row_idx
      positions <- c(positions, pos)
    }
  }

  positions_str <- paste(positions, collapse = ", ")
  query <- paste0(
    "SELECT value FROM (",
    "  SELECT value, ROW_NUMBER() OVER() as rn FROM (", x$query, ") numbered_subq",
    ") positioned WHERE rn IN (", positions_str, ")"
  )

  new_nrow <- length(row_indices)
  new_ncol <- length(col_indices)

  # Return db.vector for single column or single element with drop=TRUE
  if ((new_ncol == 1 || new_nrow == 1) && drop) {
    return(structure(list(
      con = x$con,
      table = x$table,
      column = "value",
      query = query
    ), class = "db.vector"))
  }

  # Return db.matrix
  structure(list(
    con = x$con,
    table = x$table,
    query = query,
    nrow = new_nrow,
    ncol = new_ncol
  ), class = "db.matrix")
}

#' Enhanced subsetting for db.array with position and array indexing
`[.db.array` <- function(x, i, j, k, l, m, n, o, p, ..., drop = TRUE) {
  dims <- x$dim
  n_dims <- length(dims)

  # Check if first argument is a matrix (array indexing) or numeric vector (position indexing)
  if (!missing(i)) {
    if (is.matrix(i)) {
      # ARRAY INDEXING: matrix where each row is a coordinate
      return(extract_by_array_array(x, i, drop))
    }

    if (is.numeric(i) && missing(j) && length(i) > 1) {
      # POSITION INDEXING: linear positions
      return(extract_by_position_array(x, i, drop))
    }
  }

  # Standard multi-dimensional indexing
  args <- list()

  if (n_dims >= 1) args[[1]] <- if (missing(i)) seq_len(dims[1]) else i
  if (n_dims >= 2) args[[2]] <- if (missing(j)) seq_len(dims[2]) else j
  if (n_dims >= 3) args[[3]] <- if (missing(k)) seq_len(dims[3]) else k
  if (n_dims >= 4) args[[4]] <- if (missing(l)) seq_len(dims[4]) else l
  if (n_dims >= 5) args[[5]] <- if (missing(m)) seq_len(dims[5]) else m
  if (n_dims >= 6) args[[6]] <- if (missing(n)) seq_len(dims[6]) else n
  if (n_dims >= 7) args[[7]] <- if (missing(o)) seq_len(dims[7]) else o
  if (n_dims >= 8) args[[8]] <- if (missing(p)) seq_len(dims[8]) else p

  # Handle additional dimensions from ...
  extra_args <- list(...)
  if (n_dims > 8) {
    for (dim_idx in 9:n_dims) {
      extra_idx <- dim_idx - 8
      if (extra_idx <= length(extra_args)) {
        args[[dim_idx]] <- extra_args[[extra_idx]]
      } else {
        args[[dim_idx]] <- seq_len(dims[dim_idx])
      }
    }
  }

  # Validate column indices before processing
  for (dim_idx in seq_along(args)) {
    if (any(args[[dim_idx]] > dims[dim_idx])) {
      stop("subscript out of bounds")
    }
  }

  return(extract_standard_array(x, args, drop))
}
extract_by_position_matrix <- function(x, positions, drop = TRUE) {
  total_elements <- x$nrow * x$ncol

  if (any(positions < 1) || any(positions > total_elements)) {
    stop("Position indices out of bounds [1, ", total_elements, "]")
  }

  # Build SQL to extract specific positions
  positions_str <- paste(positions, collapse = ", ")

  query <- paste0(
    "SELECT value FROM (",
    "  SELECT value, ROW_NUMBER() OVER() as rn FROM (", x$query, ") numbered_subq",
    ") positioned WHERE rn IN (", positions_str, ")",
    " ORDER BY CASE rn ",
    paste(sapply(1:length(positions), function(i) {
      paste0("WHEN ", positions[i], " THEN ", i)
    }), collapse = " "),
    " END"
  )

  return(structure(list(
    con = x$con,
    table = x$table,
    column = "value",
    query = query
  ), class = "db.vector"))
}

#' Extract from matrix using array indexing (matrix of coordinates)
extract_by_array_matrix <- function(x, index_matrix, drop = TRUE) {
  if (ncol(index_matrix) != 2) {
    stop("Matrix indexing requires exactly 2 columns [row_indices, col_indices]")
  }

  row_indices <- index_matrix[, 1]
  col_indices <- index_matrix[, 2]

  if (any(row_indices < 1) || any(row_indices > x$nrow)) {
    stop("Row indices out of bounds")
  }
  if (any(col_indices < 1) || any(col_indices > x$ncol)) {
    stop("Column indices out of bounds")
  }

  # Convert to linear positions (column-major)
  linear_positions <- (col_indices - 1) * x$nrow + row_indices

  return(extract_by_position_matrix(x, linear_positions, drop))
}

#' Extract from array using linear position indexing
extract_by_position_array <- function(x, positions, drop = TRUE) {
  dims <- x$dim
  total_elements <- prod(dims)

  if (any(positions < 1) || any(positions > total_elements)) {
    stop("Position indices out of bounds [1, ", total_elements, "]")
  }

  # Build SQL to extract specific positions (column-major order)
  # Use ROW_NUMBER() to identify positions
  positions_str <- paste(positions, collapse = ", ")

  query <- paste0(
    "SELECT value, rn FROM (",
    "  SELECT value, ROW_NUMBER() OVER() as rn FROM (", x$query, ") numbered_subq",
    ") positioned WHERE rn IN (", positions_str, ")",
    " ORDER BY CASE rn ",
    paste(sapply(1:length(positions), function(i) {
      paste0("WHEN ", positions[i], " THEN ", i)
    }), collapse = " "),
    " END"
  )

  # Return as db.vector
  return(structure(list(
    con = x$con,
    table = x$table,
    column = "value",
    query = query
  ), class = "db.vector"))
}

#' Extract from array using array indexing (matrix of coordinates)
extract_by_array_array <- function(x, index_matrix, drop = TRUE) {
  dims <- x$dim

  if (ncol(index_matrix) != length(dims)) {
    stop("Array indexing matrix must have ", length(dims), " columns")
  }

  # Validate indices
  for (dim_idx in 1:length(dims)) {
    indices <- index_matrix[, dim_idx]
    if (any(indices < 1) || any(indices > dims[dim_idx])) {
      stop("Indices for dimension ", dim_idx, " out of bounds")
    }
  }

  # Convert multi-dimensional coordinates to linear positions (column-major)
  linear_positions <- apply(index_matrix, 1, function(coord) {
    pos <- coord[1]
    multiplier <- dims[1]

    for (dim_idx in 2:length(dims)) {
      pos <- pos + (coord[dim_idx] - 1) * multiplier
      if (dim_idx < length(dims)) {
        multiplier <- multiplier * dims[dim_idx]
      }
    }
    pos
  })

  # Use position indexing
  return(extract_by_position_array(x, linear_positions, drop))
}

#' Standard array extraction (existing functionality)
extract_standard_array <- function(x, args, drop = TRUE) {
  dims <- x$dim
  n_dims <- length(dims)

  # Calculate which linear positions to extract based on multi-dimensional indices
  # This generates all combinations and converts to linear positions

  # Expand grid of all index combinations
  index_grid <- expand.grid(args)

  # Convert each combination to linear position (column-major)
  linear_positions <- apply(index_grid, 1, function(coord) {
    pos <- coord[1]
    multiplier <- dims[1]

    for (dim_idx in 2:n_dims) {
      pos <- pos + (coord[dim_idx] - 1) * multiplier
      if (dim_idx < n_dims) {
        multiplier <- multiplier * dims[dim_idx]
      }
    }
    pos
  })

  # Calculate new dimensions
  new_dims <- sapply(args, length)

  # If drop=TRUE and result is 1D, return vector
  if (drop && sum(new_dims > 1) <= 1) {
    return(extract_by_position_array(x, linear_positions, drop = TRUE))
  }

  # Otherwise return array with new dimensions
  query <- paste0(
    "SELECT value FROM (",
    "  SELECT value, ROW_NUMBER() OVER() as rn FROM (", x$query, ") numbered_subq",
    ") positioned WHERE rn IN (", paste(linear_positions, collapse = ", "), ")"
  )

  return(structure(list(
    con = x$con,
    table = x$table,
    query = query,
    dim = new_dims
  ), class = "db.array"))
}

#' ===========================================================================
#' DB.FACTOR CLASS AND FUNCTIONS
#' ===========================================================================

#' Create a database factor from a column or db.vector
#' @param x A MyDB object, db.vector, or character vector column name
#' @param levels Character vector of levels (optional, will be auto-detected if NULL)
#' @param labels Character vector of labels (optional, defaults to levels)
#' @param exclude Values to exclude when determining levels
#' @param ordered Logical, whether the factor is ordered
#' @param nmax Maximum number of levels (for safety)

#' @export
as.db.factor <- function(x, ...) {
  UseMethod("as.db.factor")
}

#' @export
as.db.factor.db.vector <- function(x, levels = NULL, labels = levels, exclude = NULL, ordered = FALSE, nmax = NA, ...) {
  create_db_factor(
    con = x$con,
    table = x$table,
    column = x$column,
    query = x$query,
    levels = levels,
    labels = labels,
    exclude = exclude,
    ordered = ordered,
    nmax = nmax
  )
}

#' @export
as.db.factor.MyDB <- function(x, column, levels = NULL, labels = levels, exclude = NULL, ordered = FALSE, nmax = NA, ...) {
  if (missing(column)) {
    stop("column argument is required")
  }

  # Build the query for the specific column
  query <- paste0("SELECT ", column, " FROM ", x$table)

  create_db_factor(
    con = x$con,
    table = x$table,
    column = column,
    query = query,
    levels = levels,
    labels = labels,
    exclude = exclude,
    ordered = ordered,
    nmax = nmax
  )
}

#' @export
as.db.factor.character <- function(x, levels = NULL, labels = levels, exclude = NULL, ordered = FALSE, nmax = NA, ...) {
  # For creating factor from character vector - convert to regular R factor
  base::factor(x, levels = levels, labels = labels, exclude = exclude, ordered = ordered, ...)
}

# Make as.factor work with db objects too
#' @export
as.factor.MyDB <- function(x, column = NULL, ...) {
  as.db.factor.MyDB(x, column = column, ...)
}

#' @export
as.factor.db.vector <- function(x, ...) {
  as.db.factor.db.vector(x, ...)
}

# Internal function to create db.factor
create_db_factor <- function(con, table, column, query, levels = NULL, labels = levels, exclude = NULL, ordered = FALSE, nmax = NA) {

  # Generate unique table names for storing factor metadata
  factor_id <- paste0("factor_", format(Sys.time(), "%Y%m%d_%H%M%S"), "_", sample(1000:9999, 1))
  levels_table <- paste0("tmp_levels_", factor_id)

  # Auto-detect levels if not provided
  if (is.null(levels)) {
    # Get unique values from the database, excluding NULLs and excluded values
    exclude_clause <- ""
    if (!is.null(exclude)) {
      exclude_values <- paste0("'", gsub("'", "''", exclude), "'", collapse = ", ")
      exclude_clause <- paste0(" AND ", column, " NOT IN (", exclude_values, ")")
    }

    distinct_query <- paste0("SELECT DISTINCT ", column, " FROM (", query, ") subq WHERE ", column, " IS NOT NULL", exclude_clause, " ORDER BY ", column)

    # Apply nmax limit if specified
    if (!is.na(nmax)) {
      distinct_query <- paste0(distinct_query, " LIMIT ", nmax)
    }

    unique_values <- DBI::dbGetQuery(con, distinct_query)
    levels <- as.character(unique_values[[column]])
  }

  # Handle labels
  if (is.null(labels)) {
    labels <- levels
  } else if (length(labels) != length(levels)) {
    stop("Length of labels must equal length of levels")
  }

  # Create temporary table to store levels and labels on disk
  levels_df <- data.frame(
    level_order = seq_along(levels),
    level_value = levels,
    level_label = labels,
    stringsAsFactors = FALSE
  )

  # Write levels table to database
  DBI::dbWriteTable(con, levels_table, levels_df, temporary = TRUE, overwrite = TRUE)

  # Create the db.factor object - NO levels/labels in RAM!
  structure(
    list(
      con = con,
      table = table,
      column = column,
      query = query,
      levels_table = levels_table,  # Reference to disk table ONLY
      ordered = ordered,
      exclude = exclude,
      factor_id = factor_id
      # NO levels or labels stored in RAM!
    ),
    class = if(ordered) c("db.factor", "db.ordered") else "db.factor"
  )
}

#' Get levels from disk (lazy - only when needed)
#' @export
levels.db.factor <- function(x) {
  # Return the level_label (what user sees), not level_value
  labels_query <- paste0("SELECT level_label FROM ", x$levels_table, " ORDER BY level_order")
  result <- DBI::dbGetQuery(x$con, labels_query)
  result$level_label
}

#' @export
level_values <- function(x) {
  if (!inherits(x, "db.factor")) {
    stop("x must be a db.factor")
  }

  values_query <- paste0("SELECT level_value FROM ", x$levels_table, " ORDER BY level_order")
  result <- DBI::dbGetQuery(x$con, values_query)
  result$level_value
}

#' Get labels from disk (lazy - only when needed)
#' @export
labels.db.factor <- function(object, ...) {
  labels_query <- paste0("SELECT level_label FROM ", object$levels_table, " ORDER BY level_order")
  result <- DBI::dbGetQuery(object$con, labels_query)
  result$level_label
}

#' Get number of levels from disk
#' @export
nlevels.db.factor <- function(x) {
  count_query <- paste0("SELECT COUNT(*) as n FROM ", x$levels_table)
  result <- DBI::dbGetQuery(x$con, count_query)
  result$n[1]
}

#' Set levels - updates the disk table
#' @export
`levels<-.db.factor` <- function(x, value) {
  current_levels <- levels(x)
  if (length(value) != length(current_levels)) {
    stop("Length of new levels must equal length of current levels")
  }

  # Update the levels table on disk - but keep level_value same, change level_label
  for (i in seq_along(value)) {
    update_query <- paste0("UPDATE ", x$levels_table,
                           " SET level_label = '", gsub("'", "''", value[i]), "' ",
                           " WHERE level_order = ", i)
    DBI::dbExecute(x$con, update_query)
  }

  x
}

#' @export
is.factor.db.factor <- function(x) {
  TRUE
}

#' @export
is.ordered.db.factor <- function(x) {
  x$ordered
}

#' Enhanced print method that fetches levels from disk only for display
#' @export
print.db.factor <- function(x, ...) {
  preview_limits <- get_preview_limit()
  cat("db.factor from ", x$table, ", column: ", x$column, " (showing first ", preview_limits$rows, " values)\n")

  # Get preview data
  preview_query <- paste0("SELECT ", x$column, " FROM (", x$query, ") subq LIMIT ", preview_limits$rows)
  result <- DBI::dbGetQuery(x$con, preview_query)
  values <- result[[x$column]]

  # Get levels and labels from disk for display
  levels_data <- DBI::dbGetQuery(x$con, paste0("SELECT level_value, level_label FROM ", x$levels_table, " ORDER BY level_order"))
  disk_levels <- levels_data$level_value
  disk_labels <- levels_data$level_label

  # Convert to factor values using levels/labels mapping
  factor_values <- factor(values, levels = disk_levels, labels = disk_labels, ordered = x$ordered)

  # Print values - these should now show the labels, not NA
  print(as.character(factor_values))

  # Show levels using labels, not level_values - limit based on preview_limits$cols
  if (length(disk_labels) <= preview_limits$cols) {
    if (x$ordered) {
      cat("Levels:", paste(disk_labels, collapse = " < "), "\n")
    } else {
      cat("Levels:", paste(disk_labels, collapse = " "), "\n")
    }
  } else {
    # Show first few and last few levels for large factors
    show_first <- min(preview_limits$cols %/% 2, 10)
    show_last <- min(3, length(disk_labels) - show_first)

    if (show_first + show_last < length(disk_labels)) {
      show_levels <- c(disk_labels[1:show_first], "...", disk_labels[(length(disk_labels)-show_last+1):length(disk_labels)])
    } else {
      show_levels <- disk_labels[1:preview_limits$cols]
    }

    if (x$ordered) {
      cat(length(disk_labels), "Levels:", paste(show_levels, collapse = " < "), "\n")
    } else {
      cat(length(disk_labels), "Levels:", paste(show_levels, collapse = " "), "\n")
    }
  }
}

#' @export
check_db_factor <- function(x) {
  if (!inherits(x, "db.factor")) {
    stop("x must be a db.factor")
  }

  cat("=== DB Factor Diagnostics ===\n")
  cat("Levels table:", x$levels_table, "\n")

  # Show levels table contents
  levels_data <- DBI::dbGetQuery(x$con, paste0("SELECT * FROM ", x$levels_table, " ORDER BY level_order"))
  cat("\nLevels table contents:\n")
  print(levels_data)

  # Test a small sample
  cat("\nTesting sample data:\n")
  sample_query <- paste0("SELECT ", x$column, " FROM (", x$query, ") subq LIMIT 3")
  sample_data <- DBI::dbGetQuery(x$con, sample_query)
  cat("Raw values:", paste(sample_data[[x$column]], collapse = ", "), "\n")

  # Test the mapping
  mapped_query <- paste0(
    "SELECT data.", x$column, " as raw_value, levels.level_label as mapped_label
    FROM (", sample_query, ") data
    LEFT JOIN ", x$levels_table, " levels ON data.", x$column, " = levels.level_value
    LIMIT 3"
  )
  mapped_result <- DBI::dbGetQuery(x$con, mapped_query)
  cat("Mapping test:\n")
  print(mapped_result)

  # Check for unmapped values
  unmapped_query <- paste0(
    "SELECT COUNT(*) as unmapped_count
    FROM (", x$query, ") data
    LEFT JOIN ", x$levels_table, " levels ON data.", x$column, " = levels.level_value
    WHERE levels.level_value IS NULL AND data.", x$column, " IS NOT NULL"
  )
  unmapped_result <- DBI::dbGetQuery(x$con, unmapped_query)
  cat("Unmapped values count:", unmapped_result$unmapped_count, "\n")

  if (unmapped_result$unmapped_count > 0) {
    cat("WARNING: There are unmapped values! This will cause NA results.\n")

    # Show some unmapped values
    unmapped_values_query <- paste0(
      "SELECT DISTINCT data.", x$column, " as unmapped_value
      FROM (", x$query, ") data
      LEFT JOIN ", x$levels_table, " levels ON data.", x$column, " = levels.level_value
      WHERE levels.level_value IS NULL AND data.", x$column, " IS NOT NULL
      LIMIT 5"
    )
    unmapped_values <- DBI::dbGetQuery(x$con, unmapped_values_query)
    cat("Sample unmapped values:\n")
    print(unmapped_values)
  }

  cat("=== End Diagnostics ===\n")
}

#' @export
rebuild_db_factor <- function(x, new_levels = NULL) {
  if (!inherits(x, "db.factor")) {
    stop("x must be a db.factor")
  }

  # Get current unique values from the data
  current_values_query <- paste0("SELECT DISTINCT ", x$column, " FROM (", x$query, ") subq WHERE ", x$column, " IS NOT NULL ORDER BY ", x$column)
  current_values <- DBI::dbGetQuery(x$con, current_values_query)
  actual_levels <- as.character(current_values[[x$column]])

  if (is.null(new_levels)) {
    new_levels <- actual_levels
  }

  # Rebuild the levels table
  new_levels_df <- data.frame(
    level_order = seq_along(new_levels),
    level_value = new_levels,
    level_label = new_levels,
    stringsAsFactors = FALSE
  )

  # Write to NEW levels table
  DBI::dbExecute(x$con, paste0("DROP TABLE IF EXISTS ", x$levels_table))
  DBI::dbWriteTable(x$con, x$levels_table, new_levels_df, temporary = TRUE, overwrite = TRUE)

  cat("Factor rebuilt with levels:", paste(new_levels, collapse = ", "), "\n")
  x
}

#' Table method that works entirely on disk
#' @export
table.db.factor <- function(x, useNA = c("no", "ifany", "always"), ...) {
  useNA <- match.arg(useNA)

  # Build the main count query
  count_query <- paste0(
    "SELECT l.level_label, COALESCE(c.count, 0) as count
    FROM ", x$levels_table, " l
    LEFT JOIN (
      SELECT ", x$column, " as level_value, COUNT(*) as count
      FROM (", x$query, ") subq
      WHERE ", x$column, " IS NOT NULL
      GROUP BY ", x$column, "
    ) c ON l.level_value = c.level_value
    ORDER BY l.level_order"
  )

  result <- DBI::dbGetQuery(x$con, count_query)

  # Handle NA values if requested
  if (useNA %in% c("ifany", "always")) {
    na_query <- paste0("SELECT COUNT(*) as na_count FROM (", x$query, ") subq WHERE ", x$column, " IS NULL")
    na_result <- DBI::dbGetQuery(x$con, na_query)
    na_count <- na_result$na_count[1]

    if (useNA == "always" || (useNA == "ifany" && na_count > 0)) {
      result <- rbind(result, data.frame(level_label = "<NA>", count = na_count, stringsAsFactors = FALSE))
    }
  }

  # Create the result as a proper table
  counts <- result$count
  names(counts) <- result$level_label

  # This creates a proper table object that R recognizes
  result_table <- array(counts, dim = length(counts), dimnames = list(names(counts)))
  class(result_table) <- "table"

  return(result_table)
}

#' Summary method using disk-based operations
#' @export
summary.db.factor <- function(object, maxsum = 10, ...) {
  # Get frequency counts using table method (which uses disk)
  level_counts <- table.db.factor(object)

  # Convert to named vector for manipulation
  level_counts <- as.vector(level_counts)
  names(level_counts) <- names(table.db.factor(object))

  # Handle maxsum limit like base R
  if (length(level_counts) > maxsum) {
    # Show top (maxsum-1) levels and aggregate rest as "Other"
    sorted_counts <- sort(level_counts, decreasing = TRUE)
    top_counts <- sorted_counts[1:(maxsum-1)]
    other_count <- sum(sorted_counts[maxsum:length(sorted_counts)])
    result <- c(top_counts, Other = other_count)
  } else {
    result <- level_counts
  }

  # Return as proper table
  result <- as.table(result)
  return(result)
}

#' @export
unique.db.factor <- function(x, incomparables = FALSE, ...) {
  # Get unique values and create a new factor with same levels
  distinct_query <- paste0("SELECT DISTINCT ", x$column, " FROM (", x$query, ") subq WHERE ", x$column, " IS NOT NULL ORDER BY ", x$column)

  structure(
    list(
      con = x$con,
      table = x$table,
      column = x$column,
      query = distinct_query,
      levels_table = x$levels_table,  # Keep same levels table
      ordered = x$ordered,
      exclude = x$exclude,
      factor_id = x$factor_id
    ),
    class = class(x)
  )
}

#' Relevel method that updates disk table
#' @export
relevel.db.factor <- function(x, ref, ...) {
  # Get current data from disk
  current_data <- DBI::dbGetQuery(x$con, paste0("SELECT * FROM ", x$levels_table, " ORDER BY level_order"))

  # Find the reference level by label (not value!)
  ref_row <- which(current_data$level_label == ref)
  if (length(ref_row) == 0) {
    # Try by level_value
    ref_row <- which(current_data$level_value == ref)
  }

  if (length(ref_row) == 0) {
    stop("Reference level '", ref, "' not found in factor levels")
  }

  # Reorder: put ref first, then the rest
  new_order <- c(ref_row, setdiff(seq_len(nrow(current_data)), ref_row))
  reordered_data <- current_data[new_order, ]

  # Update level_order
  reordered_data$level_order <- seq_len(nrow(reordered_data))

  # Replace the entire levels table
  DBI::dbExecute(x$con, paste0("DELETE FROM ", x$levels_table))
  DBI::dbWriteTable(x$con, x$levels_table, reordered_data, append = TRUE)

  x
}

#' @export
repair_db_factor <- function(x) {
  if (!inherits(x, "db.factor")) {
    stop("x must be a db.factor")
  }

  # Get the current levels table
  current_data <- DBI::dbGetQuery(x$con, paste0("SELECT * FROM ", x$levels_table, " ORDER BY level_order"))

  # If level_value and level_label got out of sync, reset them
  cat("Current levels table:\n")
  print(current_data)

  # Ask user if they want to reset
  cat("\nDo you want to reset level_label to match level_value? (y/n): ")
  response <- readline()

  if (tolower(response) == "y") {
    # Reset level_label to match level_value
    for (i in seq_len(nrow(current_data))) {
      update_query <- paste0("UPDATE ", x$levels_table,
                             " SET level_label = level_value ",
                             " WHERE level_order = ", current_data$level_order[i])
      DBI::dbExecute(x$con, update_query)
    }
    cat("Factor repaired!\n")
  }

  x
}

#' @export
droplevels.db.factor <- function(x, ...) {
  # Get actually used level_values from the database
  used_query <- paste0("SELECT DISTINCT ", x$column, " FROM (", x$query, ") subq WHERE ", x$column, " IS NOT NULL")
  used_values <- DBI::dbGetQuery(x$con, used_query)

  if (nrow(used_values) == 0) {
    # No data, create empty factor with new levels table
    new_factor_id <- paste0("factor_", format(Sys.time(), "%Y%m%d_%H%M%S"), "_", sample(1000:9999, 1))
    new_levels_table <- paste0("tmp_levels_", new_factor_id)

    # Create empty levels table
    empty_levels_df <- data.frame(
      level_order = integer(0),
      level_value = character(0),
      level_label = character(0),
      stringsAsFactors = FALSE
    )
    DBI::dbWriteTable(x$con, new_levels_table, empty_levels_df, temporary = TRUE, overwrite = TRUE)

    # Return new factor object with new levels table
    return(structure(
      list(
        con = x$con,
        table = x$table,
        column = x$column,
        query = x$query,
        levels_table = new_levels_table,  # NEW levels table
        ordered = x$ordered,
        exclude = x$exclude,
        factor_id = new_factor_id  # NEW factor ID
      ),
      class = class(x)
    ))
  }

  used_levels <- as.character(used_values[[x$column]])

  # Get current levels data and filter to used ones only
  current_data <- DBI::dbGetQuery(x$con, paste0("SELECT * FROM ", x$levels_table, " ORDER BY level_order"))

  # Filter to used levels only (match by level_value, not level_label)
  used_mask <- current_data$level_value %in% used_levels
  filtered_data <- current_data[used_mask, ]

  if (nrow(filtered_data) > 0) {
    # Reorder the level_order sequentially
    filtered_data$level_order <- seq_len(nrow(filtered_data))

    # CREATE NEW levels table instead of modifying existing one
    new_factor_id <- paste0("factor_", format(Sys.time(), "%Y%m%d_%H%M%S"), "_", sample(1000:9999, 1))
    new_levels_table <- paste0("tmp_levels_", new_factor_id)

    # Write to NEW levels table
    DBI::dbWriteTable(x$con, new_levels_table, filtered_data, temporary = TRUE, overwrite = TRUE)

    # Return NEW factor object with NEW levels table
    return(structure(
      list(
        con = x$con,
        table = x$table,
        column = x$column,
        query = x$query,
        levels_table = new_levels_table,  # NEW levels table
        ordered = x$ordered,
        exclude = x$exclude,
        factor_id = new_factor_id  # NEW factor ID
      ),
      class = class(x)
    ))
  } else {
    # No levels left - create empty factor
    new_factor_id <- paste0("factor_", format(Sys.time(), "%Y%m%d_%H%M%S"), "_", sample(1000:9999, 1))
    new_levels_table <- paste0("tmp_levels_", new_factor_id)

    empty_levels_df <- data.frame(
      level_order = integer(0),
      level_value = character(0),
      level_label = character(0),
      stringsAsFactors = FALSE
    )
    DBI::dbWriteTable(x$con, new_levels_table, empty_levels_df, temporary = TRUE, overwrite = TRUE)

    return(structure(
      list(
        con = x$con,
        table = x$table,
        column = x$column,
        query = x$query,
        levels_table = new_levels_table,  # NEW levels table
        ordered = x$ordered,
        exclude = x$exclude,
        factor_id = new_factor_id  # NEW factor ID
      ),
      class = class(x)
    ))
  }
}

#' Cleanup method to remove temporary tables
#' @export
cleanup.db.factor <- function(x) {
  if (!is.null(x$levels_table)) {
    DBI::dbExecute(x$con, paste0("DROP TABLE IF EXISTS ", x$levels_table))
    message("Cleaned up temporary factor table: ", x$levels_table)
  }
}

#' Finalizer to auto-cleanup when db.factor is garbage collected
.onLoad <- function(libname, pkgname) {
  # Register finalizer for db.factor objects
  reg.finalizer(environment(), function(e) {
    # This is a simplified approach - in practice, you'd need a more sophisticated cleanup
    message("Package unloaded - temporary factor tables may remain in database")
  }, onexit = TRUE)
}

#' @export
reorder.db.factor <- function(x, X, FUN = mean, ..., order = is.ordered(x)) {
  if (!inherits(X, c("db.vector", "numeric"))) {
    stop("X must be a db.vector or numeric vector")
  }

  if (inherits(X, "db.vector")) {
    # Calculate summary statistic for each level
    summary_query <- paste0(
      "SELECT ", x$column, ", ", toupper(deparse(substitute(FUN))), "(", X$column, ") as stat ",
      "FROM (", x$query, ") f_subq ",
      "JOIN (", X$query, ") x_subq ON f_subq.ROWID = x_subq.ROWID ",
      "GROUP BY ", x$column, " ORDER BY stat"
    )

    result <- DBI::dbGetQuery(x$con, summary_query)
    new_order <- result[[x$column]]
  } else {
    # X is a regular R vector - need to handle differently
    stop("Reordering with R vectors not yet implemented for db.factor")
  }

  # Reorder levels based on the statistic
  x$levels <- new_order
  x$labels <- new_order
  x$ordered <- order

  if (order) {
    class(x) <- c("db.factor", "db.ordered")
  }

  x
}

#' @export
`[.db.factor` <- function(x, i, drop = FALSE) {
  if (inherits(i, "db.vector")) {
    # i is a logical db.vector (like from x == "Female")
    # We need to build a filtered query

    # Extract the WHERE condition from the logical vector's query
    logical_condition <- extract_where_condition(i$query)

    # Apply this condition to filter our data
    if (logical_condition == "") {
      filtered_query <- x$query
    } else {
      # Remove any existing WHERE clause from our query and add the new one
      base_query <- remove_where_clause(x$query)
      filtered_query <- paste0("SELECT * FROM (", base_query, ") subq WHERE ", logical_condition)
    }
  } else if (is.numeric(i)) {
    # Numeric subsetting
    if (length(i) == 1 && is.na(i)) {
      # Handle NA indexing
      filtered_query <- paste0("SELECT * FROM (", x$query, ") subq WHERE 1=0") # Empty result
    } else {
      # Add row numbers and filter
      rn_query <- paste0("SELECT ROW_NUMBER() OVER() as rn, * FROM (", x$query, ") subq")
      index_list <- paste(i[!is.na(i)], collapse = ", ")
      filtered_query <- paste0("SELECT * FROM (", rn_query, ") numbered WHERE rn IN (", index_list, ")")
    }
  } else {
    # Fallback - return original
    filtered_query <- x$query
  }

  result <- structure(
    list(
      con = x$con,
      table = x$table,
      column = x$column,
      query = filtered_query,
      levels_table = x$levels_table,  # Keep same levels table reference
      ordered = x$ordered,
      exclude = x$exclude,
      factor_id = x$factor_id
    ),
    class = class(x)
  )

  # Drop unused levels if requested
  if (drop) {
    result <- droplevels(result)
  }

  result
}

# Helper functions for the subsetting method
extract_where_condition <- function(query) {
  # Extract the WHERE condition from a query like "SELECT condition as result FROM ..."
  # This is a simplified version - you may need to enhance this
  if (grepl("SELECT \\((.+)\\) as result FROM", query, perl = TRUE)) {
    condition <- gsub(".*SELECT \\((.+)\\) as result FROM.*", "\\1", query, perl = TRUE)
    return(condition)
  }
  return("")
}

remove_where_clause <- function(query) {
  # Remove WHERE clause from query - simplified version
  gsub("\\s+WHERE\\s+.*$", "", query, perl = TRUE)
}

#' @export
as.character.db.factor <- function(x, ...) {
  # Return character representation - this will pull all data!
  all_query <- paste0("SELECT ", x$column, " FROM (", x$query, ") subq")
  result <- DBI::dbGetQuery(x$con, all_query)
  values <- result[[x$column]]

  # Get levels and labels from disk
  levels_data <- DBI::dbGetQuery(x$con, paste0("SELECT level_value, level_label FROM ", x$levels_table, " ORDER BY level_order"))
  disk_levels <- levels_data$level_value
  disk_labels <- levels_data$level_label

  # Map through levels/labels
  factor_values <- factor(values, levels = disk_levels, labels = disk_labels, ordered = x$ordered)
  as.character(factor_values)
}

#' @export
as.numeric.db.factor <- function(x, ...) {
  # Return a LAZY db.vector of integer codes (not pulled into R!)
  # Map to integer codes based on level_order
  case_query <- paste0(
    "SELECT CAST(COALESCE(levels.level_order, 0) AS DOUBLE) as result
    FROM (", x$query, ") data
    LEFT JOIN ", x$levels_table, " levels
    ON CAST(data.", x$column, " AS TEXT) = CAST(levels.level_value AS TEXT)"
  )

  structure(list(con = x$con, table = x$table, column = "result", query = case_query), class = "db.vector")
}

#' @export
as.integer.db.factor <- function(x, ...) {
  # Return a LAZY db.vector of integer codes (not pulled into R!)
  case_query <- paste0(
    "SELECT CAST(COALESCE(levels.level_order, 0) AS INTEGER) as result
    FROM (", x$query, ") data
    LEFT JOIN ", x$levels_table, " levels
    ON CAST(data.", x$column, " AS TEXT) = CAST(levels.level_value AS TEXT)"
  )

  structure(list(con = x$con, table = x$table, column = "result", query = case_query), class = "db.vector")
}
#' @export
as.db.factor.db.factor <- function(x, levels = NULL, labels = levels, exclude = NULL, ordered = FALSE, nmax = NA, ...) {
  # Convert existing db.factor to new db.factor with different levels/labels
  create_db_factor(
    con = x$con,
    table = x$table,
    column = x$column,
    query = x$query,  # Use the existing query
    levels = levels,
    labels = labels,
    exclude = exclude,
    ordered = ordered,
    nmax = nmax
  )
}

#' Convert db.factor to regular R factor (pulls all data)
#' @export
as.ram.db.factor <- function(x) {
  # Get all data
  all_query <- paste0("SELECT ", x$column, " FROM (", x$query, ") subq")
  result <- DBI::dbGetQuery(x$con, all_query)
  values <- result[[x$column]]

  # Get levels and labels from disk
  levels_data <- DBI::dbGetQuery(x$con, paste0("SELECT level_value, level_label FROM ", x$levels_table, " ORDER BY level_order"))
  disk_levels <- levels_data$level_value
  disk_labels <- levels_data$level_label

  factor(values, levels = disk_levels, labels = disk_labels, ordered = x$ordered)
}

#' @export
interaction <- function(..., drop = FALSE, sep = ".", lex.order = FALSE) {
  factors <- list(...)

  # Check if any are db objects
  if (any(sapply(factors, function(f) inherits(f, c("db.factor", "db.vector"))))) {
    # Convert all to db.factor if needed
    db_factors <- lapply(factors, function(f) {
      if (inherits(f, "db.vector")) {
        as.db.factor(f)
      } else if (inherits(f, "db.factor")) {
        f
      } else {
        stop("Cannot mix db objects with regular R objects in interaction")
      }
    })

    if (length(db_factors) < 2) {
      stop("At least two factors required for interaction")
    }

    # Check all are from same connection
    first_con <- db_factors[[1]]$con
    if (!all(sapply(db_factors, function(f) identical(f$con, first_con)))) {
      stop("All db.factors must be from same database connection")
    }

    # Get the base table name (assume all factors are from the same base table)
    base_table <- db_factors[[1]]$table

    # Build interaction query by joining all factor queries
    # Create a unified query that selects all needed columns
    all_columns <- sapply(db_factors, function(f) f$column)
    base_query <- paste0("SELECT ", paste(all_columns, collapse = ", "), " FROM ", base_table)

    # Create interaction expression using direct column references
    factor_exprs <- sapply(seq_along(db_factors), function(i) {
      f <- db_factors[[i]]
      col_name <- f$column

      # Map factor values to labels using COALESCE and subquery
      paste0("COALESCE((SELECT level_label FROM ", f$levels_table,
             " WHERE level_value = base.", col_name, "), CAST(base.", col_name, " AS TEXT))")
    })

    # Use || for concatenation
    concat_expr <- paste(factor_exprs, collapse = paste0(" || '", sep, "' || "))

    # Build the final interaction query
    interaction_query <- paste0("SELECT ", concat_expr, " as interaction FROM (", base_query, ") base")

    # Get all possible level combinations for the new factor levels
    level_lists <- lapply(db_factors, function(f) levels(f))
    if (length(level_lists) >= 2) {
      level_combinations <- expand.grid(level_lists, stringsAsFactors = FALSE)
      interaction_levels <- apply(level_combinations, 1, paste, collapse = sep)
    } else {
      interaction_levels <- character(0)
    }

    # Create new factor with interaction levels
    create_db_factor(
      con = first_con,
      table = base_table,
      column = "interaction",
      query = interaction_query,
      levels = interaction_levels,
      labels = interaction_levels,
      ordered = FALSE
    )
  } else {
    # Regular R interaction
    base::interaction(..., drop = drop, sep = sep, lex.order = lex.order)
  }
}

#' Split data by factor levels - FIXED VERSION
#' @export
split <- function(x, f, drop = FALSE, ...) {
  if (inherits(x, "db.vector") && inherits(f, "db.factor")) {
    # Get the levels to split by
    split_levels <- levels(f)
    result <- list()

    for (level in split_levels) {
      # Map level_label to level_value for filtering
      level_mapping_query <- paste0("SELECT level_value FROM ", f$levels_table, " WHERE level_label = '", gsub("'", "''", level), "'")
      level_result <- DBI::dbGetQuery(f$con, level_mapping_query)

      if (nrow(level_result) > 0) {
        actual_value <- level_result$level_value[1]

        # Create filtered query for this level - KEY FIX: Use the base table directly
        # Instead of joining subqueries, filter the base table directly
        if (x$table == f$table) {
          # Both from same table - simple filter
          filtered_query <- paste0("SELECT ", x$column, " FROM ", x$table, " WHERE ", f$column, " = '", gsub("'", "''", actual_value), "'")
        } else {
          # Different tables - this is more complex and may require additional logic
          stop("Split not yet supported for db.vector and db.factor from different tables")
        }

        result[[level]] <- structure(
          list(
            con = x$con,
            table = x$table,
            column = x$column,
            query = filtered_query
          ),
          class = "db.vector"
        )
      }
    }

    # Remove empty empty levels if drop = TRUE
    if (drop) {
      non_empty <- sapply(result, function(vec) {
        count_query <- paste0("SELECT COUNT(*) as n FROM (", vec$query, ") subq")
        count_result <- DBI::dbGetQuery(vec$con, count_query)
        count_result$n[1] > 0
      })
      result <- result[non_empty]
    }

    return(result)
  } else {
    # Regular R split
    base::split(x, f, drop = drop, ...)
  }
}

#' @export
`==.db.factor` <- function(e1, e2) {
  if (!inherits(e1, "db.factor")) {
    stop("First argument must be a db.factor")
  }

  # Only handle factor-specific level mapping
  if (is.character(e2) || is.factor(e2)) {
    compare_value <- as.character(e2)[1]

    # Get level mapping from disk
    levels_data <- DBI::dbGetQuery(e1$con, paste0("SELECT level_value, level_label FROM ", e1$levels_table))

    # Find match in either level_value or level_label
    label_match <- levels_data$level_value[levels_data$level_label == compare_value]
    value_match <- levels_data$level_value[levels_data$level_value == compare_value]

    if (length(label_match) > 0) {
      actual_value <- label_match[1]
    } else if (length(value_match) > 0) {
      actual_value <- value_match[1]
    } else {
      # No match - create always-false condition
      condition <- "0 = 1"
      query <- paste0("SELECT (", condition, ") as result FROM (", e1$query, ") subq")
      return(structure(list(con = e1$con, table = e1$table, column = "result", query = query), class = "db.vector"))
    }

    # Create WHERE condition with mapped value
    condition <- paste0(e1$column, " = '", gsub("'", "''", actual_value), "'")

  } else if (is.numeric(e2)) {
    # For numeric factors, compare directly
    compare_value <- e2[1]
    condition <- paste0("CAST(", e1$column, " AS DOUBLE) = ", compare_value)

  } else if (inherits(e2, "db.factor")) {
    # Comparing two factors - just compare the underlying columns
    condition <- paste0("f1.", e1$column, " = f2.", e2$column)
    query <- paste0("SELECT (", condition, ") as result FROM (", e1$query, ") f1 JOIN (", e2$query, ") f2 ON f1.ROWID = f2.ROWID")
    return(structure(list(con = e1$con, table = e1$table, column = "result", query = query), class = "db.vector"))

  } else {
    stop("Cannot compare db.factor with ", class(e2))
  }

  # Return simple db.vector - the rest is handled by existing operators
  query <- paste0("SELECT (", condition, ") as result FROM (", e1$query, ") subq")
  return(structure(list(con = e1$con, table = e1$table, column = "result", query = query), class = "db.vector"))
}

#' @export
`%in%` <- function(x, table) {
  if (!inherits(x, "db.factor")) {
    return(base::`%in%`(x, table))
  }
  UseMethod("%in%")
}

#' @export
`%in%.db.factor` <- function(x, table) {
  # Handle numeric input for numeric factors
  if (is.numeric(table)) {
    table <- as.character(table)
  }

  if (is.character(table) || is.factor(table)) {
    compare_values <- as.character(table)

    # Get level mappings from disk
    levels_data <- DBI::dbGetQuery(x$con, paste0("SELECT level_value, level_label FROM ", x$levels_table))

    # Find all matches
    value_matches <- levels_data$level_value[levels_data$level_value %in% compare_values]
    label_matches <- levels_data$level_value[levels_data$level_label %in% compare_values]
    all_matches <- unique(c(value_matches, label_matches))

    if (length(all_matches) == 0) {
      condition <- "0 = 1"  # Always false
    } else {
      values_str <- paste0("'", gsub("'", "''", all_matches), "'", collapse = ", ")
      condition <- paste0(x$column, " IN (", values_str, ")")
    }

    # Return simple db.vector
    query <- paste0("SELECT (", condition, ") as result FROM (", x$query, ") subq")
    return(structure(list(con = x$con, table = x$table, column = "result", query = query), class = "db.vector"))

  } else {
    stop("table must be character, factor, or numeric vector")
  }
}

#' @export
`%in%.default` <- function(x, table) {
  base::`%in%`(x, table)
}

#' @export
`%in%.default` <- function(x, table) {
  base::`%in%`(x, table)
}

#' Helper to fix %in% for numeric factors
#' @export
fix_numeric_factor_in <- function(x, table) {
  if (!inherits(x, "db.factor")) {
    stop("x must be a db.factor")
  }

  if (is.numeric(table)) {
    # Check if factor has numeric levels
    levels_data <- DBI::dbGetQuery(x$con, paste0("SELECT level_value FROM ", x$levels_table, " LIMIT 1"))
    first_level <- levels_data$level_value[1]
    is_numeric_factor <- !is.na(suppressWarnings(as.numeric(first_level)))

    if (!is_numeric_factor) {
      stop("Cannot use numeric values with non-numeric db.factor")
    }

    # Convert numeric table to character for matching
    table <- as.character(table)
  }

  # Now use the regular %in% method
  x %in% table
}

#' @export
prop.table.table <- function(x, margin = NULL) {
  # This works with the table returned by table.db.factor
  base::prop.table(x, margin)
}

#' Cross-tabulation for db.factors
#' @export
xtabs.formula <- function(formula, data, ...) {
  # For now, delegate to regular xtabs - full implementation would be complex
  stop("xtabs with db.factor not yet fully implemented")
}

#' Enhanced MyDB subsetting to work with factor results
update_mydb_subsetting <- function() {
  # This function shows where to update the existing `[.MyDB` method
  # Add this check in your existing `[.MyDB` method:

  # After determining final_cols and before the final return:
  # Check if the result should be a factor based on the original column
  # You could store factor information in the MyDB object or detect it

  # For now, users need to explicitly convert:
  # sex_factor <- as.db.factor(data[["sex"]])
}

#' is.na for factors
#' @export
is.na.db.factor <- function(x) {
  query <- paste0("SELECT (", x$column, " IS NULL) as result FROM (", x$query, ") subq")
  structure(list(con = x$con, table = x$table, column = "result", query = query), class = "db.vector")
}

#' Update the existing build_where_clause to handle db.factor logical operations
build_where_clause <- function(logical_expr, table_name) {
  if (missing(logical_expr) || is.null(logical_expr)) {
    return("")
  }

  if (inherits(logical_expr, "db.vector")) {
    # Extract the logical expression
    expr <- extract_expression_from_query(logical_expr)
    expr <- gsub("= ([A-Za-z][A-Za-z0-9_]*(?!['\"]))", "= '\\1'", expr, perl = TRUE)
    return(paste0(" WHERE ", expr))
  }

  # ADD THIS for db.factor support
  if (inherits(logical_expr, "db.factor")) {
    # This shouldn't happen - factors should be compared first to create db.vector
    stop("Use factor comparisons (==, %in%, etc.) to create logical conditions")
  }

  return("")
}

#' @export
ordered.MyDB <- function(x, column, levels = NULL, labels = levels, exclude = NULL, ...) {
  as.db.factor(x, column = column, levels = levels, labels = labels, exclude = exclude, ordered = TRUE, ...)
}

#' @export
ordered.db.vector <- function(x, levels = NULL, labels = levels, exclude = NULL, ...) {
  as.db.factor(x, levels = levels, labels = labels, exclude = exclude, ordered = TRUE, ...)
}

#' Create factor from MyDB column (convenience function)
#' @export
factor.MyDB <- function(x, column, levels = NULL, labels = levels, exclude = NULL, ordered = FALSE, nmax = NA, ...) {
  as.db.factor(x, column = column, levels = levels, labels = labels, exclude = exclude, ordered = ordered, nmax = nmax, ...)
}

#' @export
factor.db.vector <- function(x, levels = NULL, labels = levels, exclude = NULL, ordered = FALSE, nmax = NA, ...) {
  as.db.factor(x, levels = levels, labels = labels, exclude = exclude, ordered = ordered, nmax = nmax, ...)
}

#' @export
factor <- function(x, ...) {
  if (inherits(x, "db.vector")) {
    factor.db.vector(x, ...)
  } else {
    base::factor(x, ...)
  }
}

#' @export
unique <- function(x, ...) {
  if (inherits(x, "db.factor")) {
    unique.db.factor(x, ...)
  } else {
    base::unique(x, ...)
  }
}

#' @export

#' Disk-based numeric conversion
#' @export

#' @export

#' @export
head.db.factor <- function(x, n = 6L, ...) {
  # Create a subset with the first n rows
  limited_query <- paste0("SELECT ", x$column, " FROM (", x$query, ") subq LIMIT ", n)

  structure(
    list(
      con = x$con,
      table = x$table,
      column = x$column,
      query = limited_query,
      levels_table = x$levels_table,  # ADD THIS - was missing!
      ordered = x$ordered,
      exclude = x$exclude,
      factor_id = x$factor_id  # ADD THIS - was missing!
    ),
    class = class(x)
  )
}

#' @export
str.db.factor <- function(object, ...) {
  # Get basic info
  n_levels <- nlevels(object)
  current_levels <- levels(object)

  # Get first few values for preview
  preview_query <- paste0("SELECT ", object$column, " FROM (", object$query, ") subq LIMIT 10")
  result <- DBI::dbGetQuery(object$con, preview_query)
  values <- result[[object$column]]

  # Convert to factor codes (integers)
  factor_values <- factor(values, levels = current_levels, ordered = object$ordered)
  codes <- as.integer(factor_values)

  # Format the output like base R
  if (object$ordered) {
    cat(" Ord.factor w/", n_levels, "levels")
  } else {
    cat(" Factor w/", n_levels, "levels")
  }

  # Show first few levels
  if (n_levels <= 6) {
    level_display <- paste0('"', current_levels, '"', collapse = ifelse(object$ordered, "<", " "))
  } else {
    level_display <- paste0('"', paste(current_levels[1:3], collapse = ifelse(object$ordered, "<", " ")),
                            ifelse(object$ordered, "<", " "), "...")
  }

  cat(" ", level_display, ": ")

  # Show the integer codes
  if (length(codes) > 0) {
    codes_display <- if(length(codes) > 10) {
      paste(c(codes[1:10], "..."), collapse = " ")
    } else {
      paste(codes, collapse = " ")
    }
    cat(codes_display)
  }
  cat("\n")
}

#' @export
table <- function(x, ...) {
  if (inherits(x, "db.factor")) {
    table.db.factor(x, ...)
  } else {
    base::table(x, ...)
  }
}

#' @export
summary <- function(object, ...) {
  if (inherits(object, "db.factor")) {
    summary.db.factor(object, ...)
  } else {
    base::summary(object, ...)
  }
}

#' Check if object is a db.factor
#' @export
is.db.factor <- function(x) {
  inherits(x, "db.factor")
}

#' Convert db.factor to db character vector
#' @export
as.db.character <- function(x, ...) {
  UseMethod("as.db.character")
}

#' Convert db.factor to db numeric vector
#' @export
as.db.numeric <- function(x, ...) {
  UseMethod("as.db.numeric")
}

#' Convert db.factor to db integer vector
#' @export
as.db.integer <- function(x, ...) {
  UseMethod("as.db.integer")
}

#' @export
as.db.character.db.factor <- function(x, ...) {
  # Get levels and labels from disk to create CASE mapping
  levels_data <- DBI::dbGetQuery(x$con, paste0("SELECT level_value, level_label FROM ", x$levels_table, " ORDER BY level_order"))

  if (nrow(levels_data) == 0) {
    # No levels, just return the column
    query <- paste0("SELECT CAST(", x$column, " AS TEXT) as result FROM (", x$query, ") subq")
  } else if (all(levels_data$level_value == levels_data$level_label)) {
    # No label mapping needed
    query <- paste0("SELECT CAST(", x$column, " AS TEXT) as result FROM (", x$query, ") subq")
  } else {
    # Need to map levels to labels using CASE
    case_parts <- sapply(seq_len(nrow(levels_data)), function(i) {
      paste0("WHEN ", x$column, " = '", gsub("'", "''", levels_data$level_value[i]), "' THEN '", gsub("'", "''", levels_data$level_label[i]), "'")
    })
    case_expr <- paste0("CASE ", paste(case_parts, collapse = " "), " ELSE CAST(", x$column, " AS TEXT) END")
    query <- paste0("SELECT ", case_expr, " as result FROM (", x$query, ") subq")
  }

  structure(list(con = x$con, table = x$table, column = "result", query = query), class = "db.vector")
}

#' @export
as.db.numeric.db.factor <- function(x, ...) {
  # Map to integer codes based on level_order
  case_query <- paste0(
    "SELECT CAST(COALESCE(levels.level_order, 0) AS NUMERIC) as result
    FROM (", x$query, ") data
    LEFT JOIN ", x$levels_table, " levels
    ON data.", x$column, " = levels.level_value"
  )

  structure(list(con = x$con, table = x$table, column = "result", query = case_query), class = "db.vector")
}

#' @export
as.db.integer.db.factor <- function(x, ...) {
  # Map to integer codes based on level_order
  case_query <- paste0(
    "SELECT CAST(COALESCE(levels.level_order, 0) AS INTEGER) as result
    FROM (", x$query, ") data
    LEFT JOIN ", x$levels_table, " levels
    ON data.", x$column, " = levels.level_value"
  )

  structure(list(con = x$con, table = x$table, column = "result", query = case_query), class = "db.vector")
}

#' @export
as.db.character.db.vector <- function(x, ...) {
  # Convert db.vector to character - cast in SQL
  query <- paste0("SELECT CAST(", x$column, " AS TEXT) as result FROM (", x$query, ") subq")
  structure(list(con = x$con, table = x$table, column = "result", query = query), class = "db.vector")
}

#' @export
as.db.numeric.db.vector <- function(x, ...) {
  # Convert db.vector to numeric - cast in SQL
  query <- paste0("SELECT CAST(", x$column, " AS NUMERIC) as result FROM (", x$query, ") subq")
  structure(list(con = x$con, table = x$table, column = "result", query = query), class = "db.vector")
}

#' @export
as.db.integer.db.vector <- function(x, ...) {
  # Convert db.vector to integer - cast in SQL
  query <- paste0("SELECT CAST(", x$column, " AS INTEGER) as result FROM (", x$query, ") subq")
  structure(list(con = x$con, table = x$table, column = "result", query = query), class = "db.vector")
}

#' @export
as.db.character.character <- function(x, ...) {
  # For character vectors, return as-is
  x
}

#' @export
as.db.numeric.character <- function(x, ...) {
  # For character vectors, convert normally
  as.numeric(x)
}

#' @export
as.db.integer.character <- function(x, ...) {
  # For character vectors, convert normally
  as.integer(x)
}

#' @export
debug_factor_levels <- function(x) {
  if (!inherits(x, "db.factor")) {
    stop("x must be a db.factor")
  }

  cat("=== Factor Level Debug Info ===\n")
  cat("Column:", x$column, "\n")
  cat("Levels table:", x$levels_table, "\n")

  # Show the levels table contents
  levels_data <- DBI::dbGetQuery(x$con, paste0("SELECT * FROM ", x$levels_table, " ORDER BY level_order"))
  cat("\nLevels table contents:\n")
  print(levels_data)

  # Show some actual data values
  sample_query <- paste0("SELECT DISTINCT ", x$column, " as actual_value FROM (", x$query, ") subq ORDER BY ", x$column, " LIMIT 10")
  actual_data <- DBI::dbGetQuery(x$con, sample_query)
  cat("\nActual data values (first 10 unique):\n")
  print(actual_data)

  cat("=== End Debug Info ===\n")
}

# 5. ADD this convenience function to rebuild factor with frequency-based levels:
#' @export
reorder_by_frequency <- function(x, decreasing = TRUE) {
  if (!inherits(x, "db.factor")) {
    stop("x must be a db.factor")
  }

  # Get frequency table
  freq_table <- table(x)

  # Sort by frequency
  if (decreasing) {
    sorted_levels <- names(sort(freq_table, decreasing = TRUE))
  } else {
    sorted_levels <- names(sort(freq_table, decreasing = FALSE))
  }

  # Create new factor with reordered levels
  as.db.factor(x, levels = sorted_levels)
}


#' @export
test_db_factor_ops <- function(f) {
  cat("=== Testing DB Factor Operations ===\n")

  # Test levels
  cat("Levels:", paste(levels(f), collapse = ", "), "\n")

  # Test equality
  first_level <- levels(f)[1]
  cat("Testing f ==", first_level, "\n")
  eq_result <- f == first_level
  cat("Equality query:", eq_result$query, "\n")

  # Test subsetting
  cat("Testing subsetting f[f ==", first_level, "]\n")
  subset_result <- f[f == first_level]
  cat("Subset query:", subset_result$query, "\n")

  # Test actual results
  preview_query <- paste0("SELECT ", subset_result$column, " FROM (", subset_result$query, ") subq LIMIT 5")
  preview_result <- DBI::dbGetQuery(subset_result$con, preview_query)
  cat("Subset preview:\n")
  print(preview_result)

  cat("=== End Testing ===\n")
}


#' ===========================================================================
#' FIXED DB.LIST CLASS AND FUNCTIONS
#' ===========================================================================

#' Create a database-backed list from columns or other db objects
#' @param x A MyDB, db.dataframe, or list of db objects
#' @param ... Additional arguments (e.g., names for elements)
#' @export
as.db.list <- function(x, ...) {
  UseMethod("as.db.list")
}

#' @export
as.db.list.MyDB <- function(x, ...) {
  col_names <- DBI::dbListFields(x$con, x$table)
  elements <- lapply(col_names, function(col) {
    query <- paste0("SELECT ", col, " FROM ", x$table)
    create_db_vector(x$con, x$table, col, query)
  })
  names(elements) <- col_names
  structure(elements, class = "db.list")
}

#' Updated as.db.list.db.dataframe method using enhanced vector creation
#' @export
as.db.list.db.dataframe <- function(x, ...) {
  elements <- lapply(x$columns, function(col) {
    query <- paste0("SELECT ", col, " FROM (", x$query, ") subq")
    create_db_vector(x$con, x$table, col, query)
  })
  names(elements) <- x$columns
  structure(elements, class = "db.list")
}


#' @export
as.db.list.list <- function(x, ...) {
  # Check if all elements are db objects
  is_db_object <- sapply(x, function(elem) inherits(elem, c("db.vector", "db.matrix", "db.array", "db.dataframe", "db.factor")))

  if (all(is_db_object)) {
    # All elements are db objects - convert to db.list
    structure(x, class = "db.list")
  } else {
    # Mixed or non-db objects - provide helpful error message
    non_db_elements <- names(x)[!is_db_object]
    if (is.null(non_db_elements)) {
      non_db_elements <- which(!is_db_object)
    }
    stop("All elements must be db objects to convert to db.list. Non-db elements found: ",
         paste(non_db_elements, collapse = ", "),
         "\nUse db.list() constructor for mixed objects or convert non-db elements to db objects first.")
  }
}

#' Constructor function for creating db.list from individual db objects
#' @param ... Named db objects (db.vector, db.matrix, etc.)
#' @export
db.list <- function(...) {
  elements <- list(...)

  # Check that all elements are db objects
  if (length(elements) > 0 && !all(sapply(elements, function(elem) {
    inherits(elem, c("db.vector", "db.matrix", "db.array", "db.dataframe", "db.factor"))
  }))) {
    stop("All elements must be db objects (db.vector, db.matrix, db.array, db.dataframe, or db.factor)")
  }

  structure(elements, class = "db.list")
}

#' Check if object is a db.list
#' @export
is.db.list <- function(x) {
  inherits(x, "db.list")
}

#' Print method for db.list with previews
#' @export
print.db.list <- function(x, ...) {
  preview_limits <- get_preview_limit()
  cat("db.list with ", length(x), " elements (showing previews):\n")

  for (i in seq_along(x)) {
    elem <- x[[i]]
    name <- names(x)[i]
    cat(ifelse(is.null(name), paste0("[[", i, "]]"), paste0("$", name)), ": ")
    if (inherits(elem, "db.vector")) {
      cat("db.vector preview (first ", preview_limits$rows, " rows):\n")
      print(elem)  # This will call print.db.vector
    } else if (inherits(elem, "db.matrix")) {
      cat("db.matrix preview (first ", preview_limits$rows, " rows, ", min(preview_limits$cols, ncol(elem)), " cols):\n")
      print(head(elem, preview_limits$rows))
    } else if (inherits(elem, "db.array")) {
      cat("db.array preview:\n")
      print(head(elem, preview_limits$rows))
    } else if (inherits(elem, "db.dataframe")) {
      cat("db.dataframe preview (first ", preview_limits$rows, " rows):\n")
      print(head(elem, preview_limits$rows))
    } else if (inherits(elem, "db.factor")) {
      cat("db.factor preview (first ", preview_limits$rows, " rows):\n")
      print(head(elem, preview_limits$rows))
    } else {
      print(elem)
    }
    if (i < length(x)) cat("\n")
  }
}

#' Structure display for db.list
#' @export
str.db.list <- function(object, ...) {
  cat("db.list of length ", length(object), "\n")

  # Calculate the maximum name length for proper alignment
  element_names <- names(object)
  if (is.null(element_names)) {
    element_names <- paste0("[[", seq_along(object), "]]")
  } else {
    # Handle cases where some names might be missing
    for (i in seq_along(element_names)) {
      if (is.na(element_names[i]) || element_names[i] == "") {
        element_names[i] <- paste0("[[", i, "]]")
      }
    }
  }

  # Find the maximum width needed for alignment
  max_width <- max(nchar(element_names))

  for (i in seq_along(object)) {
    elem <- object[[i]]
    name <- element_names[i]

    # Create properly aligned prefix
    if (grepl("^\\[\\[", name)) {
      # For numeric indices like [[1]]
      prefix <- paste0(" ", name)
      spaces_needed <- max_width - nchar(name) + 1
    } else {
      # For named elements like $name
      prefix <- paste0(" $ ", name)
      spaces_needed <- max_width - nchar(name)
    }

    # Add spaces to align the colons
    aligned_prefix <- paste0(prefix, paste(rep(" ", spaces_needed), collapse = ""), ": ")

    if (inherits(elem, "db.vector")) {
      # Get a small sample to determine data type
      sample_query <- paste0("SELECT ", elem$column, " FROM ", elem$table, " LIMIT 10")
      tryCatch({
        sample_data <- DBI::dbGetQuery(elem$con, sample_query)
        sample_vector <- sample_data[[elem$column]]

        # Get total count
        count_query <- paste0("SELECT COUNT(*) as n FROM ", elem$table)
        count_result <- DBI::dbGetQuery(elem$con, count_query)
        total_count <- count_result$n

        # Determine the R data type
        if (is.numeric(sample_vector)) {
          if (all(sample_vector == as.integer(sample_vector), na.rm = TRUE)) {
            data_type <- "int"
          } else {
            data_type <- "num"
          }
        } else if (is.character(sample_vector)) {
          data_type <- "chr"
        } else if (is.logical(sample_vector)) {
          data_type <- "logi"
        } else {
          data_type <- class(sample_vector)[1]
        }

        cat(aligned_prefix, "db.vector -> ", data_type, " [1:", total_count, "] ", sep = "")
        # Show first few values
        cat(paste(head(sample_vector, 6), collapse = " "), "...\n")

      }, error = function(e) {
        cat(aligned_prefix, "db.vector (", elem$column, " from ", elem$table, ")\n")
      })

    } else if (inherits(elem, "db.dataframe")) {
      cat(aligned_prefix, "db.dataframe with ", length(elem$columns), " columns\n")
    } else if (inherits(elem, "db.matrix")) {
      cat(aligned_prefix, "db.matrix\n")
    } else if (inherits(elem, "db.array")) {
      cat(aligned_prefix, "db.array\n")
    } else if (inherits(elem, "db.factor")) {
      cat(aligned_prefix, "db.factor\n")
    } else {
      cat(aligned_prefix)
      str(elem, give.attr = FALSE, ...)
    }
  }
}

#' Length of db.list
#' @export
length.db.list <- function(x) {
  base::length(unclass(x))  # Since it's a regular list underneath
}

#' Names of db.list
#' @export
names.db.list <- function(x) {
  base::names(unclass(x))
}

#' Set names for db.list
#' @export
`names<-.db.list` <- function(x, value) {
  x_unclassed <- unclass(x)
  base::names(x_unclassed) <- value
  structure(x_unclassed, class = "db.list")
}

#' Single-bracket subsetting for db.list (returns sublist)
#' @export
`[.db.list` <- function(x, i) {
  result <- base::`[`(unclass(x), i)
  structure(result, class = "db.list")
}

#' Double-bracket extraction for db.list (returns element)
#' @export
`[[.db.list` <- function(x, i) {
  base::`[[`(unclass(x), i)
}

#' Dollar extraction for db.list - FIXED VERSION
#' @export
`$.db.list` <- function(x, name) {
  # Convert to character to handle both string and name inputs
  name_char <- as.character(substitute(name))

  # Use the [[.db.list method to get the element by name
  x[[name_char]]
}

#' Assignment to db.list elements
#' @export
`[[<-.db.list` <- function(x, i, value) {
  # Handle NULL assignment (for removing elements)
  if (is.null(value)) {
    x_unclassed <- unclass(x)
    x_unclassed[[i]] <- NULL
    return(structure(x_unclassed, class = "db.list"))
  }

  # For non-NULL values, check they are db objects
  if (!inherits(value, c("db.vector", "db.matrix", "db.array", "db.dataframe", "db.factor"))) {
    stop("Can only assign db objects to db.list")
  }

  x_unclassed <- unclass(x)
  x_unclassed[[i]] <- value
  structure(x_unclassed, class = "db.list")
}

#' Dollar assignment for db.list - FIXED VERSION
#' @export
`$<-.db.list` <- function(x, name, value) {
  # Handle NULL assignment (for removing elements)
  if (is.null(value)) {
    # Convert to character to handle both string and name inputs
    name_char <- as.character(substitute(name))
    x_unclassed <- unclass(x)
    x_unclassed[[name_char]] <- NULL
    return(structure(x_unclassed, class = "db.list"))
  }

  # For non-NULL values, check they are db objects
  if (!inherits(value, c("db.vector", "db.matrix", "db.array", "db.dataframe", "db.factor"))) {
    stop("Can only assign db objects to db.list")
  }

  # Convert to character to handle both string and name inputs
  name_char <- as.character(substitute(name))

  x_unclassed <- unclass(x)
  x_unclassed[[name_char]] <- value
  structure(x_unclassed, class = "db.list")
}

#' Combine db.lists
#' @export
c.db.list <- function(...) {
  args <- list(...)
  result <- do.call(base::c, lapply(args, unclass))
  structure(result, class = "db.list")
}

#' Append to db.list
#' @export
append.db.list <- function(x, values, after = length(x)) {
  if (!is.list(values)) values <- list(values)
  result <- base::append(unclass(x), values, after)
  structure(result, class = "db.list")
}

#' Reverse db.list
#' @export
rev.db.list <- function(x) {
  result <- base::rev(unclass(x))
  structure(result, class = "db.list")
}

#' Enhanced db.vector creation with specific classes
#' This function should be used in your as.db.list methods instead of the basic structure() call

create_db_vector <- function(con, table, column, query) {
  # Get a sample to determine the data type
  sample_query <- paste0("SELECT ", column, " FROM ", table, " LIMIT 10")
  tryCatch({
    sample_data <- DBI::dbGetQuery(con, sample_query)
    sample_vector <- sample_data[[column]]

    # Determine the appropriate class based on data type
    if (is.numeric(sample_vector)) {
      if (all(sample_vector == as.integer(sample_vector), na.rm = TRUE)) {
        classes <- c("db.integer", "db.vector")
      } else {
        classes <- c("db.numeric", "db.vector")
      }
    } else if (is.character(sample_vector)) {
      classes <- c("db.character", "db.vector")
    } else if (is.logical(sample_vector)) {
      classes <- c("db.logical", "db.vector")
    } else {
      classes <- c("db.vector")  # fallback
    }

    structure(
      list(con = con, table = table, column = column, query = query),
      class = classes
    )

  }, error = function(e) {
    # If we can't determine type, use generic db.vector
    structure(
      list(con = con, table = table, column = column, query = query),
      class = "db.vector"
    )
  })
}

#' Enhanced class method for db vectors
#' @export
class.db.vector <- function(x) {
  current_classes <- attr(x, "class")

  # If we have a more specific class, return a more descriptive result
  if ("db.integer" %in% current_classes) {
    return("db.integer")
  } else if ("db.numeric" %in% current_classes) {
    return("db.numeric")
  } else if ("db.character" %in% current_classes) {
    return("db.character")
  } else if ("db.logical" %in% current_classes) {
    return("db.logical")
  } else {
    return("db.vector")
  }
}

#' ===========================================================================
#' HELPER FUNCTIONS FOR ARITHMETIC OPERATIONS
#' ===========================================================================

#' Get the length of a db.vector
get_vector_length <- function(db_vec) {
  count_query <- paste0("SELECT COUNT(*) as count FROM (", db_vec$query, ") subq")
  DBI::dbGetQuery(db_vec$con, count_query)$count
}

#' Get total elements in db.matrix
get_matrix_length <- function(db_mat) {
  db_mat$nrow * db_mat$ncol
}

#' Get total elements in db.array
get_array_length <- function(db_arr) {
  prod(db_arr$dim)
}

#' ===========================================================================
#' VECTOR ARITHMETIC AND COMPARISON OPERATIONS
#' ===========================================================================

#' Build arithmetic/comparison query for db.vector operations
build_vector_arithmetic_query <- function(e1, e2, operation) {

  # Determine SQL operator (handle comparison operators)
  get_sql_operator <- function(op, col1_expr, col2_expr) {
    switch(op,
           "^" = paste0("POWER(", col1_expr, ", ", col2_expr, ")"),
           "%%" = paste0("MOD(", col1_expr, ", ", col2_expr, ")"),
           "%/%" = paste0("CAST(FLOOR(", col1_expr, " / ", col2_expr, ") AS INTEGER)"),
           "==" = paste0("(", col1_expr, " = ", col2_expr, ")"),
           "!=" = paste0("(", col1_expr, " != ", col2_expr, ")"),
           "&" = paste0("(", col1_expr, " AND ", col2_expr, ")"),
           "|" = paste0("(", col1_expr, " OR ", col2_expr, ")"),
           paste0("(", col1_expr, " ", op, " ", col2_expr, ")")  # +, -, *, /, <, >, <=, >=
    )
  }

  # Case 1: Both are db.vectors
  if (inherits(e1, "db.vector") && inherits(e2, "db.vector")) {
    if (e1$table != e2$table) {
      stop("Operations between different tables not yet supported")
    }

    len1 <- get_vector_length(e1)
    len2 <- get_vector_length(e2)
    max_len <- max(len1, len2)

    # Extract column references
    col1 <- e1$column
    col2 <- e2$column

    # Build SQL expression based on operation
    sql_op <- get_sql_operator(operation, paste0("v1.", col1), paste0("v2.", col2))

    if (len1 == len2) {
      # Same length - simple join
      query <- paste0("
        SELECT ", sql_op, " as result
        FROM (SELECT *, ROW_NUMBER() OVER() as rn FROM (", e1$query, ") s1) v1
        JOIN (SELECT *, ROW_NUMBER() OVER() as rn FROM (", e2$query, ") s2) v2
          ON v1.rn = v2.rn
        ORDER BY v1.rn
      ")
    } else {
      # Different lengths - recycling needed
      if (max_len %% min(len1, len2) != 0) {
        warning("longer object length is not a multiple of shorter object length")
      }

      query <- paste0("
        WITH
        v1 AS (SELECT ", col1, ", ROW_NUMBER() OVER() as rn FROM (", e1$query, ") s1),
        v2 AS (SELECT ", col2, ", ROW_NUMBER() OVER() as rn FROM (", e2$query, ") s2),
        positions AS (
          SELECT generate_series AS pos FROM generate_series(1, ", max_len, ")
        )
        SELECT ", sql_op, " as result
        FROM positions p
        JOIN v1 ON v1.rn = ((p.pos - 1) % ", len1, " + 1)
        JOIN v2 ON v2.rn = ((p.pos - 1) % ", len2, " + 1)
        ORDER BY p.pos
      ")
    }

    return(list(query = query, con = e1$con, table = e1$table, column = "result"))
  }

  # Case 2: db.vector and R scalar/vector (e1 is db.vector)
  else if (inherits(e1, "db.vector") && (is.numeric(e2) || is.logical(e2) || is.character(e2))) {
    col1 <- e1$column

    if (length(e2) == 1) {
      # Scalar operation
      e2_val <- if (is.logical(e2)) {
        if (e2) "TRUE" else "FALSE"
      } else if (is.character(e2)) {
        # Properly escape single quotes in SQL strings
        paste0("'", gsub("'", "''", e2), "'")
      } else {
        e2
      }

      sql_op <- get_sql_operator(operation, col1, e2_val)
      query <- paste0("SELECT ", sql_op, " as result FROM (", e1$query, ") subq")
    } else {
      # Vector recycling
      len1 <- get_vector_length(e1)
      len2 <- length(e2)

      if (max(len1, len2) %% min(len1, len2) != 0) {
        warning("longer object length is not a multiple of shorter object length")
      }

      # Create values list for IN clause
      values_list <- if (is.logical(e2)) {
        paste(ifelse(e2, "TRUE", "FALSE"), collapse = ", ")
      } else if (is.character(e2)) {
        # Properly escape and quote character vectors
        paste(paste0("'", gsub("'", "''", e2), "'"), collapse = ", ")
      } else {
        paste(e2, collapse = ", ")
      }

      sql_op <- get_sql_operator(operation, paste0("v.", col1), "r.val")

      query <- paste0("
        WITH
        v AS (SELECT ", col1, ", ROW_NUMBER() OVER() as rn FROM (", e1$query, ") s),
        r AS (SELECT unnest([", values_list, "]) as val, generate_series as rn
              FROM generate_series(1, ", len2, "))
        SELECT ", sql_op, " as result
        FROM v
        JOIN r ON r.rn = ((v.rn - 1) % ", len2, " + 1)
        ORDER BY v.rn
      ")
    }

    return(list(query = query, con = e1$con, table = e1$table, column = "result"))
  }

  # Case 3: R scalar/vector and db.vector (e2 is db.vector)
  else if ((is.numeric(e1) || is.logical(e1) || is.character(e1)) && inherits(e2, "db.vector")) {
    col2 <- e2$column

    if (length(e1) == 1) {
      # Scalar operation
      e1_val <- if (is.logical(e1)) {
        if (e1) "TRUE" else "FALSE"
      } else if (is.character(e1)) {
        # Properly escape single quotes in SQL strings
        paste0("'", gsub("'", "''", e1), "'")
      } else {
        e1
      }

      sql_op <- get_sql_operator(operation, e1_val, col2)
      query <- paste0("SELECT ", sql_op, " as result FROM (", e2$query, ") subq")
    } else {
      # Vector recycling
      len1 <- length(e1)
      len2 <- get_vector_length(e2)

      if (max(len1, len2) %% min(len1, len2) != 0) {
        warning("longer object length is not a multiple of shorter object length")
      }

      values_list <- if (is.logical(e1)) {
        paste(ifelse(e1, "TRUE", "FALSE"), collapse = ", ")
      } else if (is.character(e1)) {
        # Properly escape and quote character vectors
        paste(paste0("'", gsub("'", "''", e1), "'"), collapse = ", ")
      } else {
        paste(e1, collapse = ", ")
      }

      sql_op <- get_sql_operator(operation, "r.val", paste0("v.", col2))

      query <- paste0("
        WITH
        v AS (SELECT ", col2, ", ROW_NUMBER() OVER() as rn FROM (", e2$query, ") s),
        r AS (SELECT unnest([", values_list, "]) as val, generate_series as rn
              FROM generate_series(1, ", len1, "))
        SELECT ", sql_op, " as result
        FROM v
        JOIN r ON r.rn = ((v.rn - 1) % ", len1, " + 1)
        ORDER BY v.rn
      ")
    }

    return(list(query = query, con = e2$con, table = e2$table, column = "result"))
  }

  stop("Unsupported operand types for operation")
}
#' ===========================================================================
#' MATRIX ARITHMETIC AND COMPARISON OPERATIONS
#' ===========================================================================

#' Build arithmetic/comparison query for db.matrix operations
build_matrix_arithmetic_query <- function(e1, e2, operation) {

  # Determine SQL operator
  get_sql_operator <- function(op, val1_expr, val2_expr) {
    switch(op,
           "^" = paste0("POWER(", val1_expr, ", ", val2_expr, ")"),
           "%%" = paste0("MOD(", val1_expr, ", ", val2_expr, ")"),
           "%/%" = paste0("CAST(FLOOR(", val1_expr, " / ", val2_expr, ") AS INTEGER)"),
           "==" = paste0("(", val1_expr, " = ", val2_expr, ")"),
           "!=" = paste0("(", val1_expr, " != ", val2_expr, ")"),
           "&" = paste0("(", val1_expr, " AND ", val2_expr, ")"),
           "|" = paste0("(", val1_expr, " OR ", val2_expr, ")"),
           paste0("(", val1_expr, " ", op, " ", val2_expr, ")")
    )
  }

  # Case 1: Both are db.matrix
  if (inherits(e1, "db.matrix") && inherits(e2, "db.matrix")) {
    if (e1$table != e2$table) {
      stop("Operations between different tables not yet supported")
    }

    # Check dimensions match
    if (e1$nrow != e2$nrow || e1$ncol != e2$ncol) {
      stop("non-conformable arrays")
    }

    # Build SQL expression
    sql_op <- get_sql_operator(operation, "m1.value", "m2.value")

    query <- paste0("
      SELECT ", sql_op, " as value
      FROM (SELECT value, ROW_NUMBER() OVER() as rn FROM (", e1$query, ") s1) m1
      JOIN (SELECT value, ROW_NUMBER() OVER() as rn FROM (", e2$query, ") s2) m2
        ON m1.rn = m2.rn
      ORDER BY m1.rn
    ")

    return(list(query = query, con = e1$con, table = e1$table,
                nrow = e1$nrow, ncol = e1$ncol))
  }

  # Case 2: db.vector and db.matrix
  else if (inherits(e1, "db.vector") && inherits(e2, "db.matrix")) {
    if (e1$table != e2$table) {
      stop("Operations between different tables not yet supported")
    }

    len_vec <- get_vector_length(e1)
    len_mat <- get_matrix_length(e2)

    # R's logic: Vector LONGER than matrix -> ERROR (always)
    if (len_vec > len_mat) {
      warning("longer object length is not a multiple of shorter object length")
      stop("dims [product ", len_mat, "] do not match the length of object [", len_vec, "]")
    } else if (len_vec < len_mat && len_mat %% len_vec != 0) {
      warning("longer object length is not a multiple of shorter object length")
    }

    col_vec <- e1$column
    sql_op <- get_sql_operator(operation, "v.val", "m.value")

    query <- paste0("
      WITH
      v AS (SELECT ", col_vec, " as val, ROW_NUMBER() OVER() as rn FROM (", e1$query, ") s1),
      m AS (SELECT value, ROW_NUMBER() OVER() as rn FROM (", e2$query, ") s2)
      SELECT ", sql_op, " as value
      FROM m
      LEFT JOIN v ON v.rn = ((m.rn - 1) % ", len_vec, " + 1)
      ORDER BY m.rn
    ")

    return(list(query = query, con = e2$con, table = e2$table,
                nrow = e2$nrow, ncol = e2$ncol))
  }

  # Case 3: db.matrix and db.vector
  else if (inherits(e1, "db.matrix") && inherits(e2, "db.vector")) {
    if (e1$table != e2$table) {
      stop("Operations between different tables not yet supported")
    }

    len_mat <- get_matrix_length(e1)
    len_vec <- get_vector_length(e2)

    if (len_vec > len_mat) {
      warning("longer object length is not a multiple of shorter object length")
      stop("dims [product ", len_mat, "] do not match the length of object [", len_vec, "]")
    } else if (len_vec < len_mat && len_mat %% len_vec != 0) {
      warning("longer object length is not a multiple of shorter object length")
    }

    col_vec <- e2$column
    sql_op <- get_sql_operator(operation, "m.value", "v.val")

    query <- paste0("
      WITH
      m AS (SELECT value, ROW_NUMBER() OVER() as rn FROM (", e1$query, ") s1),
      v AS (SELECT ", col_vec, " as val, ROW_NUMBER() OVER() as rn FROM (", e2$query, ") s2)
      SELECT ", sql_op, " as value
      FROM m
      LEFT JOIN v ON v.rn = ((m.rn - 1) % ", len_vec, " + 1)
      ORDER BY m.rn
    ")

    return(list(query = query, con = e1$con, table = e1$table,
                nrow = e1$nrow, ncol = e1$ncol))
  }

  # Case 4: db.matrix and scalar
  else if (inherits(e1, "db.matrix") && (is.numeric(e2) || is.logical(e2)) && length(e2) == 1) {
    e2_val <- if (is.logical(e2)) {
      if (e2) "TRUE" else "FALSE"
    } else {
      e2
    }

    sql_op <- get_sql_operator(operation, "value", e2_val)
    query <- paste0("SELECT ", sql_op, " as value FROM (", e1$query, ") subq")

    return(list(query = query, con = e1$con, table = e1$table,
                nrow = e1$nrow, ncol = e1$ncol))
  }

  # Case 5: scalar and db.matrix
  else if ((is.numeric(e1) || is.logical(e1)) && length(e1) == 1 && inherits(e2, "db.matrix")) {
    e1_val <- if (is.logical(e1)) {
      if (e1) "TRUE" else "FALSE"
    } else {
      e1
    }

    sql_op <- get_sql_operator(operation, e1_val, "value")
    query <- paste0("SELECT ", sql_op, " as value FROM (", e2$query, ") subq")

    return(list(query = query, con = e2$con, table = e2$table,
                nrow = e2$nrow, ncol = e2$ncol))
  }

  # Case 6: db.matrix and R vector (recycling)
  else if (inherits(e1, "db.matrix") && (is.numeric(e2) || is.logical(e2)) && length(e2) > 1) {
    len_mat <- get_matrix_length(e1)
    len_vec <- length(e2)

    if (len_mat %% len_vec != 0) {
      warning("longer object length is not a multiple of shorter object length")
    }

    values_list <- if (is.logical(e2)) {
      paste(ifelse(e2, "TRUE", "FALSE"), collapse = ", ")
    } else {
      paste(e2, collapse = ", ")
    }

    sql_op <- get_sql_operator(operation, "m.value", "r.val")

    query <- paste0("
      WITH
      m AS (SELECT value, ROW_NUMBER() OVER() as rn FROM (", e1$query, ") s),
      r AS (SELECT unnest([", values_list, "]) as val, generate_series as rn
            FROM generate_series(1, ", len_vec, "))
      SELECT ", sql_op, " as value
      FROM m
      JOIN r ON r.rn = ((m.rn - 1) % ", len_vec, " + 1)
      ORDER BY m.rn
    ")

    return(list(query = query, con = e1$con, table = e1$table,
                nrow = e1$nrow, ncol = e1$ncol))
  }

  # Case 7: R vector and db.matrix (recycling)
  else if ((is.numeric(e1) || is.logical(e1)) && length(e1) > 1 && inherits(e2, "db.matrix")) {
    len_vec <- length(e1)
    len_mat <- get_matrix_length(e2)

    if (len_mat %% len_vec != 0) {
      warning("longer object length is not a multiple of shorter object length")
    }

    values_list <- if (is.logical(e1)) {
      paste(ifelse(e1, "TRUE", "FALSE"), collapse = ", ")
    } else {
      paste(e1, collapse = ", ")
    }

    sql_op <- get_sql_operator(operation, "r.val", "m.value")

    query <- paste0("
      WITH
      m AS (SELECT value, ROW_NUMBER() OVER() as rn FROM (", e2$query, ") s),
      r AS (SELECT unnest([", values_list, "]) as val, generate_series as rn
            FROM generate_series(1, ", len_vec, "))
      SELECT ", sql_op, " as value
      FROM m
      JOIN r ON r.rn = ((m.rn - 1) % ", len_vec, " + 1)
      ORDER BY m.rn
    ")

    return(list(query = query, con = e2$con, table = e2$table,
                nrow = e2$nrow, ncol = e2$ncol))
  }

  stop("Unsupported operand types for matrix operation")
}

#' ===========================================================================
#' ARRAY ARITHMETIC AND COMPARISON OPERATIONS
#' ===========================================================================

#' Build arithmetic/comparison query for db.array operations
build_array_arithmetic_query <- function(e1, e2, operation) {

  # Determine SQL operator
  get_sql_operator <- function(op, val1_expr, val2_expr) {
    switch(op,
           "^" = paste0("POWER(", val1_expr, ", ", val2_expr, ")"),
           "%%" = paste0("MOD(", val1_expr, ", ", val2_expr, ")"),
           "%/%" = paste0("CAST(FLOOR(", val1_expr, " / ", val2_expr, ") AS INTEGER)"),
           "==" = paste0("(", val1_expr, " = ", val2_expr, ")"),
           "!=" = paste0("(", val1_expr, " != ", val2_expr, ")"),
           "&" = paste0("(", val1_expr, " AND ", val2_expr, ")"),
           "|" = paste0("(", val1_expr, " OR ", val2_expr, ")"),
           paste0("(", val1_expr, " ", op, " ", val2_expr, ")")
    )
  }

  # Case 1: Both are db.array
  if (inherits(e1, "db.array") && inherits(e2, "db.array")) {
    if (e1$table != e2$table) {
      stop("Operations between different tables not yet supported")
    }

    # Check dimensions match
    if (!identical(e1$dim, e2$dim)) {
      stop("non-conformable arrays")
    }

    sql_op <- get_sql_operator(operation, "a1.value", "a2.value")

    query <- paste0("
      SELECT ", sql_op, " as value
      FROM (SELECT value, ROW_NUMBER() OVER() as rn FROM (", e1$query, ") s1) a1
      JOIN (SELECT value, ROW_NUMBER() OVER() as rn FROM (", e2$query, ") s2) a2
        ON a1.rn = a2.rn
      ORDER BY a1.rn
    ")

    return(list(query = query, con = e1$con, table = e1$table, dim = e1$dim))
  }

  # Case 1a: db.matrix and db.array
  else if (inherits(e1, "db.matrix") && inherits(e2, "db.array")) {
    if (e1$table != e2$table) {
      stop("Operations between different tables not yet supported")
    }

    if (length(e2$dim) == 2) {
      if (e1$nrow != e2$dim[1] || e1$ncol != e2$dim[2]) {
        stop("non-conformable arrays")
      }

      sql_op <- get_sql_operator(operation, "m.value", "a.value")

      query <- paste0("
        SELECT ", sql_op, " as value
        FROM (SELECT value, ROW_NUMBER() OVER() as rn FROM (", e1$query, ") s1) m
        JOIN (SELECT value, ROW_NUMBER() OVER() as rn FROM (", e2$query, ") s2) a
          ON m.rn = a.rn
        ORDER BY m.rn
      ")

      return(list(query = query, con = e1$con, table = e1$table,
                  nrow = e1$nrow, ncol = e1$ncol))
    } else {
      stop("non-conformable arrays")
    }
  }

  # Case 1b: db.array and db.matrix
  else if (inherits(e1, "db.array") && inherits(e2, "db.matrix")) {
    if (e1$table != e2$table) {
      stop("Operations between different tables not yet supported")
    }

    if (length(e1$dim) == 2) {
      if (e1$dim[1] != e2$nrow || e1$dim[2] != e2$ncol) {
        stop("non-conformable arrays")
      }

      sql_op <- get_sql_operator(operation, "a.value", "m.value")

      query <- paste0("
        SELECT ", sql_op, " as value
        FROM (SELECT value, ROW_NUMBER() OVER() as rn FROM (", e1$query, ") s1) a
        JOIN (SELECT value, ROW_NUMBER() OVER() as rn FROM (", e2$query, ") s2) m
          ON a.rn = m.rn
        ORDER BY a.rn
      ")

      return(list(query = query, con = e2$con, table = e2$table,
                  nrow = e2$nrow, ncol = e2$ncol))
    } else {
      stop("non-conformable arrays")
    }
  }

  # Case 2: db.vector and db.array
  else if (inherits(e1, "db.vector") && inherits(e2, "db.array")) {
    if (e1$table != e2$table) {
      stop("Operations between different tables not yet supported")
    }

    len_vec <- get_vector_length(e1)
    len_arr <- get_array_length(e2)

    if (len_vec > len_arr && len_arr %% len_vec != 0) {
      warning("longer object length is not a multiple of shorter object length")
      stop("dims [product ", len_arr, "] do not match the length of object [", len_vec, "]")
    } else if (len_vec != len_arr && len_vec %% len_arr != 0 && len_arr %% len_vec != 0) {
      warning("longer object length is not a multiple of shorter object length")
    }

    col_vec <- e1$column
    sql_op <- get_sql_operator(operation, "v.val", "a.value")

    query <- paste0("
      WITH
      v AS (SELECT ", col_vec, " as val, ROW_NUMBER() OVER() as rn FROM (", e1$query, ") s1),
      a AS (SELECT value, ROW_NUMBER() OVER() as rn FROM (", e2$query, ") s2)
      SELECT ", sql_op, " as value
      FROM a
      LEFT JOIN v ON v.rn = ((a.rn - 1) % ", len_vec, " + 1)
      ORDER BY a.rn
    ")

    return(list(query = query, con = e2$con, table = e2$table, dim = e2$dim))
  }

  # Case 3: db.array and db.vector
  else if (inherits(e1, "db.array") && inherits(e2, "db.vector")) {
    if (e1$table != e2$table) {
      stop("Operations between different tables not yet supported")
    }

    len_arr <- get_array_length(e1)
    len_vec <- get_vector_length(e2)

    if (len_vec > len_arr && len_arr %% len_vec != 0) {
      warning("longer object length is not a multiple of shorter object length")
      stop("dims [product ", len_arr, "] do not match the length of object [", len_vec, "]")
    } else if (len_vec != len_arr && len_vec %% len_arr != 0 && len_arr %% len_vec != 0) {
      warning("longer object length is not a multiple of shorter object length")
    }

    col_vec <- e2$column
    sql_op <- get_sql_operator(operation, "a.value", "v.val")

    query <- paste0("
      WITH
      a AS (SELECT value, ROW_NUMBER() OVER() as rn FROM (", e1$query, ") s1),
      v AS (SELECT ", col_vec, " as val, ROW_NUMBER() OVER() as rn FROM (", e2$query, ") s2)
      SELECT ", sql_op, " as value
      FROM a
      LEFT JOIN v ON v.rn = ((a.rn - 1) % ", len_vec, " + 1)
      ORDER BY a.rn
    ")

    return(list(query = query, con = e1$con, table = e1$table, dim = e1$dim))
  }

  # Case 4: db.array and scalar
  else if (inherits(e1, "db.array") && (is.numeric(e2) || is.logical(e2)) && length(e2) == 1) {
    e2_val <- if (is.logical(e2)) {
      if (e2) "TRUE" else "FALSE"
    } else {
      e2
    }

    sql_op <- get_sql_operator(operation, "value", e2_val)
    query <- paste0("SELECT ", sql_op, " as value FROM (", e1$query, ") subq")

    return(list(query = query, con = e1$con, table = e1$table, dim = e1$dim))
  }

  # Case 5: scalar and db.array
  else if ((is.numeric(e1) || is.logical(e1)) && length(e1) == 1 && inherits(e2, "db.array")) {
    e1_val <- if (is.logical(e1)) {
      if (e1) "TRUE" else "FALSE"
    } else {
      e1
    }

    sql_op <- get_sql_operator(operation, e1_val, "value")
    query <- paste0("SELECT ", sql_op, " as value FROM (", e2$query, ") subq")

    return(list(query = query, con = e2$con, table = e2$table, dim = e2$dim))
  }

  # Case 6: db.array and R vector (recycling)
  else if (inherits(e1, "db.array") && (is.numeric(e2) || is.logical(e2)) && length(e2) > 1) {
    len_arr <- get_array_length(e1)
    len_vec <- length(e2)

    if (len_arr %% len_vec != 0) {
      warning("longer object length is not a multiple of shorter object length")
    }

    values_list <- if (is.logical(e2)) {
      paste(ifelse(e2, "TRUE", "FALSE"), collapse = ", ")
    } else {
      paste(e2, collapse = ", ")
    }

    sql_op <- get_sql_operator(operation, "a.value", "r.val")

    query <- paste0("
      WITH
      a AS (SELECT value, ROW_NUMBER() OVER() as rn FROM (", e1$query, ") s),
      r AS (SELECT unnest([", values_list, "]) as val, generate_series as rn
            FROM generate_series(1, ", len_vec, "))
      SELECT ", sql_op, " as value
      FROM a
      JOIN r ON r.rn = ((a.rn - 1) % ", len_vec, " + 1)
      ORDER BY a.rn
    ")

    return(list(query = query, con = e1$con, table = e1$table, dim = e1$dim))
  }

  # Case 7: R vector and db.array (recycling)
  else if ((is.numeric(e1) || is.logical(e1)) && length(e1) > 1 && inherits(e2, "db.array")) {
    len_vec <- length(e1)
    len_arr <- get_array_length(e2)

    if (len_arr %% len_vec != 0) {
      warning("longer object length is not a multiple of shorter object length")
    }

    values_list <- if (is.logical(e1)) {
      paste(ifelse(e1, "TRUE", "FALSE"), collapse = ", ")
    } else {
      paste(e1, collapse = ", ")
    }

    sql_op <- get_sql_operator(operation, "r.val", "a.value")

    query <- paste0("
      WITH
      a AS (SELECT value, ROW_NUMBER() OVER() as rn FROM (", e2$query, ") s),
      r AS (SELECT unnest([", values_list, "]) as val, generate_series as rn
            FROM generate_series(1, ", len_vec, "))
      SELECT ", sql_op, " as value
      FROM a
      JOIN r ON r.rn = ((a.rn - 1) % ", len_vec, " + 1)
      ORDER BY a.rn
    ")

    return(list(query = query, con = e2$con, table = e2$table, dim = e2$dim))
  }

  stop("non-conformable arrays")
}

#' ===========================================================================
#' UNIFIED OPS GROUP GENERIC FOR ARITHMETIC AND COMPARISON
#' ===========================================================================

#' @export
Ops.db.vector <- function(e1, e2) {
  # Handle unary operations
  if (missing(e2)) {
    if (.Generic == "-") {
      # Unary minus
      if (inherits(e1, "db.array")) {
        query <- paste0("SELECT -value as value FROM (", e1$query, ") subq")
        return(structure(list(query = query, con = e1$con, table = e1$table, dim = e1$dim),
                         class = "db.array"))
      } else if (inherits(e1, "db.matrix")) {
        query <- paste0("SELECT -value as value FROM (", e1$query, ") subq")
        return(structure(list(query = query, con = e1$con, table = e1$table,
                              nrow = e1$nrow, ncol = e1$ncol), class = "db.matrix"))
      } else {
        query <- paste0("SELECT -", e1$column, " as result FROM (", e1$query, ") subq")
        return(structure(list(query = query, con = e1$con, table = e1$table, column = "result"),
                         class = "db.vector"))
      }
    } else if (.Generic == "+") {
      return(e1)
    } else if (.Generic == "!") {
      # Logical NOT
      if (inherits(e1, "db.array")) {
        query <- paste0("SELECT (NOT value) as value FROM (", e1$query, ") subq")
        return(structure(list(query = query, con = e1$con, table = e1$table, dim = e1$dim),
                         class = "db.array"))
      } else if (inherits(e1, "db.matrix")) {
        query <- paste0("SELECT (NOT value) as value FROM (", e1$query, ") subq")
        return(structure(list(query = query, con = e1$con, table = e1$table,
                              nrow = e1$nrow, ncol = e1$ncol), class = "db.matrix"))
      } else {
        query <- paste0("SELECT (NOT ", e1$column, ") as result FROM (", e1$query, ") subq")
        return(structure(list(query = query, con = e1$con, table = e1$table, column = "result"),
                         class = "db.vector"))
      }
    } else {
      stop("Unary operator '", .Generic, "' not supported")
    }
  }

  # Binary operations - route to appropriate handler
  if (.Generic %in% c("+", "-", "*", "/", "^", "%%", "%/%",
                      ">", "<", ">=", "<=", "==", "!=", "&", "|")) {
    # Determine object types
    is_arr1 <- inherits(e1, "db.array")
    is_arr2 <- inherits(e2, "db.array")
    is_mat1 <- inherits(e1, "db.matrix")
    is_mat2 <- inherits(e2, "db.matrix")
    is_vec1 <- inherits(e1, "db.vector")
    is_vec2 <- inherits(e2, "db.vector")

    # Route to appropriate handler
    if (is_arr1 || is_arr2) {
      result <- build_array_arithmetic_query(e1, e2, .Generic)
      # Return as matrix if result has matrix structure
      if (!is.null(result$nrow) && !is.null(result$ncol)) {
        return(structure(result, class = "db.matrix"))
      }
      return(structure(result, class = "db.array"))
    } else if (is_mat1 || is_mat2) {
      result <- build_matrix_arithmetic_query(e1, e2, .Generic)
      return(structure(result, class = "db.matrix"))
    } else if (is_vec1 || is_vec2) {
      result <- build_vector_arithmetic_query(e1, e2, .Generic)
      return(structure(result, class = "db.vector"))
    }
  }

  stop("Operation '", .Generic, "' not supported")
}

#' @export
Ops.db.matrix <- Ops.db.vector

#' @export
Ops.db.array <- Ops.db.vector

#' ===========================================================================
#' LOGICAL NOT OPERATOR
#' ===========================================================================

#' @export
"!.db.vector" <- function(x) {
  if (inherits(x, "db.array")) {
    query <- paste0("SELECT (NOT value) as value FROM (", x$query, ") subq")
    result <- list(query = query, con = x$con, table = x$table, dim = x$dim)
    return(structure(result, class = "db.array"))
  } else if (inherits(x, "db.matrix")) {
    query <- paste0("SELECT (NOT value) as value FROM (", x$query, ") subq")
    result <- list(query = query, con = x$con, table = x$table,
                   nrow = x$nrow, ncol = x$ncol)
    return(structure(result, class = "db.matrix"))
  } else {
    query <- paste0("SELECT (NOT ", x$column, ") as result FROM (", x$query, ") subq")
    result <- list(query = query, con = x$con, table = x$table, column = "result")
    return(structure(result, class = "db.vector"))
  }
}

#' @export
"!.db.matrix" <- "!.db.vector"

#' @export
"!.db.array" <- "!.db.vector"

#' ===========================================================================
#' DESCRIPTIVE STATISTICS FOR DATABASE OBJECTS
#' ===========================================================================

#' Override base R functions to support db objects
#' These functions check for db objects first before calling base R versions

# Store original base R functions
.base_sd <- stats::sd
.base_var <- stats::var
.base_IQR <- stats::IQR
.base_mad <- stats::mad

#' @export
sd <- function(x, na.rm = FALSE) {
  if (inherits(x, c("db.vector", "db.matrix", "db.array"))) {
    UseMethod("sd")
  } else {
    .base_sd(x, na.rm = na.rm)
  }
}

#' @export
var <- function(x, y = NULL, na.rm = FALSE, use) {
  if (inherits(x, c("db.vector", "db.matrix", "db.array"))) {
    UseMethod("var")
  } else {
    if (missing(use)) {
      .base_var(x, y = y, na.rm = na.rm)
    } else {
      .base_var(x, y = y, na.rm = na.rm, use = use)
    }
  }
}

#' @export
IQR <- function(x, na.rm = FALSE, type = 7) {
  if (inherits(x, c("db.vector", "db.matrix", "db.array"))) {
    UseMethod("IQR")
  } else {
    .base_IQR(x, na.rm = na.rm, type = type)
  }
}

#' @export
mad <- function(x, center = median(x), constant = 1.4826, na.rm = FALSE,
                low = FALSE, high = FALSE) {
  if (inherits(x, c("db.vector", "db.matrix", "db.array"))) {
    UseMethod("mad")
  } else {
    .base_mad(x, center = center, constant = constant, na.rm = na.rm,
              low = low, high = high)
  }
}

#' Generic sum function for database objects
#' @export
sum.db.vector <- function(x, na.rm = FALSE, ...) {
  na_clause <- if (na.rm) paste0(" WHERE ", x$column, " IS NOT NULL") else ""
  query <- paste0("SELECT SUM(", x$column, ") as result FROM (", x$query, ") subq", na_clause)
  result <- DBI::dbGetQuery(x$con, query)$result
  if (is.na(result)) return(NA_real_)
  return(result)
}

#' @export
sum.db.matrix <- function(x, na.rm = FALSE, ...) {
  na_clause <- if (na.rm) " WHERE value IS NOT NULL" else ""
  query <- paste0("SELECT SUM(value) as result FROM (", x$query, ") subq", na_clause)
  result <- DBI::dbGetQuery(x$con, query)$result
  if (is.na(result)) return(NA_real_)
  return(result)
}

#' @export
sum.db.array <- sum.db.matrix

#' Generic mean function for database objects
#' @export
mean.db.vector <- function(x, na.rm = FALSE, ...) {
  na_clause <- if (na.rm) paste0(" WHERE ", x$column, " IS NOT NULL") else ""
  query <- paste0("SELECT AVG(", x$column, ") as result FROM (", x$query, ") subq", na_clause)
  result <- DBI::dbGetQuery(x$con, query)$result
  if (is.na(result)) return(NA_real_)
  return(result)
}

#' @export
mean.db.matrix <- function(x, na.rm = FALSE, ...) {
  na_clause <- if (na.rm) " WHERE value IS NOT NULL" else ""
  query <- paste0("SELECT AVG(value) as result FROM (", x$query, ") subq", na_clause)
  result <- DBI::dbGetQuery(x$con, query)$result
  if (is.na(result)) return(NA_real_)
  return(result)
}

#' @export
mean.db.array <- mean.db.matrix

#' Generic median function for database objects
#' @export
median.db.vector <- function(x, na.rm = FALSE, ...) {
  na_clause <- if (na.rm) paste0(" WHERE ", x$column, " IS NOT NULL") else ""
  query <- paste0("SELECT MEDIAN(", x$column, ") as result FROM (", x$query, ") subq", na_clause)
  result <- DBI::dbGetQuery(x$con, query)$result
  if (is.na(result)) return(NA_real_)
  return(result)
}

#' @export
median.db.matrix <- function(x, na.rm = FALSE, ...) {
  na_clause <- if (na.rm) " WHERE value IS NOT NULL" else ""
  query <- paste0("SELECT MEDIAN(value) as result FROM (", x$query, ") subq", na_clause)
  result <- DBI::dbGetQuery(x$con, query)$result
  if (is.na(result)) return(NA_real_)
  return(result)
}

#' @export
median.db.array <- median.db.matrix

#' Statistical mode function for database objects (most frequent value)
#' Note: Use stat_mode() since base R's mode() returns object type
#' @export
stat_mode <- function(x, na.rm = FALSE) {
  UseMethod("stat_mode")
}

#' @export
stat_mode.default <- function(x, na.rm = FALSE) {
  if (na.rm) x <- x[!is.na(x)]
  ux <- unique(x)
  tab <- tabulate(match(x, ux))
  ux[which.max(tab)]
}

#' @export
stat_mode.db.vector <- function(x, na.rm = FALSE) {
  na_clause <- if (na.rm) paste0(" WHERE ", x$column, " IS NOT NULL") else ""
  query <- paste0(
    "SELECT ", x$column, " as result FROM (", x$query, ") subq", na_clause,
    " GROUP BY ", x$column,
    " ORDER BY COUNT(*) DESC, ", x$column, " LIMIT 1"
  )
  result <- DBI::dbGetQuery(x$con, query)
  if (nrow(result) == 0) return(NA)
  return(result$result[1])
}

#' @export
stat_mode.db.matrix <- function(x, na.rm = FALSE) {
  na_clause <- if (na.rm) " WHERE value IS NOT NULL" else ""
  query <- paste0(
    "SELECT value as result FROM (", x$query, ") subq", na_clause,
    " GROUP BY value ORDER BY COUNT(*) DESC, value LIMIT 1"
  )
  result <- DBI::dbGetQuery(x$con, query)
  if (nrow(result) == 0) return(NA)
  return(result$result[1])
}

#' @export
stat_mode.db.array <- stat_mode.db.matrix

#' Generic min function for database objects
#' @export
min.db.vector <- function(x, na.rm = FALSE, ...) {
  na_clause <- if (na.rm) paste0(" WHERE ", x$column, " IS NOT NULL") else ""
  query <- paste0("SELECT MIN(", x$column, ") as result FROM (", x$query, ") subq", na_clause)
  result <- DBI::dbGetQuery(x$con, query)$result
  if (is.na(result) && !na.rm) return(NA_real_)
  if (is.na(result) && na.rm) return(Inf)
  return(result)
}

#' @export
min.db.matrix <- function(x, na.rm = FALSE, ...) {
  na_clause <- if (na.rm) " WHERE value IS NOT NULL" else ""
  query <- paste0("SELECT MIN(value) as result FROM (", x$query, ") subq", na_clause)
  result <- DBI::dbGetQuery(x$con, query)$result
  if (is.na(result) && !na.rm) return(NA_real_)
  if (is.na(result) && na.rm) return(Inf)
  return(result)
}

#' @export
min.db.array <- min.db.matrix

#' Generic max function for database objects
#' @export
max.db.vector <- function(x, na.rm = FALSE, ...) {
  na_clause <- if (na.rm) paste0(" WHERE ", x$column, " IS NOT NULL") else ""
  query <- paste0("SELECT MAX(", x$column, ") as result FROM (", x$query, ") subq", na_clause)
  result <- DBI::dbGetQuery(x$con, query)$result
  if (is.na(result) && !na.rm) return(NA_real_)
  if (is.na(result) && na.rm) return(-Inf)
  return(result)
}

#' @export
max.db.matrix <- function(x, na.rm = FALSE, ...) {
  na_clause <- if (na.rm) " WHERE value IS NOT NULL" else ""
  query <- paste0("SELECT MAX(value) as result FROM (", x$query, ") subq", na_clause)
  result <- DBI::dbGetQuery(x$con, query)$result
  if (is.na(result) && !na.rm) return(NA_real_)
  if (is.na(result) && na.rm) return(-Inf)
  return(result)
}

#' @export
max.db.array <- max.db.matrix

#' Range function for database objects
#' @export
range.db.vector <- function(x, na.rm = FALSE, ...) {
  c(min(x, na.rm = na.rm), max(x, na.rm = na.rm))
}

#' @export
range.db.matrix <- range.db.vector

#' @export
range.db.array <- range.db.vector

#' Standard deviation function for database objects
#' @export
sd.db.vector <- function(x, na.rm = FALSE, ...) {
  na_clause <- if (na.rm) paste0(" WHERE ", x$column, " IS NOT NULL") else ""
  query <- paste0("SELECT STDDEV_SAMP(", x$column, ") as result FROM (", x$query, ") subq", na_clause)
  result <- DBI::dbGetQuery(x$con, query)$result
  if (is.na(result)) return(NA_real_)
  return(result)
}

#' @export
sd.db.matrix <- function(x, na.rm = FALSE, ...) {
  na_clause <- if (na.rm) " WHERE value IS NOT NULL" else ""
  query <- paste0("SELECT STDDEV_SAMP(value) as result FROM (", x$query, ") subq", na_clause)
  result <- DBI::dbGetQuery(x$con, query)$result
  if (is.na(result)) return(NA_real_)
  return(result)
}

#' @export
sd.db.array <- sd.db.matrix

#' Variance function for database objects
#' @export
var.db.vector <- function(x, y = NULL, na.rm = FALSE, use, ...) {
  if (!is.null(y)) {
    stop("Covariance between two variables not yet supported for db objects")
  }
  na_clause <- if (na.rm) paste0(" WHERE ", x$column, " IS NOT NULL") else ""
  query <- paste0("SELECT VAR_SAMP(", x$column, ") as result FROM (", x$query, ") subq", na_clause)
  result <- DBI::dbGetQuery(x$con, query)$result
  if (is.na(result)) return(NA_real_)
  return(result)
}

#' @export
var.db.matrix <- function(x, y = NULL, na.rm = FALSE, use, ...) {
  if (!is.null(y)) {
    stop("Covariance between two variables not yet supported for db objects")
  }

  # Match base R behavior: return covariance matrix between columns
  ncol <- x$ncol
  nrow <- x$nrow

  # Build query to compute covariance matrix
  # Strategy: Reshape matrix into columns, then compute covariances

  # Create a query that extracts each column as a separate column in a table
  col_queries <- lapply(1:ncol, function(col_idx) {
    # Calculate positions for this column (column-major order)
    positions <- seq(from = (col_idx - 1) * nrow + 1, to = col_idx * nrow)
    positions_str <- paste(positions, collapse = ", ")

    paste0(
      "(SELECT value as col", col_idx, ", ROW_NUMBER() OVER() as row_num ",
      "FROM (SELECT value, ROW_NUMBER() OVER() as rn FROM (", x$query, ") numbered_subq) positioned ",
      "WHERE rn IN (", positions_str, ") ORDER BY rn)"
    )
  })

  # Join all columns together
  from_clause <- col_queries[[1]]
  if (ncol > 1) {
    for (i in 2:ncol) {
      from_clause <- paste0(from_clause, " JOIN ", col_queries[[i]], " USING (row_num)")
    }
  }

  # Build covariance calculations
  cov_matrix <- matrix(NA_real_, nrow = ncol, ncol = ncol)

  for (i in 1:ncol) {
    for (j in i:ncol) {
      # Compute covariance between column i and column j
      if (i == j) {
        # Variance (diagonal)
        query <- paste0(
          "SELECT VAR_SAMP(col", i, ") as result FROM ", from_clause
        )
      } else {
        # Covariance (off-diagonal)
        query <- paste0(
          "SELECT COVAR_SAMP(col", i, ", col", j, ") as result FROM ", from_clause
        )
      }

      result <- DBI::dbGetQuery(x$con, query)$result
      cov_matrix[i, j] <- result
      cov_matrix[j, i] <- result  # Symmetric
    }
  }

  return(cov_matrix)
}

#' @export
var.db.array <- function(x, y = NULL, na.rm = FALSE, use, ...) {
  if (!is.null(y)) {
    stop("Covariance between two variables not yet supported for db objects")
  }

  # Check if this is a 2D array (behave like matrix)
  if (length(x$dim) == 2) {
    # Treat as matrix - return covariance matrix
    ncol <- x$dim[2]
    nrow <- x$dim[1]

    # Build query to compute covariance matrix
    # Create a query that extracts each column as a separate column in a table
    col_queries <- lapply(1:ncol, function(col_idx) {
      # Calculate positions for this column (column-major order)
      positions <- seq(from = (col_idx - 1) * nrow + 1, to = col_idx * nrow)
      positions_str <- paste(positions, collapse = ", ")

      paste0(
        "(SELECT value as col", col_idx, ", ROW_NUMBER() OVER() as row_num ",
        "FROM (SELECT value, ROW_NUMBER() OVER() as rn FROM (", x$query, ") numbered_subq) positioned ",
        "WHERE rn IN (", positions_str, ") ORDER BY rn)"
      )
    })

    # Join all columns together
    from_clause <- col_queries[[1]]
    if (ncol > 1) {
      for (i in 2:ncol) {
        from_clause <- paste0(from_clause, " JOIN ", col_queries[[i]], " USING (row_num)")
      }
    }

    # Build covariance calculations
    cov_matrix <- matrix(NA_real_, nrow = ncol, ncol = ncol)

    for (i in 1:ncol) {
      for (j in i:ncol) {
        # Compute covariance between column i and column j
        if (i == j) {
          # Variance (diagonal)
          query <- paste0(
            "SELECT VAR_SAMP(col", i, ") as result FROM ", from_clause
          )
        } else {
          # Covariance (off-diagonal)
          query <- paste0(
            "SELECT COVAR_SAMP(col", i, ", col", j, ") as result FROM ", from_clause
          )
        }

        result <- DBI::dbGetQuery(x$con, query)$result
        cov_matrix[i, j] <- result
        cov_matrix[j, i] <- result  # Symmetric
      }
    }

    return(cov_matrix)
  } else {
    # For 1D or 3D+ arrays, compute variance of all elements (like base R)
    na_clause <- if (na.rm) " WHERE value IS NOT NULL" else ""
    query <- paste0("SELECT VAR_SAMP(value) as result FROM (", x$query, ") subq", na_clause)
    result <- DBI::dbGetQuery(x$con, query)$result
    if (is.na(result)) return(NA_real_)
    return(result)
  }
}

#' Quantile function for database objects
#' @export
quantile.db.vector <- function(x, probs = seq(0, 1, 0.25), na.rm = FALSE, ...) {
  if (!is.numeric(probs) || any(probs < 0) || any(probs > 1)) {
    stop("'probs' must be numeric values between 0 and 1")
  }

  na_clause <- if (na.rm) paste0(" WHERE ", x$column, " IS NOT NULL") else ""

  # Build multiple quantile calculations
  quantile_exprs <- paste0(
    "QUANTILE_CONT(", x$column, ", ", probs, ")"
  )

  query <- paste0(
    "SELECT ", paste(quantile_exprs, collapse = ", "),
    " FROM (", x$query, ") subq", na_clause
  )

  result <- DBI::dbGetQuery(x$con, query)
  result_vec <- as.numeric(result[1, ])
  names(result_vec) <- paste0(probs * 100, "%")
  return(result_vec)
}

#' @export
quantile.db.matrix <- function(x, probs = seq(0, 1, 0.25), na.rm = FALSE, ...) {
  if (!is.numeric(probs) || any(probs < 0) || any(probs > 1)) {
    stop("'probs' must be numeric values between 0 and 1")
  }

  na_clause <- if (na.rm) " WHERE value IS NOT NULL" else ""

  quantile_exprs <- paste0("QUANTILE_CONT(value, ", probs, ")")

  query <- paste0(
    "SELECT ", paste(quantile_exprs, collapse = ", "),
    " FROM (", x$query, ") subq", na_clause
  )

  result <- DBI::dbGetQuery(x$con, query)
  result_vec <- as.numeric(result[1, ])
  names(result_vec) <- paste0(probs * 100, "%")
  return(result_vec)
}

#' @export
quantile.db.array <- quantile.db.matrix

#' IQR (Interquartile Range) function for database objects
#' @export
IQR.db.vector <- function(x, na.rm = FALSE, type = 7, ...) {
  q <- quantile.db.vector(x, probs = c(0.25, 0.75), na.rm = na.rm)
  return(unname(q[2] - q[1]))
}

#' @export
IQR.db.matrix <- function(x, na.rm = FALSE, type = 7, ...) {
  q <- quantile.db.matrix(x, probs = c(0.25, 0.75), na.rm = na.rm)
  return(unname(q[2] - q[1]))
}

#' @export
IQR.db.array <- IQR.db.matrix

#' MAD (Mean Absolute Deviation) function for database objects
#' @export
mad.db.vector <- function(x, center = NULL, constant = 1.4826, na.rm = FALSE, low = FALSE, high = FALSE, ...) {
  # Calculate center if not provided
  if (is.null(center)) {
    center <- median.db.vector(x, na.rm = na.rm)
  }

  na_clause <- if (na.rm) paste0(" WHERE ", x$column, " IS NOT NULL") else ""

  # Calculate median of absolute deviations
  query <- paste0(
    "SELECT MEDIAN(ABS(", x$column, " - ", center, ")) * ", constant, " as result ",
    "FROM (", x$query, ") subq", na_clause
  )

  result <- DBI::dbGetQuery(x$con, query)$result
  if (is.na(result)) return(NA_real_)
  return(result)
}

#' @export
mad.db.matrix <- function(x, center = NULL, constant = 1.4826, na.rm = FALSE, low = FALSE, high = FALSE, ...) {
  # Calculate center if not provided
  if (is.null(center)) {
    center <- median.db.matrix(x, na.rm = na.rm)
  }

  na_clause <- if (na.rm) " WHERE value IS NOT NULL" else ""

  query <- paste0(
    "SELECT MEDIAN(ABS(value - ", center, ")) * ", constant, " as result ",
    "FROM (", x$query, ") subq", na_clause
  )

  result <- DBI::dbGetQuery(x$con, query)$result
  if (is.na(result)) return(NA_real_)
  return(result)
}

#' @export
mad.db.array <- mad.db.matrix

#' ===========================================================================
#' SUMMARY FUNCTION
#' ===========================================================================

#' Summary statistics for database objects
#' @export
summary.db.vector <- function(object, ...) {
  # Compute all statistics in one query for efficiency
  query <- paste0(
    "SELECT ",
    "MIN(", object$column, ") as min_val, ",
    "QUANTILE_CONT(", object$column, ", 0.25) as q1, ",
    "MEDIAN(", object$column, ") as median_val, ",
    "AVG(", object$column, ") as mean_val, ",
    "QUANTILE_CONT(", object$column, ", 0.75) as q3, ",
    "MAX(", object$column, ") as max_val ",
    "FROM (", object$query, ") subq"
  )

  result <- DBI::dbGetQuery(object$con, query)

  # Format as named vector like base R
  summary_vec <- c(
    result$min_val,
    result$q1,
    result$median_val,
    result$mean_val,
    result$q3,
    result$max_val
  )
  names(summary_vec) <- c("Min.", "1st Qu.", "Median", "Mean", "3rd Qu.", "Max.")

  class(summary_vec) <- c("summaryDefault", "table")
  return(summary_vec)
}

#' @export
summary.db.matrix <- function(object, ...) {
  ncol <- object$ncol
  nrow <- object$nrow

  # Create a numeric matrix to store values
  values_matrix <- matrix(NA_real_, nrow = 6, ncol = ncol)

  stat_labels <- c("Min.", "1st Qu.", "Median", "Mean", "3rd Qu.", "Max.")

  for (col_idx in 1:ncol) {
    # Calculate positions for this column (column-major order)
    positions <- seq(from = (col_idx - 1) * nrow + 1, to = col_idx * nrow)
    positions_str <- paste(positions, collapse = ", ")

    # Query for this column's statistics
    query <- paste0(
      "SELECT ",
      "MIN(value) as min_val, ",
      "QUANTILE_CONT(value, 0.25) as q1, ",
      "MEDIAN(value) as median_val, ",
      "AVG(value) as mean_val, ",
      "QUANTILE_CONT(value, 0.75) as q3, ",
      "MAX(value) as max_val ",
      "FROM (",
      "  SELECT value FROM (",
      "    SELECT value, ROW_NUMBER() OVER() as rn FROM (", object$query, ") numbered_subq",
      "  ) positioned WHERE rn IN (", positions_str, ")",
      ") col_data"
    )

    result <- DBI::dbGetQuery(object$con, query)

    # Store values
    values_matrix[, col_idx] <- c(
      result$min_val,
      result$q1,
      result$median_val,
      result$mean_val,
      result$q3,
      result$max_val
    )
  }

  # Now format with proper alignment
  summary_matrix <- matrix("", nrow = 6, ncol = ncol)
  colnames(summary_matrix) <- paste0("V", 1:ncol)

  # Find the longest label for alignment
  max_label_width <- max(nchar(stat_labels))

  # Format each column with consistent decimals
  for (col_idx in 1:ncol) {
    col_values <- values_matrix[, col_idx]

    # Determine if we need decimals (if any value is non-integer)
    needs_decimals <- any(col_values != floor(col_values))

    if (needs_decimals) {
      # Format with 1 decimal place (matching base R)
      formatted_values <- sprintf("%.1f", col_values)
    } else {
      # Format as integers
      formatted_values <- sprintf("%.0f", col_values)
    }

    # Create formatted strings with aligned colons
    for (i in 1:6) {
      # Pad label to max width and add colon
      padded_label <- sprintf(paste0("%-", max_label_width, "s"), stat_labels[i])
      summary_matrix[i, col_idx] <- paste0(" ", padded_label, ":", formatted_values[i])
    }
  }

  # Return as table class for proper printing
  class(summary_matrix) <- c("table")
  return(summary_matrix)
}

#' @export
summary.db.array <- function(object, ...) {
  # Check if this is a 2D array (behave like matrix)
  if (length(object$dim) == 2) {
    ncol <- object$dim[2]
    nrow <- object$dim[1]

    # Create a numeric matrix to store values
    values_matrix <- matrix(NA_real_, nrow = 6, ncol = ncol)

    stat_labels <- c("Min.", "1st Qu.", "Median", "Mean", "3rd Qu.", "Max.")

    for (col_idx in 1:ncol) {
      # Calculate positions for this column (column-major order)
      positions <- seq(from = (col_idx - 1) * nrow + 1, to = col_idx * nrow)
      positions_str <- paste(positions, collapse = ", ")

      # Query for this column's statistics
      query <- paste0(
        "SELECT ",
        "MIN(value) as min_val, ",
        "QUANTILE_CONT(value, 0.25) as q1, ",
        "MEDIAN(value) as median_val, ",
        "AVG(value) as mean_val, ",
        "QUANTILE_CONT(value, 0.75) as q3, ",
        "MAX(value) as max_val ",
        "FROM (",
        "  SELECT value FROM (",
        "    SELECT value, ROW_NUMBER() OVER() as rn FROM (", object$query, ") numbered_subq",
        "  ) positioned WHERE rn IN (", positions_str, ")",
        ") col_data"
      )

      result <- DBI::dbGetQuery(object$con, query)

      # Store values
      values_matrix[, col_idx] <- c(
        result$min_val,
        result$q1,
        result$median_val,
        result$mean_val,
        result$q3,
        result$max_val
      )
    }

    # Now format with proper alignment
    summary_matrix <- matrix("", nrow = 6, ncol = ncol)
    colnames(summary_matrix) <- paste0("V", 1:ncol)

    # Find the longest label for alignment
    max_label_width <- max(nchar(stat_labels))

    # Format each column with consistent decimals
    for (col_idx in 1:ncol) {
      col_values <- values_matrix[, col_idx]

      # Determine if we need decimals (if any value is non-integer)
      needs_decimals <- any(col_values != floor(col_values))

      if (needs_decimals) {
        # Format with 1 decimal place (matching base R)
        formatted_values <- sprintf("%.1f", col_values)
      } else {
        # Format as integers
        formatted_values <- sprintf("%.0f", col_values)
      }

      # Create formatted strings with aligned colons
      for (i in 1:6) {
        # Pad label to max width and add colon
        padded_label <- sprintf(paste0("%-", max_label_width, "s"), stat_labels[i])
        summary_matrix[i, col_idx] <- paste0(" ", padded_label, ":", formatted_values[i])
      }
    }

    # Return as table class
    class(summary_matrix) <- c("table")
    return(summary_matrix)
  } else {
    # For 1D or 3D+ arrays, compute summary of all elements
    query <- paste0(
      "SELECT ",
      "MIN(value) as min_val, ",
      "QUANTILE_CONT(value, 0.25) as q1, ",
      "MEDIAN(value) as median_val, ",
      "AVG(value) as mean_val, ",
      "QUANTILE_CONT(value, 0.75) as q3, ",
      "MAX(value) as max_val ",
      "FROM (", object$query, ") subq"
    )

    result <- DBI::dbGetQuery(object$con, query)

    # Format as named vector like base R
    summary_vec <- c(
      result$min_val,
      result$q1,
      result$median_val,
      result$mean_val,
      result$q3,
      result$max_val
    )
    names(summary_vec) <- c("Min.", "1st Qu.", "Median", "Mean", "3rd Qu.", "Max.")

    class(summary_vec) <- c("summaryDefault", "table")
    return(summary_vec)
  }
}
