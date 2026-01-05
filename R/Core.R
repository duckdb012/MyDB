library(R6)
library(DBI)
library(duckdb)

#' ===========================================================================
#' GLOBAL PREVIEW CONFIGURATION
#' ==========================================================================
#' Get the current preview limit from global options
#' @return Integer preview limit (default: 10)
#' @export
get_preview_limit <- function() {
  list(
    rows = getOption("mydb.preview.rows", default = 30),
    cols = getOption("mydb.preview.cols", default = 20)
  )
}

#' Set the global preview limit for all MyDB objects
#' @param rows Integer, number of rows to show in previews
#' @param cols Integer, number of columns to show in previews
#' @export
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
#' @export
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
                        # Don't error yet, might be creating a new one
                        message("Connected to database. No tables found yet.")
                      }
                    } else {
                      self$table <- table
                    }
                  },

                  connect = function() {
                    self$con <- DBI::dbConnect(duckdb::duckdb(), dbdir = self$file_path)
                    # message("Successfully connected to DuckDB")
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

#' Updated print methods with configurable preview limit
#' @export
print.db.vector <- function(x, ...) {
  preview_limits <- get_preview_limit()

  # Get total count first to determine if we should show preview
  count_query <- paste0("SELECT COUNT(*) as total FROM (", x$query, ") count_subq")
  total_count <- DBI::dbGetQuery(x$con, count_query)$total

  # Only show preview message and limit if total count > limit
  if (total_count > preview_limits$rows) {
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
    # For small datasets, show all without any message
    result <- DBI::dbGetQuery(x$con, x$query)
  }

  print(as.vector(result[[x$column]]))
}

#' @export
print.db.dataframe <- function(x, ...) {
  preview_limits <- get_preview_limit()

  # Get total count first to determine if we should show preview
  count_query <- paste0("SELECT COUNT(*) as total FROM (", x$query, ") count_subq")
  total_count <- DBI::dbGetQuery(x$con, count_query)$total

  # Apply column limiting
  col_info <- apply_column_preview_limit(x$query, x$columns, preview_limits$cols)

  # Only show messages and limit if total count > limit
  if (total_count > preview_limits$rows) {
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
    # For small datasets, show all without any message
    preview_query <- col_info$query
  }

  result <- DBI::dbGetQuery(x$con, preview_query)
  print(as.data.frame(result))

  # Show truncation info only for large datasets with truncated columns
  if (col_info$truncated) {
    hidden_cols <- col_info$total_cols - col_info$shown_cols
    cat("... (", hidden_cols, " more columns not shown. Use as.ram() to see all data)\n")
  }
}

#' @export
print.db.matrix <- function(x, ...) {
  preview_limits <- get_preview_limit()

  # Get total count to determine if we need preview message
  count_query <- paste0("SELECT COUNT(*) as total FROM (", x$query, ") count_subq")
  total_count <- DBI::dbGetQuery(x$con, count_query)$total

  nrow <- x$nrow
  ncol <- x$ncol

  # Determine rows to show
  rows_to_show <- if (nrow > preview_limits$rows) {
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
  # Fetch data in column-major order
  positions <- c()
  for (col_idx in 1:cols_to_show) {
    for (row_idx in 1:rows_to_show) {
      pos <- (col_idx - 1) * nrow + row_idx
      positions <- c(positions, pos)
    }
  }

  if (length(positions) > 0) {
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
  } else {
    cat("Empty matrix\n")
  }
}

#' @export
as.ram <- function(x) {
  UseMethod("as.ram")
}

#' @export
as.ram.db.vector <- function(x) {
  result <- DBI::dbGetQuery(x$con, x$query)
  as.vector(result[[x$column]])
}

#' @export
as.ram.db.dataframe <- function(x) {
  result <- DBI::dbGetQuery(x$con, x$query)
  as.data.frame(result)
}

#' @export
as.ram.db.matrix <- function(x) {
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
}
