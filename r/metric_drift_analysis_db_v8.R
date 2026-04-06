suppressPackageStartupMessages({
  library(DBI)
  library(dplyr)
  library(lubridate)
  library(optparse)
  library(glue)
  library(tidyr)
})

get_backend <- function() {
  tolower(Sys.getenv("DB_BACKEND", unset = "mysql"))
}

get_env_required <- function(name, default = "") {
  val <- Sys.getenv(name, unset = default)
  if (identical(val, "") || is.na(val)) {
    stop(glue("Missing required environment variable: {name}"))
  }
  val
}

get_conn <- function() {
  backend <- get_backend()

  if (backend == "sqlite") {
    suppressPackageStartupMessages(library(RSQLite))
    path <- get_env_required("SQLITE_PATH")
    return(dbConnect(RSQLite::SQLite(), path))
  }

  if (backend %in% c("mysql", "mariadb")) {
    suppressPackageStartupMessages(library(RMariaDB))
    host <- Sys.getenv("DB_HOST", unset = "127.0.0.1")
    port <- as.integer(Sys.getenv("DB_PORT", unset = "3306"))
    dbname <- get_env_required("DB_NAME", "weblog")
    user <- get_env_required("DB_USER", "nethru")
    password <- get_env_required("DB_PASSWORD", "nethru1234")

    message(glue("[INFO] RMariaDB connect host={host} port={port} db={dbname} user={user}"))
    return(
      dbConnect(
        RMariaDB::MariaDB(),
        host = host,
        port = port,
        dbname = dbname,
        user = user,
        password = password,
        bigint = "numeric"
      )
    )
  }

  suppressPackageStartupMessages(library(RPostgres))
  dbConnect(
    RPostgres::Postgres(),
    host = Sys.getenv("DB_HOST", unset = "127.0.0.1"),
    port = as.integer(Sys.getenv("DB_PORT", unset = "5432")),
    dbname = get_env_required("DB_NAME"),
    user = get_env_required("DB_USER"),
    password = get_env_required("DB_PASSWORD")
  )
}

safe_z <- function(obs, mean_val, sd_val) {
  if (length(obs) == 0 || length(mean_val) == 0 || length(sd_val) == 0) return(0)
  if (is.na(obs) || is.na(mean_val) || is.na(sd_val) || sd_val == 0) return(0)
  (obs - mean_val) / sd_val
}

status_from_score <- function(method, score) {
  if (length(method) == 0 || is.null(method) || is.na(method) || method == "") return("normal")
  if (length(score) == 0 || is.null(score) || is.na(score)) score <- 0

  abs_score <- abs(score)
  if (method == "zscore") {
    if (abs_score >= 3) return("alert")
    if (abs_score >= 2) return("warn")
    return("normal")
  }
  if (method == "psi") {
    if (score >= 0.25) return("alert")
    if (score >= 0.10) return("warn")
    return("normal")
  }
  if (method == "funnel_change") {
    if (abs_score >= 0.20) return("alert")
    if (abs_score >= 0.10) return("warn")
    return("normal")
  }
  return("normal")
}

severity_from_status <- function(status) {
  if (length(status) == 0 || is.null(status) || is.na(status) || status == "") return("low")
  if (status == "alert") return("high")
  if (status == "warn") return("medium")
  return("low")
}

compute_psi_like <- function(obs, base) {
  if (length(obs) == 0 || length(base) == 0) return(0)
  if (is.na(obs) || is.na(base) || base == 0) return(0)
  p <- max(as.numeric(obs), 1e-6)
  q <- max(as.numeric(base), 1e-6)
  (p - q) * log(p / q)
}

metric_group_from_name <- function(metric_name) {
  if (metric_name %in% c(
    "daily_active_users", "login_success_count", "new_user_ratio", "page_view_count", "avg_session_duration_sec"
  )) return("user_activity")

  if (metric_name %in% c(
    "auth_attempt_count", "auth_success_count", "auth_fail_count",
    "auth_success_rate", "auth_fail_rate", "otp_request_count", "risk_login_count"
  )) return("auth_security")

  if (metric_name %in% c(
    "loan_view_count", "loan_apply_start_count", "loan_apply_submit_count",
    "card_apply_start_count", "card_apply_submit_count", "card_apply_submit_rate",
    "loan_funnel_conversion", "card_funnel_conversion"
  )) return("financial_service")

  if (metric_name %in% c(
    "collector_event_count", "raw_event_count", "estimated_missing_rate", "schema_change_count", "batch_delay_sec"
  )) return("system_operation")

  return("unknown")
}

source_layer_from_name <- function(metric_name) {
  if (metric_name %in% c("raw_event_count")) return("raw")
  if (metric_name %in% c("estimated_missing_rate", "schema_change_count", "batch_delay_sec")) return("control")
  if (metric_name %in% c("loan_funnel_conversion", "card_funnel_conversion")) return("drift")
  return("collector")
}

to_sql_literal_date <- function(d) {
  format(as.Date(d), "%Y-%m-%d")
}

get_table_meta <- function(conn, table_name) {
  backend <- get_backend()

  out <- tryCatch({
    if (backend %in% c("mysql", "mariadb")) {
      x <- dbGetQuery(conn, paste("DESCRIBE", table_name))
      names(x) <- tolower(names(x))
      x <- x %>%
        transmute(
          field = tolower(.data$field),
          type = tolower(.data$type),
          is_nullable = tolower(.data$null) == "yes",
          default_value = .data$default
        )
      return(x)
    }

    if (backend == "postgres") {
      sql <- glue("
        select
          lower(column_name) as field,
          lower(data_type) as type,
          is_nullable = 'YES' as is_nullable,
          column_default as default_value
        from information_schema.columns
        where table_schema = current_schema()
          and table_name = '{table_name}'
        order by ordinal_position
      ")
      return(dbGetQuery(conn, sql))
    }

    if (backend == "sqlite") {
      x <- dbGetQuery(conn, paste0("PRAGMA table_info(", table_name, ")"))
      names(x) <- tolower(names(x))
      x <- x %>%
        transmute(
          field = tolower(.data$name),
          type = tolower(.data$type),
          is_nullable = .data$notnull == 0,
          default_value = .data$dflt_value
        )
      return(x)
    }

    data.frame(field = character(0), type = character(0), is_nullable = logical(0), default_value = character(0))
  }, error = function(e) {
    data.frame(field = character(0), type = character(0), is_nullable = logical(0), default_value = character(0))
  })

  out
}

align_to_table <- function(df, table_meta, run_id_str, run_id_num) {
  if (nrow(table_meta) == 0) stop("Could not inspect target table columns.")

  table_cols <- table_meta$field
  table_types <- setNames(table_meta$type, table_meta$field)

  names(df) <- tolower(names(df))

  if ("created_at" %in% table_cols && !("created_at" %in% names(df))) df$created_at <- Sys.time()
  if ("updated_at" %in% table_cols && !("updated_at" %in% names(df))) df$updated_at <- Sys.time()
  if ("source_layer" %in% table_cols && !("source_layer" %in% names(df))) {
    df$source_layer <- vapply(df$metric_name, source_layer_from_name, character(1))
  }
  if ("metric_group" %in% table_cols && !("metric_group" %in% names(df))) {
    df$metric_group <- vapply(df$metric_name, metric_group_from_name, character(1))
  }
  if ("detail" %in% table_cols && !("detail" %in% names(df))) df$detail <- NA_character_
  if ("note" %in% table_cols && !("note" %in% names(df))) df$note <- NA_character_
  if ("severity" %in% table_cols && !("severity" %in% names(df))) df$severity <- "low"

  if ("run_id" %in% table_cols) {
    run_id_type <- table_types[["run_id"]]
    if (grepl("int|bigint|smallint|tinyint|numeric|decimal", run_id_type)) {
      df$run_id <- as.numeric(run_id_num)
    } else {
      df$run_id <- as.character(run_id_str)
    }
  }

  keep_cols <- intersect(table_cols, names(df))
  out <- df[, keep_cols, drop = FALSE]

  if ("hh" %in% names(out)) out$hh <- as.integer(out$hh)

  out
}

delete_existing_rows <- function(conn, table_name, profile_id, target_date) {
  backend <- get_backend()
  if (backend == "sqlite") {
    dbExecute(
      conn,
      sprintf("delete from %s where profile_id = ? and dt = ?", table_name),
      params = list(profile_id, as.character(target_date))
    )
  } else {
    sql <- glue(
      "delete from {table_name} where profile_id = '{profile_id}' and dt = '{to_sql_literal_date(target_date)}'"
    )
    dbExecute(conn, sql)
  }
}

option_list <- list(
  make_option(c("--date"), type = "character"),
  make_option(c("--profile-id"), type = "character"),
  make_option(c("--output-csv"), type = "character", default = "")
)

opt <- parse_args(OptionParser(option_list = option_list))
if (is.null(opt$date) || is.null(opt$`profile-id`)) stop("--date and --profile-id are required")

run_id_str <- paste0(
  "drift-", opt$`profile-id`, "-", opt$date, "-", format(Sys.time(), "%Y%m%d%H%M%S")
)
run_id_num <- as.numeric(format(Sys.time(), "%Y%m%d%H%M%S"))

conn <- get_conn()
on.exit(try(dbDisconnect(conn), silent = TRUE), add = TRUE)

target_date <- as.Date(opt$date)
weekday_num <- wday(target_date, week_start = 1)
profile_id <- opt$`profile-id`

hourly_sql <- glue("
select profile_id, dt, hh, metric_name, metric_value
from metric_value_hh
where profile_id = '{profile_id}'
  and dt between '{to_sql_literal_date(target_date - days(35))}' and '{to_sql_literal_date(target_date)}'
")

hourly_df <- dbGetQuery(conn, hourly_sql) %>%
  mutate(
    dt = as.Date(dt),
    hh = as.integer(hh),
    metric_value = as.numeric(metric_value)
  )

if (nrow(hourly_df) == 0) {
  message("No hourly metrics found for drift window.")
  quit(save = "no", status = 0)
}

hist_df <- hourly_df %>%
  mutate(weekday_num = wday(dt, week_start = 1))

target_hh <- hist_df %>% filter(dt == target_date)
if (nrow(target_hh) == 0) {
  message(glue("No target-day hourly metrics found for profile_id={profile_id}, dt={target_date}."))
  quit(save = "no", status = 0)
}

baseline_hh <- hist_df %>%
  filter(dt < target_date, weekday_num == !!weekday_num) %>%
  group_by(metric_name, hh) %>%
  summarise(
    baseline_value = mean(metric_value, na.rm = TRUE),
    baseline_sd = sd(metric_value, na.rm = TRUE),
    .groups = "drop"
  )

zscore_metrics <- c(
  "auth_success_count", "auth_fail_count", "otp_request_count",
  "risk_login_count", "page_view_count", "collector_event_count",
  "raw_event_count", "estimated_missing_rate"
)

psi_metrics <- c(
  "loan_view_count", "loan_apply_start_count", "loan_apply_submit_count",
  "card_apply_start_count", "card_apply_submit_count"
)

result_hh <- target_hh %>%
  left_join(baseline_hh, by = c("metric_name", "hh")) %>%
  rowwise() %>%
  mutate(
    drift_method = dplyr::case_when(
      metric_name %in% zscore_metrics ~ "zscore",
      metric_name %in% psi_metrics ~ "psi",
      TRUE ~ "zscore"
    ),
    drift_score = ifelse(
      drift_method == "psi",
      compute_psi_like(metric_value, baseline_value),
      safe_z(metric_value, baseline_value, baseline_sd)
    ),
    drift_status = status_from_score(drift_method, drift_score),
    severity = severity_from_status(drift_status),
    metric_group = metric_group_from_name(metric_name),
    source_layer = source_layer_from_name(metric_name),
    detail = paste0("weekday+hour baseline; weekday=", weekday_num),
    note = NA_character_,
    run_id = run_id_str
  ) %>%
  ungroup() %>%
  transmute(
    profile_id = profile_id,
    dt = dt,
    hh = hh,
    metric_name = metric_name,
    metric_group = metric_group,
    source_layer = source_layer,
    baseline_value = round(coalesce(baseline_value, 0), 6),
    observed_value = round(metric_value, 6),
    drift_score = round(drift_score, 6),
    drift_method = drift_method,
    drift_status = drift_status,
    severity = severity,
    note = note,
    detail = detail,
    run_id = run_id
  )

day_sql <- glue("
select dt, metric_name, metric_value
from metric_value_day
where profile_id = '{profile_id}'
  and dt between '{to_sql_literal_date(target_date - days(35))}' and '{to_sql_literal_date(target_date)}'
  and metric_name in (
    'loan_apply_start_count','loan_apply_submit_count',
    'card_apply_start_count','card_apply_submit_count'
  )
")

day_df <- dbGetQuery(conn, day_sql) %>%
  mutate(
    dt = as.Date(dt),
    metric_value = as.numeric(metric_value),
    weekday_num = wday(dt, week_start = 1)
  )

get_metric_value <- function(df, metric) {
  x <- df %>% filter(metric_name == metric) %>% pull(metric_value)
  if (length(x) == 0 || is.na(x[[1]])) return(0)
  as.numeric(x[[1]])
}

baseline_days <- day_df %>% filter(dt < target_date, weekday_num == !!weekday_num)
target_days <- day_df %>% filter(dt == target_date)

loan_target_rate <- ifelse(
  get_metric_value(target_days, "loan_apply_start_count") == 0,
  0,
  get_metric_value(target_days, "loan_apply_submit_count") / get_metric_value(target_days, "loan_apply_start_count")
)

card_target_rate <- ifelse(
  get_metric_value(target_days, "card_apply_start_count") == 0,
  0,
  get_metric_value(target_days, "card_apply_submit_count") / get_metric_value(target_days, "card_apply_start_count")
)

if (nrow(baseline_days) > 0) {
  loan_base_rates <- baseline_days %>%
    select(dt, metric_name, metric_value) %>%
    tidyr::pivot_wider(names_from = metric_name, values_from = metric_value, values_fill = 0) %>%
    mutate(rate = ifelse(loan_apply_start_count == 0, 0, loan_apply_submit_count / loan_apply_start_count))

  card_base_rates <- baseline_days %>%
    select(dt, metric_name, metric_value) %>%
    tidyr::pivot_wider(names_from = metric_name, values_from = metric_value, values_fill = 0) %>%
    mutate(rate = ifelse(card_apply_start_count == 0, 0, card_apply_submit_count / card_apply_start_count))

  loan_base_mean <- mean(loan_base_rates$rate, na.rm = TRUE)
  card_base_mean <- mean(card_base_rates$rate, na.rm = TRUE)
} else {
  loan_base_mean <- 0
  card_base_mean <- 0
}

funnel_rows <- data.frame(
  profile_id = c(profile_id, profile_id),
  dt = c(target_date, target_date),
  hh = c(NA_integer_, NA_integer_),
  metric_name = c("loan_funnel_conversion", "card_funnel_conversion"),
  metric_group = c("financial_service", "financial_service"),
  source_layer = c("drift", "drift"),
  baseline_value = c(loan_base_mean, card_base_mean),
  observed_value = c(loan_target_rate, card_target_rate),
  drift_score = c(loan_target_rate - loan_base_mean, card_target_rate - card_base_mean),
  drift_method = c("funnel_change", "funnel_change"),
  stringsAsFactors = FALSE
) %>%
  rowwise() %>%
  mutate(
    drift_status = status_from_score(drift_method, drift_score),
    severity = severity_from_status(drift_status),
    note = NA_character_,
    detail = "weekday baseline daily funnel conversion",
    run_id = run_id_str
  ) %>%
  ungroup()

result_all <- bind_rows(result_hh, funnel_rows)

target_tables <- c("metric_drift_result_r", "metric_drift_result")

for (tbl in target_tables) {
  tbl_meta <- get_table_meta(conn, tbl)
  if (nrow(tbl_meta) == 0) {
    message(glue("Skip write: table not found or inaccessible: {tbl}"))
    next
  }

  delete_existing_rows(conn, tbl, profile_id, target_date)
  out_df <- align_to_table(result_all, tbl_meta, run_id_str, run_id_num)
  dbWriteTable(conn, tbl, out_df, append = TRUE, row.names = FALSE)
}

if (opt$`output-csv` != "") {
  write.csv(result_all, opt$`output-csv`, row.names = FALSE)
}

message(glue("[OK] drift completed: run_id={run_id_str}, rows={nrow(result_all)}"))
