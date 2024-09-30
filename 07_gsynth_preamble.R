## ==========================================================================##
# Project: GIRFT Elective Hubs Evaluation

# Team: Improvement Analytics Unit (IAU) at the Health Foundation

# Script: 07_gsynth_preamble.R

# Corresponding author: Tatjana Marks (tatjana.marks@health.org.uk)

# Description:
# Manipulate dataset created in scripts 01-06 into format required by gsynth function
# Also define functions and defaults for applying and interpreting gysnth

# Dependencies:
# '00_preamble.R'

# Inputs:
# 'Trust look up for ITS 20230703.csv' (reference csv file containing data on when each hub opened, acquired from GIRFT)
# 'HES - all elective and hvlc activity and los by provider and month.csv' (final dataset created in scripts 01-06)

# Outputs:
# Note: All outputs saved into global environment (not exported externally)
# lm_dat_old (final gsynth input dataset old hubs)
# lm_dat_new (final gsynth input dataset new hubs)
# Model defaults used across analyses (e.g. rates, los, dc + main and sensitivity)

# Notes:
# Some naming conventions are leftover from previous modelling attempts, e.g. use of "lm" in name of datasets
#
# To use, need to adjust locations of R scripts and csv files
# E.g. replace source("~/iaelecthubs1/hes_did/pipeline/00_preamble.R") with location where '00_preamble.R' script is saved in own directory
# E.g. replace "data/reference csvs/Trust look up for ITS 20230703.csv" with location where reference csv is saved

## ==========================================================================##

# Source relevant scripts -------------------------------------------------

source("~/iaelecthubs1/hes_did/pipeline/00_preamble.R")


# Define objects used across all gsynth models ----------------------------

## Set function argument defaults -----------------------------------------------

na.rm <- TRUE # List-wise delete missing data
index <- c("PROCODE3", "time") # Specify the unit (group) and time indicators
force <- "two-way" # Impose both unit and time fixed effects
r <- c(0, 5) # Allow for between 0 and 5 factors (as cross validating not technically necessary to specify)
CV <- TRUE # Cross validate
se <- TRUE # Produce uncertainly estimates
nboots <- 10000 # Specify number of bootstrap runs
inference <- "parametric" # Parametric and non-parametric setting leads to warning messages due to sample size
cores <- detectCores() - 1 # Automatically detect number of available cores and leave one free
seed <- 42 # Set seed for bootstrapping for replicability
min.T0 <- 6 # At least 6 months of data pre intervention required


## Set character string defaults --------------------------------------

## Define covariates
# Covariates for models using rate outcomes (elective rates)
covariates_rates <- " + catchment_cat + sex_male_pct + white_pct + age_65_plus_pct"

# Covariates for models using length of stay or day case ratio
covariates_los_dc <- " + catchment_cat + sex_male_pct_ec + white_pct_ec + age_65_plus_pct_ec + quintile_1_pct_ec + comorb_pct"

## Define weights, using same naming system as for covariates
weight_rates <- "wt_catchment_cat"
weight_los_dc <- "wt_n_activity"


# Define objects used across all data processing  ----------------------------

## Define function to create weights column -------------------------------

add_weights <- function(dat, var) {
  ## This function adds a weight to each row of dat. The weight corresponds to the
  ## variable given in var divided by its sum over treated unit and treated time.
  sum1 <- dat %>%
    group_by(treated_unit, time) %>%
    summarise_at(
      .vars = var,
      .funs = funs(var_sum = sum)
    ) %>%
    as.data.frame()

  dat.out <- dat %>%
    left_join(sum1, by = c("treated_unit", "time")) %>%
    mutate_at(
      .vars = var,
      .funs = funs(wt := . / var_sum)
    ) %>%
    dplyr::select(-var_sum) %>%
    as.data.frame()
  dat.out
}

## Define study dates ------------------------------------------------

study_start_date <- "2018-04-01" # Set earliest date for which we have reliable data

study_end_date <- "2022-12-01" # Set latest date for which we have reliable data

pandemic_start <- "2020-03-01" # Set starting month of pandemic

pandemic_end <- "2021-04-01" # Set last date of pandemic


# Read in data used in all gsynth models ------------------------------------------------------------

## Hubs start dates --------------------------------------------------------

hub_start_date_raw <- s3read_using(read.table,
  header = TRUE, sep = ",", quote = "\"", fill = TRUE,
  object = "data/reference csvs/Trust look up for ITS 20230703.csv",
  bucket = project_bucket
) # Load .csv spreadsheet sheet of hub start dates

hub_start_date <- hub_start_date_raw %>%
  dplyr::select(trust_code, hub_start_date) %>%
  distinct() %>%
  mutate(hub_start_date = as.Date(hub_start_date, format = "%d/%m/%Y")) %>%
  group_by(trust_code) %>%
  filter(hub_start_date == min(hub_start_date)) # Clean the file, keeping only the earliest date where a trust has multiple hub dates


## HES data ----------------------------------------------------------------

# Read in
hes_dat <- s3read_using(read.table,
  header = TRUE, sep = ",", quote = "\"", row.names = 1,
  object = "data/aggregate data sets/HES - all elective and hvlc activity and los by provider and month.csv",
  bucket = project_bucket
)

# Rename columns
names(hes_dat)[match(c("el_rate", "los_avg", "dc", "hvlc_rate"),
  table = names(hes_dat)
)] <- c(
  paste(c("rate", "los_avg", "dc_prop"), "elect", sep = "_"),
  "rate_hvlc"
) # Rename selected variables in the aggregate outcomes data-frame - el_rate to rate_elect, los_avg to los_avg_elect, dc to dc_prop_elect, hvlc_rate to rate_hvlc

names(hes_dat)[grep("dc|elect|los|rate",
  x = names(hes_dat)
)] <- sub("los_avg",
  replacement = "los.ip_avg",
  x = sub("dc_prop",
    replacement = "los.dc_ratio",
    x = sub("(^.+)(_)([[:alpha:]]+$)",
      replacement = "\\3\\2\\1",
      x = grep("dc|elect|los|rate", x = names(hes_dat), value = TRUE)
    )
  )
) # Rename selected variables in the aggregate outcomes data-frame - replace los_avg with los.ip_avg, dc_prop with los.dc_ratio, reverse order where e.g. rate was at the end of the column name,


# Process data into format required for gysnth function ----------------

## Prepare old hub df ------------------------------------------------------

## Differences old to new hubs analysis data preparation:
## 1. Changed any mentioned of "new" to "old"
## 2. The "post" column is created based on fixed date

gsynth_dat_wo_start_date_old <- subset(hes_dat,
  subset = cat_elective_hub %in% c("old", "non_hub")
) # Subset outcomes data-frame to key trust types and variables

gsynth_dat_old <- gsynth_dat_wo_start_date_old %>%
  left_join(hub_start_date, join_by(PROCODE3 == trust_code)) # Add information on when each hub started onto the data-frame

gsynth_dat_old <- subset(gsynth_dat_old,
  subset = as.Date(paste(year_month, "01", sep = "_"), format = "%Y_%m_%d") %in%
    c(
      seq(as.Date(study_start_date), as.Date(pandemic_start), by = "month"),
      seq(as.Date(pandemic_end), as.Date(study_end_date), by = "month")
    )
) # Subset aggregate outcomes data-frame to non-pandemic "year_month" values within the '2018_04" - "2022_12" time window - to be updated if new HES data cut becomes available


lm_dat_old <- within(gsynth_dat_old, expr = {
  yyyymmdd <- as.Date(paste(sub("_", replacement = "-", x = year_month), "01", sep = "-")) # Derive date variable from formatting "year_month" as Date by adding fictitious day value
  intervention <- cat_elective_hub == "old" # all old trusts should be labelled as intervention trusts
  post <- case_when(
    yyyymmdd >= pandemic_end ~ TRUE,
    yyyymmdd >= study_start_date & yyyymmdd <= pandemic_start ~ FALSE,
    TRUE ~ NA
  ) # Derive post-intervention period status logical variable
}) # Append to outcomes data-frame intervention trust status and post-intervention period status logical variables

lm_dat_old <- lm_dat_old %>%
  mutate(
    intervention = ifelse(intervention == TRUE, 1, 0),
    post = ifelse(post == TRUE, 1, 0)
  ) %>%
  group_by(PROCODE3) %>%
  mutate(time = row_number()) %>%
  ungroup() %>%
  rename(treated_unit = intervention) %>%
  mutate(
    intervention = ifelse(treated_unit == 1 & post == 1, 1, 0),
    intervention = ifelse(is.na(post), NA, intervention)
  ) %>%
  add_weights("n_activity") %>%
  rename(wt_n_activity = wt) %>%
  add_weights("catchment_cat") %>%
  rename(wt_catchment_cat = wt) # Create additional columns required for gsynth


## Prepare new hub df ------------------------------------------------------

gsynth_dat_wo_start_date_new <- subset(hes_dat,
  subset = cat_elective_hub %in% c("new", "non_hub")
) # Subset outcomes data-frame to key trust types and variables

gsynth_dat_new <- gsynth_dat_wo_start_date_new %>%
  left_join(hub_start_date, join_by(PROCODE3 == trust_code)) # Add information on when each hub started onto the data-frame

gsynth_dat_new <- subset(gsynth_dat_new,
  subset = as.Date(paste(year_month, "01", sep = "_"), format = "%Y_%m_%d") %in%
    c(
      seq(as.Date(study_start_date), as.Date(pandemic_start), by = "month"),
      seq(as.Date(pandemic_end), as.Date(study_end_date), by = "month")
    )
) # Subset aggregate outcomes data-frame to non-pandemic "year_month" values within the '2018_04" - "2022_12" time window - to be updated once new HES data cut becomes available

lm_dat_new <- within(gsynth_dat_new, expr = {
  yyyymmdd <- as.Date(paste(sub("_", replacement = "-", x = year_month), "01", sep = "-")) # Derive date variable from formatting "year_month" as Date by adding fictitious day value
  treated_unit <- cat_elective_hub == "new" # all new trusts should be labelled as intervention trusts
  intervention <- case_when(
    yyyymmdd >= hub_start_date & cat_elective_hub == "new" ~ 1,
    yyyymmdd < hub_start_date & cat_elective_hub == "new" ~ 0,
    cat_elective_hub == "non_hub" ~ 0,
    TRUE ~ NA
  )
}) # Append to outcomes data-frame intervention trust status and post-intervention period status logical variables

lm_dat_new <- lm_dat_new %>%
  group_by(PROCODE3) %>%
  mutate(time = row_number()) %>%
  ungroup() %>%
  add_weights("n_activity") %>%
  rename(wt_n_activity = wt) %>%
  add_weights("catchment_cat") %>%
  rename(wt_catchment_cat = wt) # Create additional columns required for gsynth

# Define custom function for gsynth model ---------------------------------

### For testing

## Main analysis example
# dat_name <- lm_dat_new
# hub_type <- "new"
# output_name <- "elect_rate"
# subfolder_name <- "pooled"
# covariates_type <- covariates_rates
# weight_type <- weight_rates

# Note, the core part of this function is applying the gsynth function using the model defaults
# The rest (e.g. the tryCatch function) is for efficiency purposes
gsynth_custom <- function(dat_name, hub_type, output_name, subfolder_name, covariates_type, weight_type) {
  gsynth_frm <- as.formula(paste0(output_name, " ~ intervention", covariates_type)) # Derive gsynth formula for rate outcomes

  gsynth_pooled <-
    tryCatch(
      {
        gsynth(
          formula = gsynth_frm,
          data = dat_name, na.rm = na.rm, index = index,
          weight = weight_type,
          force = force, r = r, CV = CV, se = se, nboots = nboots, seed = seed,
          inference = inference, min.T0 = min.T0, cores = cores
        ) # Fit gsynth formula model to outcomes applying model defaults used above
      },
      error = function(e) {
        cat("An error occured:", conditionMessage(e), "\n")
        return(vector())
      }
    ) # Return empty vector if gsynth fails (due to data structure for some specialties)

  s3write_using(
    x = gsynth_pooled,
    FUN = saveRDS,
    object = paste0(
      output_bucket, "gsynth results/gsynth objects/", subfolder_name, "/",
      hub_type, "_hub_", output_name, ".rds"
    ),
    bucket = output_bucket
  ) # Save gsynth object in project folder
}
