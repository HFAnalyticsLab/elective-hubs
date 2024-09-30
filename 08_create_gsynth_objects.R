## ==========================================================================##
# Project: GIRFT Elective Hubs Evaluation

# Team: Improvement Analytics Unit (IAU) at the Health Foundation

# Script: 08_create_gsynth_objects.R

# Corresponding author: Tatjana Marks (tatjana.marks@health.org.uk)

# Description:
# Apply gsynth function and export model objects

# Dependencies:
# '07_gsynth_preamble.R'

# Inputs:
# Takes inputs created in '07_gsynth_preamble.R'

# Outputs:
# exports final datasets externally
# exports gsynth model objects externally

# Notes: To use, need to adjust locations of R scripts and csv files
# E.g. replace 'gsynth results/gsynth input datasets/lm_dat_new.csv' with location of where you saved 'lm_dat_new.csv'

## ==========================================================================##

# Source relevant scripts -------------------------------------------------

source("~/iaelecthubs1/hes_did/pipeline/07_gsynth_preamble.R")

# Export final datasets  ----------------------------------------

s3write_using(lm_dat_new,
  FUN = write.csv,
  object = paste0(output_bucket, "gsynth results/gsynth input datasets/lm_dat_new.csv"),
  bucket = output_bucket
)

s3write_using(lm_dat_old,
  FUN = write.csv,
  object = paste0(output_bucket, "gsynth results/gsynth input datasets/lm_dat_old.csv"),
  bucket = output_bucket
)

# Apply function for main analyses ----------------------------------------

### Differences between "_rates" and "_los_dc" models
## 1. Different "weights" column
## 2. Different covariates


## New hubs ----------------------------------------------------------------

# Elect rate
gsynth_custom(
  dat_name = lm_dat_new, hub_type = "new", output_name = "elect_rate", subfolder_name = "pooled",
  covariates_type = covariates_rates, weight_type = weight_rates
)
gsynth_custom(
  dat_name = lm_dat_new, hub_type = "new", output_name = "hvlc_rate", subfolder_name = "pooled",
  covariates_type = covariates_rates, weight_type = weight_rates
)

# Dc
gsynth_custom(
  dat_name = lm_dat_new, hub_type = "new", output_name = "elect_los.dc_ratio", subfolder_name = "pooled",
  covariates_type = covariates_los_dc, weight_type = weight_los_dc
)
gsynth_custom(
  dat_name = lm_dat_new, hub_type = "new", output_name = "hvlc_los.dc_ratio", subfolder_name = "pooled",
  covariates_type = covariates_los_dc, weight_type = weight_los_dc
)

# Los
gsynth_custom(
  dat_name = lm_dat_new, hub_type = "new", output_name = "elect_los.ip_avg", subfolder_name = "pooled",
  covariates_type = covariates_los_dc, weight_type = weight_los_dc
)
gsynth_custom(
  dat_name = lm_dat_new, hub_type = "new", output_name = "hvlc_los.ip_avg", subfolder_name = "pooled",
  covariates_type = covariates_los_dc, weight_type = weight_los_dc
)


## Old hubs ----------------------------------------------------------------

# Elect rate
gsynth_custom(
  dat_name = lm_dat_old, hub_type = "old", output_name = "elect_rate", subfolder_name = "pooled",
  covariates_type = covariates_rates, weight_type = weight_rates
)
gsynth_custom(
  dat_name = lm_dat_old, hub_type = "old", output_name = "hvlc_rate", subfolder_name = "pooled",
  covariates_type = covariates_rates, weight_type = weight_rates
)

# Dc
gsynth_custom(
  dat_name = lm_dat_old, hub_type = "old", output_name = "elect_los.dc_ratio", subfolder_name = "pooled",
  covariates_type = covariates_los_dc, weight_type = weight_los_dc
)
gsynth_custom(
  dat_name = lm_dat_old, hub_type = "old", output_name = "hvlc_los.dc_ratio", subfolder_name = "pooled",
  covariates_type = covariates_los_dc, weight_type = weight_los_dc
)

# Los
gsynth_custom(
  dat_name = lm_dat_old, hub_type = "old", output_name = "elect_los.ip_avg", subfolder_name = "pooled",
  covariates_type = covariates_los_dc, weight_type = weight_los_dc
)
gsynth_custom(
  dat_name = lm_dat_old, hub_type = "old", output_name = "hvlc_los.ip_avg", subfolder_name = "pooled",
  covariates_type = covariates_los_dc, weight_type = weight_los_dc
)
