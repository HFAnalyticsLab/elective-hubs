## ========================================================================== ##
# Project: GIRFT Elective Hubs Evaluation

# Team: Improvement Analytics Unit (IAU) at the Health Foundation

# Script: 03_hes_filter_hes_intermediate_definitions.R

# Corresponding author:Freya Tracey (freya.tracey@health.org.uk)

# Description:
# Limiting HES to intermediate definition of surgeries, using Abbott et al. 2017

# Dependencies:
# '02_HES_finish_dataset.R'

# Inputs:
# Takes spells from 02 script

# Outputs:
# Subset of HES spells which meet intermediate definition of surgeries 

# Notes: To use, need to adjust locations of R scripts and csv files

## ========================================================================== ##

library(data.table) #not in preamble
library(arrow) #not in preamble 
library(stringr) #not in preamble 

project_bucket <- ''  # Set project directory 

opcs <- s3read_using(FUN = read.csv, object = 'data/reference csvs/OPCS code lists.csv', bucket = proj_bucket)[1:4]
#use Abbott et al 2017 supp material to make a csv of all OPCS codes 

# Upload intermediate definitions of elective surgery
include <- opcs$intermediate[1:1047]

data <- open_dataset('') %>% 
  # head(1000) %>% 
  filter(str_detect(tnrprocall, paste(include, collapse = "|"))) %>% 
  collect() %>%
  as.data.frame ()

s3write_using(data, FUN = write_parquet, object = "hes_intermediate_surg.parquet", bucket = project_bucket)
