## ==========================================================================##
# Project: GIRFT Elective Hubs Evaluation 

# Team: Improvement Analytics Unit (IAU) at the Health Foundation

# Script: 00_preamble.R 

# Corresponding authors: Stefano Conti (stefano.conti@nhs.net) and Tatjana Marks (tatjana.marks@health.org.uk)

# Description: 
# Load required libraries and set default object names used across multiple later scripts 

# Dependencies: 
# N/A

# Inputs: 
# N/A

# Outputs:
# Libraries and values loaded in environment 

# Notes: 
# To use, need to adjust location of where input data etc are saved in own analysis - see 'Set bucket names' section  
# Generally may need to adjust file paths across all scripts as currently assumed S3 bucket system and GitHub structure 
# Some libraries are left over from exploratory analyses and not used in final scripts, e.g. bcp 
## ==========================================================================##

# Clean environment -------------------------------------------------------

rm(list = ls())

# Load packages -----------------------------------------------------------

library(tibble) # for results table data manipulation
  
library(tidyverse) # for data manipulation and plots 
  
library(dplyr) # for data manipulation
  
library(here) # used to get easier filepath references for objects not in Git
  
library(tidyr) # for data manipulation
  
library(janitor) # for data manipulation
  
library(abind) # to join arrays
  
library(aws.s3) # for AWS S3 API
  
library(bcp) # for Barry and Hartigan change-point model
  
library(forecast) # time-series forecasting
  
library(lme4) # for GLMM
  
library(MASS) # for "Modern Applied Statistics with S" utilities
  
library(multcomp) # for multiple inferences
  
library(readxl) # for importing MS Excel spreadsheets
  
library(gsynth) # to use synthetic control methods
  
library(panelView) # to produce gsynth plots
  
library(parallel) # to check number of cores
  
library(forcats) # to relevel rows for forest plot
  

# Set bucket names --------------------------------------------------------

project_bucket <- '' # Set project directory

output_bucket <- '' # Set output directory on AWS S3 bucket system

output_internal_dir <- ""  # Set output directory on R server


# Define strings used for each outcome in Rmd files -----------------------

rates <- "elect_rate"
dc <- "elect_los.dc_ratio"
ip_los <- "elect_los.ip_avg"

rates_hvlc <- "hvlc_rate"
dc_hvlc <- "hvlc_los.dc_ratio"
ip_los_hvlc <- "hvlc_los.ip_avg"


# Set up ancillary parameters ---------------------------------------------

old_par <- par(no.readonly=TRUE)  # Set default graphical parameters


hvlc_names.vec <- setNames(c("ort", "opht", "gs", "uro", "ent", "gm", "spn"), 
                           nm=c("Orthopaedics", "Ophthalmology", "General Surgery", 
                                "Urology", "ENT", "Gynaecology and Maternity", "Spinal")
                           )  # Set named vector of HVLC specialty names


out_names.arr <- outer(c(Elective="elect", HVLC="hvlc", hvlc_names.vec), 
                       setNames(c("rate", paste("lhs", c("ip_mean", "op_prob"), sep=".")), 
                                nm=c("Activity rate", "Mean in-patient LHS", "Out-patient ratio")
                                ), 
                       FUN=paste, sep="_"
                       )  # Set 2d-array by "specialty", "statistic" of outcome labels

names(dimnames(out_names.arr)) <- c("specialty", "statistic")  # Set outcome labels 2d-array margin names


out_labels.arr <- outer(dimnames(out_names.arr)$specialty, 
                        dimnames(out_names.arr)$statistic, 
                        FUN=paste, sep=" "
                        )  # Derive 2d-array of outcome labels for graphical representation

dimnames(out_labels.arr) <- dimnames(out_names.arr)  # Set outcome labels 2d-array margin names

out_labels.ls <- out_labels.arr  # Initialise list of formatted outcome labels

out_labels.ls[grep("rate$", x=out_labels.ls)] <- sapply(grep("rate$", x=out_labels.ls, value=TRUE), 
                                                    FUN=function(out) 
                                                      bquote(.(out) %*% "1,000")
                                                    )  # Format outcome labels

names(out_labels.ls) <- out_labels.arr  # Set names to list of formatted outcome labels


pre.post_key.vec.ls <- list(pre=rev(-seq.int(12)), 
                            post=seq.int(12)
                            )  # Set list by study period of key intervention times


pre.post_span_key.vec <- c(pre=paste(rev(range(pre.post_key.vec.ls$pre)), collapse=" - "), 
                           post=paste(range(pre.post_key.vec.ls$post), collapse=" - ")
                           )  # Derive vector by study period of key intervention time spans of interest
