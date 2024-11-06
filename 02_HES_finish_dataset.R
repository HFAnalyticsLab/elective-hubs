## ==========================================================================##
# Project: GIRFT Elective Hubs Evaluation

# Team: Improvement Analytics Unit (IAU) at the Health Foundation

# Script: 02_HES_finish_dataset.R

# Corresponding author:Freya Tracey (freya.tracey@health.org.uk)

# Description:
# Continuation completing the processing of HES data
# Add the concatenated diagnosis and procedure variables to the data set 
# using the 'long' tables

# Dependencies:
# '01_HES_Spells_EH.R'

# Inputs:
# Takes spells made in 01_HES_Spells_EH.R

# Outputs:
# HES spells with diagnosis and procedure variables 

# Notes: To use, need to adjust locations of R scripts and csv files

## ==========================================================================##

library(data.table) #not included in preamble 
library(stringr) #not included in preamble 

project_bucket <- ''  # Set project directory

## Read apcs data back in
apcs <- s3readRDS(object = 'all_HES/apcs_17_admmthd.rds',
                  bucket = project_bucket)

## Read the diagnoses back in
diag <- s3readRDS(object = 'all_HES/diag_sel.rds',
                  bucket = project_bucket)

## Create the wide version of the diagnosis data
diag[, num := seq_len(.N), by = id] # add DIAG number for wide field header
max(diag$num)
diag <- diag %>%
  pivot_wider(names_from = num,
              values_from = ICD10code)

## Get the primary diagnosis variable from the APCS data
## NOTE: the primary diagnosis is NOT in the long diagnosis table
diag_01 <- apcs %>% select(id, DIAG_01)

## Combine the primary diagnosis to the other diagnoses
diagall <- merge(x = diag_01, y = diag, by = "id", all.x = TRUE)

sam <- diagall %>% slice(1:1000)
rm(diag)
rm(diag_01)
gc()

## Concatenate all the diags into one variable
tnrdiagall <- diagall %>% unite("tnrdiagall", 2:21, sep = ",", na.rm = TRUE)

sam <- tnrdiagall %>% slice(1:1000)
rm(diagall)
gc()

## Save the diagnoses just in case
s3saveRDS(x = tnrdiagall
          ,object = 'all_HES/tnrdiagall.rds'
          ,bucket = project_bucket
          ,multipart=TRUE)

#################################
## TERMINATE R to clear memory ##
#################################

## Read apcs data back in
apcs <- s3readRDS(object = 'all_HES/apcs_17_admmthd.rds',
                  bucket = project_bucket)

## Read the procs data back in
oper <- s3readRDS(object = 'all_HES/apcs_oper.rds',
                  bucket = project_bucket)

## Get the primary operation variable from the APCS data
oper_01 <- apcs %>% select(id, OPERTN_01)

rm(apcs)
gc()

## Create the wide version of the procedures data
oper[, num := seq_len(.N), by = id] # add OPER number for wide field header
max(oper$num)
oper <- oper %>%
  pivot_wider(names_from = num,
              values_from = OPERTNcode)

## Combine the primary operation to the other operations0
operall <- merge(x = oper_01, y = oper, by = "id", all.x = TRUE)

sam <- operall %>% slice(1:1000)

## Save the operations all as need to terminate R for memory issues
s3saveRDS(x = operall
          ,object = 'all_HES/operall.rds'
          ,bucket = project_bucket
          ,multipart=TRUE)

#################################
## TERMINATE R to clear memory ##
#################################

operall <- s3readRDS(object = 'all_HES/operall.rds',
                     bucket = project_bucket)

# ## Slice the data to try to overcome memory issues with the unite package
# oper1 <- operall %>% slice(1:4000000)
# oper2 <- operall %>% slice(4000001:8000000)
# oper3 <- operall %>% slice(8000001:12000000)
# oper4 <- operall %>% slice(12000001:16428443)

rm(oper)

tnrprocall <- operall %>% unite("tnrprocall", 2:25, sep = ",", na.rm = TRUE)


# rm(operall)
# gc()
# 
# ## Concatenate all the procs into one variable
# tnrprocall_1 <- oper1 %>% unite("tnrprocall", 2:25, sep = ",", na.rm = TRUE)
# rm(oper1)
# gc()
# 
# tnrprocall_2 <- oper2 %>% unite("tnrprocall", 2:25, sep = ",", na.rm = TRUE)
# rm(oper2)
# gc()
# 
# tnrprocall_3 <- oper3 %>% unite("tnrprocall", 2:25, sep = ",", na.rm = TRUE)
# rm(oper3)
# gc()
# 
# tnrprocall_4 <- oper4 %>% unite("tnrprocall", 2:25, sep = ",", na.rm = TRUE)
# rm(oper4)
# gc()
# 
# ## Bind all the data sets together
# tnrprocall <- rbind(tnrprocall_1, tnrprocall_2, tnrprocall_3, tnrprocall_4)

sam <- tnrprocall %>% slice(1:1000)


## Save the operations all as need to terminate R for memory issues
s3saveRDS(x = tnrprocall
          ,object = 'all_HES/tnrprocall.rds'
          ,bucket = project_bucket
          ,multipart=TRUE)

rm(tnrprocall)
rm(oper_01)
rm(operall)
# rm(tnrprocall_1)
# rm(tnrprocall_2)
# rm(tnrprocall_3)
# rm(tnrprocall_4)
gc()

#################################
## TERMINATE R to clear memory ##
#################################

## Read in the procedures and diagnosis data
tnrprocall <- s3readRDS(object = 'all_HES/tnrprocall.rds'
                     ,bucket = project_bucket
                     ,multipart=TRUE)

tnrdiagall <- s3readRDS(object = 'all_HES/tnrdiagall.rds'
                        ,bucket = project_bucket
                        ,multipart=TRUE)

## Merge the new proc and diag data together
diag_proc_all <- merge(x = tnrdiagall, y = tnrprocall, by = "id", all.x = TRUE)

sam <- diag_proc_all %>%  slice(1:1000)

rm(tnrdiagall)
rm(tnrprocall)
gc()

## Read in the APCS data
apcs <- s3readRDS(object = 'all_HES/apcs_17_admmthd.rds',
                  bucket = project_bucket)

## Merge the diag and proc data back on to the apcs data
apcs <- merge(x = apcs, y = diag_proc_all, by = "id", all.x = TRUE)

sam <- apcs %>% slice(1:1000)

ds_col <- as.data.frame(colnames(sam))

## Reorder the data set
apcs2 <- apcs %>% select(TOKEN_PERSON_ID,spell_id,SUSRECID,spellstartdate,spellenddate,ADMIDATE,admmnth,admmyr,ADMIMETH,
           ADMINCAT,ADMISORC,DISDATE,DISDEST,DISMETH,EPISTART,EPIEND,EPIORDER,EPIKEY,EPISTAT,EPIDUR,
           DIAG_01,tnrdiagall,OPERTN_01,tnrprocall,OPDATE_01,CLASSPAT,ETHNIC5,ETHNOS,SEX,STARTAGE,
           STARTAGE_CALC,GPPRAC,LSOA11,PROCODE3,PROCODET,INTMANIG,MAINSPEF,TRETSPEF,DUPLICATE,ELECDATE,
           ENCRYPTED_HESID,apc_order_id,transit_nhsd,id)

sam <- apcs2 %>% slice(1:1000)

## Save the the final spells table
s3saveRDS(x = apcs2
          ,object = 'all_HES/eh_hes_spells.rds'
          ,bucket = project_bucket
          ,multipart=TRUE)

rm(list = ls())
