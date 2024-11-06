## ==========================================================================##
# Project: GIRFT Elective Hubs Evaluation

# Team: Improvement Analytics Unit (IAU) at the Health Foundation

# Script: 01_HES_Spells_EH.R

# Corresponding author:Freya Tracey (freya.tracey@health.org.uk)

# Description:
# Initial processing of HES data to create spells and analysis dataset. 

# Dependencies:
# '00_preamble.R'

# Inputs:
# None

# Outputs:
# HES spells

# Notes: To use, need to adjust locations of R scripts and csv files

## ==========================================================================##

# Source relevant scripts -------------------------------------------------

source("00_preamble.R")  # Source preamble scripts

library(data.table) # not included in 00_preamble
library(arrow) # not included in 00_preamble

## Get the key variables needed to create spells from the 2021 data
APC_location <- '' #specify where admitted patient care data lives 

open_dataset(file.path(APC_location)) %>% 
  select(ADMIDATE, ADMIMETH, ADMISORC, PROCODET, # not all cols - use SUSRECID to link back
         DISDATE, DISDEST, EPIDUR, EPIEND,
         EPIKEY, EPIORDER, EPISTART, EPISTAT, SUSRECID, TOKEN_PERSON_ID) %>%
  filter(EPISTAT == 3 & !is.na(ADMIDATE)) %>% # only FCEs with admission date
  collect() %>%
  #  top_n(10000) %>% # only 10k for code testing
  setDT() -> apc_21

APC_location <-  '' #respecify where admitted patient care data lives for relevant year

## Get the key variables needed to create spells from the 2022 data
open_dataset(file.path(APC_location)) %>% ## grouper asks for PROVSPNO, we only have PROVSPNOPS?
  select(ADMIDATE, ADMIMETH, ADMISORC, PROCODET, # not all cols - use SUSRECID to link back
         DISDATE, DISDEST, EPIDUR, EPIEND,
         EPIKEY, EPIORDER, EPISTART, EPISTAT, SUSRECID, TOKEN_PERSON_ID) %>%
  filter(EPISTAT == 3 & !is.na(ADMIDATE)) %>% # only FCEs with admission date
  collect() %>%
  #  top_n(10000) %>% # only 10k for code testing
  setDT() -> apc_22


APC_location <- '' #respecify where admitted patient care data lives for relevant year

## Get the key variables needed to create spells from the 2018-20 data
open_dataset(file.path(APC_location)) %>% ## grouper asks for PROVSPNO, we only have PROVSPNOPS?
  select(ADMIDATE, ADMIMETH, ADMISORC, PROCODET, # not all cols - use SUSRECID to link back
         DISDATE, DISDEST, EPIDUR, EPIEND,
         EPIKEY, EPIORDER, EPISTART, EPISTAT, SUSRECID, TOKEN_PERSON_ID) %>%
  filter(EPISTAT == 3 & !is.na(ADMIDATE)) %>% # only FCEs with admission date
  collect() %>%
  #  top_n(10000) %>% # only 10k for code testing
  setDT() -> apc_18_to_20

## Bind the 2021 and 2022 data together
apc <- rbind(apc_18_to_20, apc_21, apc_22)

rm(apc_18_to_20)
rm(apc_21)
rm(apc_22)

## Run HES Spells creation code
apc[, transit_nhsd := fifelse( # create transfer flag - QA logic
  (!(ADMISORC %in% c(49:53, 87)) | !(ADMIMETH %in% c('81', '2B'))) & 
    DISDEST %in% c(49:53, 84, 87), 1, 
  fifelse(
    (ADMISORC %in% c(49:53, 87) | ADMIMETH %in% c('81', '2B')) &
      DISDEST %in% c(49:53, 84, 87), 2,
    fifelse(
      (ADMISORC %in% c(49:53, 87) | ADMIMETH %in% c('81', '2B')) &
        !(DISDEST %in% c(49:53, 84, 87)), 3,
      0)))]

#apc[, .N, by = transit_nhsd]

## NOTE: TOKEN_PERSON_ID used instead of ENCRYPTED_HESID or_hesid
apc <- apc[order(TOKEN_PERSON_ID, EPISTART, EPIORDER, EPIEND, transit_nhsd)] %>% # order data correctly
  .[, .SD[1], by = .(TOKEN_PERSON_ID, EPISTART, EPIORDER, EPIEND, transit_nhsd)] # remove dups with these columns

# clean DISDATE
apc[DISDATE == '1801-01-01' | DISDATE == '1800-01-01' | is.na(DISDATE), 'DISDATE'] <- NA

# get all episodes meeting criteria with previous missing DISDATE
apc[, disdate_for_spell := DISDATE] # create new date to use
apc[, lead_disdate := lead(DISDATE)] # replace with this where required

apc[, disdate_for_spell := if_else(TOKEN_PERSON_ID == lead(TOKEN_PERSON_ID) & # on specific spell conditions
                                     PROCODET == lead(PROCODET) &
                                     ADMIDATE == lead(ADMIDATE) &
                                     is.na(DISDATE), lead_disdate, disdate_for_spell)]

# assign spell ids
apc[order(TOKEN_PERSON_ID, ADMIDATE, EPISTART, EPIORDER, EPIEND, transit_nhsd, disdate_for_spell, PROCODET, EPIKEY)] %>%
  .[, spell_id := .GRP, by = .(TOKEN_PERSON_ID, PROCODET, ADMIDATE, disdate_for_spell)] -> apc

apc[, `:=` (lead_disdate = NULL,
            disdate_for_spell = NULL)]

## Create some admission data month and year variables to summarise on
apc <- apc %>% mutate(apc, admmnth = format(as.Date(apc$ADMIDATE, format = "%Y-%m-%d"),"%m"))
apc <- apc %>% mutate(apc, admmyr = format(as.Date(apc$ADMIDATE, format = "%Y-%m-%d"),"%Y"))

## Free up memory
gc()

## Select the first episode in each spell
## Add an row id to the APC table
apc$id <- 1:nrow(apc)

max(apc$spell_id) ## 33300250, now 59231820 with 18-20 data

## Create a subset of the APC data to use to calculate the first episode in each spell, the earliest episode start data (for spellstartdate)
## and the latest episode end date (for spellenddate)
spell_id_id <- apc %>% 
  select(spell_id, id, EPISTART, EPIEND)
  
## Break the spell_id_id dataset into chunks (2m spell_ids) to make the which.min() function less time consuming
apc1 <- spell_id_id %>% subset(spell_id > 0 & spell_id <= 2000000) %>% group_by(spell_id) %>% slice(which.min(id)) %>% ungroup()
apc2 <- spell_id_id %>% subset(spell_id > 2000000 & spell_id <= 4000000) %>% group_by(spell_id) %>% slice(which.min(id)) %>% ungroup()
apc3 <- spell_id_id %>% subset(spell_id > 4000000 & spell_id <= 6000000) %>% group_by(spell_id) %>% slice(which.min(id)) %>% ungroup()
apc4 <- spell_id_id %>% subset(spell_id > 6000000 & spell_id <= 8000000) %>% group_by(spell_id) %>% slice(which.min(id)) %>% ungroup()
apc5 <- spell_id_id %>% subset(spell_id > 8000000 & spell_id <= 10000000) %>% group_by(spell_id) %>% slice(which.min(id)) %>% ungroup()
apc6 <- spell_id_id %>% subset(spell_id > 10000000 & spell_id <= 12000000) %>% group_by(spell_id) %>% slice(which.min(id)) %>% ungroup()
apc7 <- spell_id_id %>% subset(spell_id > 12000000 & spell_id <= 14000000) %>% group_by(spell_id) %>% slice(which.min(id)) %>% ungroup()
apc8 <- spell_id_id %>% subset(spell_id > 14000000 & spell_id <= 16000000) %>% group_by(spell_id) %>% slice(which.min(id)) %>% ungroup()
apc9 <- spell_id_id %>% subset(spell_id > 16000000 & spell_id <= 18000000) %>% group_by(spell_id) %>% slice(which.min(id)) %>% ungroup()
apc10 <- spell_id_id %>% subset(spell_id > 18000000 & spell_id <= 20000000) %>% group_by(spell_id) %>% slice(which.min(id)) %>% ungroup()
apc11 <- spell_id_id %>% subset(spell_id > 20000000 & spell_id <= 22000000) %>% group_by(spell_id) %>% slice(which.min(id)) %>% ungroup()
apc12 <- spell_id_id %>% subset(spell_id > 22000000 & spell_id <= 24000000) %>% group_by(spell_id) %>% slice(which.min(id)) %>% ungroup()
apc13 <- spell_id_id %>% subset(spell_id > 24000000 & spell_id <= 26000000) %>% group_by(spell_id) %>% slice(which.min(id)) %>% ungroup()
apc14 <- spell_id_id %>% subset(spell_id > 26000000 & spell_id <= 28000000) %>% group_by(spell_id) %>% slice(which.min(id)) %>% ungroup()
apc15 <- spell_id_id %>% subset(spell_id > 28000000 & spell_id <= 30000000) %>% group_by(spell_id) %>% slice(which.min(id)) %>% ungroup()
apc16 <- spell_id_id %>% subset(spell_id > 30000000 & spell_id <= 32000000) %>% group_by(spell_id) %>% slice(which.min(id)) %>% ungroup()
apc17 <- spell_id_id %>% subset(spell_id > 32000000 & spell_id <= 34000000) %>% group_by(spell_id) %>% slice(which.min(id)) %>% ungroup()
apc18 <- spell_id_id %>% subset(spell_id > 34000000 & spell_id <= 36000000) %>% group_by(spell_id) %>% slice(which.min(id)) %>% ungroup()
apc19 <- spell_id_id %>% subset(spell_id > 36000000 & spell_id <= 38000000) %>% group_by(spell_id) %>% slice(which.min(id)) %>% ungroup()
apc20 <- spell_id_id %>% subset(spell_id > 38000000 & spell_id <= 40000000) %>% group_by(spell_id) %>% slice(which.min(id)) %>% ungroup()
apc21 <- spell_id_id %>% subset(spell_id > 40000000 & spell_id <= 42000000) %>% group_by(spell_id) %>% slice(which.min(id)) %>% ungroup()
apc22 <- spell_id_id %>% subset(spell_id > 42000000 & spell_id <= 44000000) %>% group_by(spell_id) %>% slice(which.min(id)) %>% ungroup()
apc23 <- spell_id_id %>% subset(spell_id > 44000000 & spell_id <= 46000000) %>% group_by(spell_id) %>% slice(which.min(id)) %>% ungroup()
apc24 <- spell_id_id %>% subset(spell_id > 46000000 & spell_id <= 48000000) %>% group_by(spell_id) %>% slice(which.min(id)) %>% ungroup()
apc25 <- spell_id_id %>% subset(spell_id > 48000000 & spell_id <= 50000000) %>% group_by(spell_id) %>% slice(which.min(id)) %>% ungroup()
apc26 <- spell_id_id %>% subset(spell_id > 50000000 & spell_id <= 52000000) %>% group_by(spell_id) %>% slice(which.min(id)) %>% ungroup()
apc27 <- spell_id_id %>% subset(spell_id > 52000000 & spell_id <= 54000000) %>% group_by(spell_id) %>% slice(which.min(id)) %>% ungroup()
apc28 <- spell_id_id %>% subset(spell_id > 54000000 & spell_id <= 56000000) %>% group_by(spell_id) %>% slice(which.min(id)) %>% ungroup()
apc29 <- spell_id_id %>% subset(spell_id > 56000000 & spell_id <= 58000000) %>% group_by(spell_id) %>% slice(which.min(id)) %>% ungroup()
apc30 <- spell_id_id %>% subset(spell_id > 58000000 & spell_id <= 60000000) %>% group_by(spell_id) %>% slice(which.min(id)) %>% ungroup()



## Get the minimum episode start date from all the episodes in each spell to use as the spellstartdate
apc_start1 <- spell_id_id %>% subset(spell_id > 0 & spell_id <= 2000000) %>% group_by(spell_id) %>% slice(which.min(EPISTART)) %>% ungroup()
apc_start2 <- spell_id_id %>% subset(spell_id > 2000000 & spell_id <= 4000000) %>% group_by(spell_id) %>% slice(which.min(EPISTART)) %>% ungroup()
apc_start3 <- spell_id_id %>% subset(spell_id > 4000000 & spell_id <= 6000000) %>% group_by(spell_id) %>% slice(which.min(EPISTART)) %>% ungroup()
apc_start4 <- spell_id_id %>% subset(spell_id > 6000000 & spell_id <= 8000000) %>% group_by(spell_id) %>% slice(which.min(EPISTART)) %>% ungroup()
apc_start5 <- spell_id_id %>% subset(spell_id > 8000000 & spell_id <= 10000000) %>% group_by(spell_id) %>% slice(which.min(EPISTART)) %>% ungroup()
apc_start6 <- spell_id_id %>% subset(spell_id > 10000000 & spell_id <= 12000000) %>% group_by(spell_id) %>% slice(which.min(EPISTART)) %>% ungroup()
apc_start7 <- spell_id_id %>% subset(spell_id > 12000000 & spell_id <= 14000000) %>% group_by(spell_id) %>% slice(which.min(EPISTART)) %>% ungroup()
apc_start8 <- spell_id_id %>% subset(spell_id > 14000000 & spell_id <= 16000000) %>% group_by(spell_id) %>% slice(which.min(EPISTART)) %>% ungroup()
apc_start9 <- spell_id_id %>% subset(spell_id > 16000000 & spell_id <= 18000000) %>% group_by(spell_id) %>% slice(which.min(EPISTART)) %>% ungroup()
apc_start10 <- spell_id_id %>% subset(spell_id > 18000000 & spell_id <= 20000000) %>% group_by(spell_id) %>% slice(which.min(EPISTART)) %>% ungroup()
apc_start11 <- spell_id_id %>% subset(spell_id > 20000000 & spell_id <= 22000000) %>% group_by(spell_id) %>% slice(which.min(EPISTART)) %>% ungroup()
apc_start12 <- spell_id_id %>% subset(spell_id > 22000000 & spell_id <= 24000000) %>% group_by(spell_id) %>% slice(which.min(EPISTART)) %>% ungroup()
apc_start13 <- spell_id_id %>% subset(spell_id > 24000000 & spell_id <= 26000000) %>% group_by(spell_id) %>% slice(which.min(EPISTART)) %>% ungroup()
apc_start14 <- spell_id_id %>% subset(spell_id > 26000000 & spell_id <= 28000000) %>% group_by(spell_id) %>% slice(which.min(EPISTART)) %>% ungroup()
apc_start15 <- spell_id_id %>% subset(spell_id > 28000000 & spell_id <= 30000000) %>% group_by(spell_id) %>% slice(which.min(EPISTART)) %>% ungroup()
apc_start16 <- spell_id_id %>% subset(spell_id > 30000000 & spell_id <= 32000000) %>% group_by(spell_id) %>% slice(which.min(EPISTART)) %>% ungroup()
apc_start17 <- spell_id_id %>% subset(spell_id > 32000000 & spell_id <= 34000000) %>% group_by(spell_id) %>% slice(which.min(EPISTART)) %>% ungroup()
apc_start18 <- spell_id_id %>% subset(spell_id > 34000000 & spell_id <= 36000000) %>% group_by(spell_id) %>% slice(which.min(EPISTART)) %>% ungroup()
apc_start19 <- spell_id_id %>% subset(spell_id > 36000000 & spell_id <= 38000000) %>% group_by(spell_id) %>% slice(which.min(EPISTART)) %>% ungroup()
apc_start20 <- spell_id_id %>% subset(spell_id > 38000000 & spell_id <= 40000000) %>% group_by(spell_id) %>% slice(which.min(EPISTART)) %>% ungroup()
apc_start21 <- spell_id_id %>% subset(spell_id > 40000000 & spell_id <= 42000000) %>% group_by(spell_id) %>% slice(which.min(EPISTART)) %>% ungroup()
apc_start22 <- spell_id_id %>% subset(spell_id > 42000000 & spell_id <= 44000000) %>% group_by(spell_id) %>% slice(which.min(EPISTART)) %>% ungroup()
apc_start23 <- spell_id_id %>% subset(spell_id > 44000000 & spell_id <= 46000000) %>% group_by(spell_id) %>% slice(which.min(EPISTART)) %>% ungroup()
apc_start24 <- spell_id_id %>% subset(spell_id > 46000000 & spell_id <= 48000000) %>% group_by(spell_id) %>% slice(which.min(EPISTART)) %>% ungroup()
apc_start25 <- spell_id_id %>% subset(spell_id > 48000000 & spell_id <= 50000000) %>% group_by(spell_id) %>% slice(which.min(EPISTART)) %>% ungroup()
apc_start26 <- spell_id_id %>% subset(spell_id > 50000000 & spell_id <= 52000000) %>% group_by(spell_id) %>% slice(which.min(EPISTART)) %>% ungroup()
apc_start27 <- spell_id_id %>% subset(spell_id > 52000000 & spell_id <= 54000000) %>% group_by(spell_id) %>% slice(which.min(EPISTART)) %>% ungroup()
apc_start28 <- spell_id_id %>% subset(spell_id > 54000000 & spell_id <= 56000000) %>% group_by(spell_id) %>% slice(which.min(EPISTART)) %>% ungroup()
apc_start29 <- spell_id_id %>% subset(spell_id > 56000000 & spell_id <= 58000000) %>% group_by(spell_id) %>% slice(which.min(EPISTART)) %>% ungroup()
apc_start30 <- spell_id_id %>% subset(spell_id > 58000000 & spell_id <= 60000000) %>% group_by(spell_id) %>% slice(which.min(EPISTART)) %>% ungroup()


## Get the maximum episode end date from all the episodes in each spell to use as the spellenddate
apc_end1 <- spell_id_id %>% subset(spell_id > 0 & spell_id <= 2000000) %>% group_by(spell_id) %>% slice(which.max(EPIEND)) %>% ungroup()
apc_end2 <- spell_id_id %>% subset(spell_id > 2000000 & spell_id <= 4000000) %>% group_by(spell_id) %>% slice(which.max(EPIEND)) %>% ungroup()
apc_end3 <- spell_id_id %>% subset(spell_id > 4000000 & spell_id <= 6000000) %>% group_by(spell_id) %>% slice(which.max(EPIEND)) %>% ungroup()
apc_end4 <- spell_id_id %>% subset(spell_id > 6000000 & spell_id <= 8000000) %>% group_by(spell_id) %>% slice(which.max(EPIEND)) %>% ungroup()
apc_end5 <- spell_id_id %>% subset(spell_id > 8000000 & spell_id <= 10000000) %>% group_by(spell_id) %>% slice(which.max(EPIEND)) %>% ungroup()
apc_end6 <- spell_id_id %>% subset(spell_id > 10000000 & spell_id <= 12000000) %>% group_by(spell_id) %>% slice(which.max(EPIEND)) %>% ungroup()
apc_end7 <- spell_id_id %>% subset(spell_id > 12000000 & spell_id <= 14000000) %>% group_by(spell_id) %>% slice(which.max(EPIEND)) %>% ungroup()
apc_end8 <- spell_id_id %>% subset(spell_id > 14000000 & spell_id <= 16000000) %>% group_by(spell_id) %>% slice(which.max(EPIEND)) %>% ungroup()
apc_end9 <- spell_id_id %>% subset(spell_id > 16000000 & spell_id <= 18000000) %>% group_by(spell_id) %>% slice(which.max(EPIEND)) %>% ungroup()
apc_end10 <- spell_id_id %>% subset(spell_id > 18000000 & spell_id <= 20000000) %>% group_by(spell_id) %>% slice(which.max(EPIEND)) %>% ungroup()
apc_end11 <- spell_id_id %>% subset(spell_id > 20000000 & spell_id <= 22000000) %>% group_by(spell_id) %>% slice(which.max(EPIEND)) %>% ungroup()
apc_end12 <- spell_id_id %>% subset(spell_id > 22000000 & spell_id <= 24000000) %>% group_by(spell_id) %>% slice(which.max(EPIEND)) %>% ungroup()
apc_end13 <- spell_id_id %>% subset(spell_id > 24000000 & spell_id <= 26000000) %>% group_by(spell_id) %>% slice(which.max(EPIEND)) %>% ungroup()
apc_end14 <- spell_id_id %>% subset(spell_id > 26000000 & spell_id <= 28000000) %>% group_by(spell_id) %>% slice(which.max(EPIEND)) %>% ungroup()
apc_end15 <- spell_id_id %>% subset(spell_id > 28000000 & spell_id <= 30000000) %>% group_by(spell_id) %>% slice(which.max(EPIEND)) %>% ungroup()
apc_end16 <- spell_id_id %>% subset(spell_id > 30000000 & spell_id <= 32000000) %>% group_by(spell_id) %>% slice(which.max(EPIEND)) %>% ungroup()
apc_end17 <- spell_id_id %>% subset(spell_id > 32000000 & spell_id <= 34000000) %>% group_by(spell_id) %>% slice(which.max(EPIEND)) %>% ungroup()
apc_end18 <- spell_id_id %>% subset(spell_id > 34000000 & spell_id <= 36000000) %>% group_by(spell_id) %>% slice(which.max(EPIEND)) %>% ungroup()
apc_end19 <- spell_id_id %>% subset(spell_id > 36000000 & spell_id <= 38000000) %>% group_by(spell_id) %>% slice(which.max(EPIEND)) %>% ungroup()
apc_end20 <- spell_id_id %>% subset(spell_id > 38000000 & spell_id <= 40000000) %>% group_by(spell_id) %>% slice(which.max(EPIEND)) %>% ungroup()
apc_end21 <- spell_id_id %>% subset(spell_id > 40000000 & spell_id <= 42000000) %>% group_by(spell_id) %>% slice(which.max(EPIEND)) %>% ungroup()
apc_end22 <- spell_id_id %>% subset(spell_id > 42000000 & spell_id <= 44000000) %>% group_by(spell_id) %>% slice(which.max(EPIEND)) %>% ungroup()
apc_end23 <- spell_id_id %>% subset(spell_id > 44000000 & spell_id <= 46000000) %>% group_by(spell_id) %>% slice(which.max(EPIEND)) %>% ungroup()
apc_end24 <- spell_id_id %>% subset(spell_id > 46000000 & spell_id <= 48000000) %>% group_by(spell_id) %>% slice(which.max(EPIEND)) %>% ungroup()
apc_end25 <- spell_id_id %>% subset(spell_id > 48000000 & spell_id <= 50000000) %>% group_by(spell_id) %>% slice(which.max(EPIEND)) %>% ungroup()
apc_end26 <- spell_id_id %>% subset(spell_id > 50000000 & spell_id <= 52000000) %>% group_by(spell_id) %>% slice(which.max(EPIEND)) %>% ungroup()
apc_end27 <- spell_id_id %>% subset(spell_id > 52000000 & spell_id <= 54000000) %>% group_by(spell_id) %>% slice(which.max(EPIEND)) %>% ungroup()
apc_end28 <- spell_id_id %>% subset(spell_id > 54000000 & spell_id <= 56000000) %>% group_by(spell_id) %>% slice(which.max(EPIEND)) %>% ungroup()
apc_end29 <- spell_id_id %>% subset(spell_id > 56000000 & spell_id <= 58000000) %>% group_by(spell_id) %>% slice(which.max(EPIEND)) %>% ungroup()
apc_end30 <- spell_id_id %>% subset(spell_id > 58000000 & spell_id <= 60000000) %>% group_by(spell_id) %>% slice(which.max(EPIEND)) %>% ungroup()

## Run through each set of id, start and end data sets and link them together
## Set 1
apc1 <- apc1 %>% select(spell_id, id)
apc_start1 <- apc_start1 %>% select(spell_id, EPISTART) %>% rename("spellstartdate" = "EPISTART")
apc_end1 <- apc_end1 %>% select(spell_id, EPIEND) %>% rename("spellenddate" = "EPIEND")
j1 <- merge(x = apc1, y = apc_start1, by = "spell_id", all.x = TRUE)
apc_all_1 <- merge(x = j1, y = apc_end1, by = "spell_id", all.x = TRUE)

## Set 2
apc2 <- apc2 %>% select(spell_id, id)
apc_start2 <- apc_start2 %>% select(spell_id, EPISTART) %>% rename("spellstartdate" = "EPISTART")
apc_end2 <- apc_end2 %>% select(spell_id, EPIEND) %>% rename("spellenddate" = "EPIEND")
j1 <- merge(x = apc2, y = apc_start2, by = "spell_id", all.x = TRUE)
apc_all_2 <- merge(x = j1, y = apc_end2, by = "spell_id", all.x = TRUE)

## Set 3
apc3 <- apc3 %>% select(spell_id, id)
apc_start3 <- apc_start3 %>% select(spell_id, EPISTART) %>% rename("spellstartdate" = "EPISTART")
apc_end3 <- apc_end3 %>% select(spell_id, EPIEND) %>% rename("spellenddate" = "EPIEND")
j1 <- merge(x = apc3, y = apc_start3, by = "spell_id", all.x = TRUE)
apc_all_3 <- merge(x = j1, y = apc_end3, by = "spell_id", all.x = TRUE)

## Set 4
apc4 <- apc4 %>% select(spell_id, id)
apc_start4 <- apc_start4 %>% select(spell_id, EPISTART) %>% rename("spellstartdate" = "EPISTART")
apc_end4 <- apc_end4 %>% select(spell_id, EPIEND) %>% rename("spellenddate" = "EPIEND")
j1 <- merge(x = apc4, y = apc_start4, by = "spell_id", all.x = TRUE)
apc_all_4 <- merge(x = j1, y = apc_end4, by = "spell_id", all.x = TRUE)

## Set 5
apc5 <- apc5 %>% select(spell_id, id)
apc_start5 <- apc_start5 %>% select(spell_id, EPISTART) %>% rename("spellstartdate" = "EPISTART")
apc_end5 <- apc_end5 %>% select(spell_id, EPIEND) %>% rename("spellenddate" = "EPIEND")
j1 <- merge(x = apc5, y = apc_start5, by = "spell_id", all.x = TRUE)
apc_all_5 <- merge(x = j1, y = apc_end5, by = "spell_id", all.x = TRUE)

## Set 6
apc6 <- apc6 %>% select(spell_id, id)
apc_start6 <- apc_start6 %>% select(spell_id, EPISTART) %>% rename("spellstartdate" = "EPISTART")
apc_end6 <- apc_end6 %>% select(spell_id, EPIEND) %>% rename("spellenddate" = "EPIEND")
j1 <- merge(x = apc6, y = apc_start6, by = "spell_id", all.x = TRUE)
apc_all_6 <- merge(x = j1, y = apc_end6, by = "spell_id", all.x = TRUE)

## Set 7
apc7 <- apc7 %>% select(spell_id, id)
apc_start7 <- apc_start7 %>% select(spell_id, EPISTART) %>% rename("spellstartdate" = "EPISTART")
apc_end7 <- apc_end7 %>% select(spell_id, EPIEND) %>% rename("spellenddate" = "EPIEND")
j1 <- merge(x = apc7, y = apc_start7, by = "spell_id", all.x = TRUE)
apc_all_7 <- merge(x = j1, y = apc_end7, by = "spell_id", all.x = TRUE)

## Set 8
apc8 <- apc8 %>% select(spell_id, id)
apc_start8 <- apc_start8 %>% select(spell_id, EPISTART) %>% rename("spellstartdate" = "EPISTART")
apc_end8 <- apc_end8 %>% select(spell_id, EPIEND) %>% rename("spellenddate" = "EPIEND")
j1 <- merge(x = apc8, y = apc_start8, by = "spell_id", all.x = TRUE)
apc_all_8 <- merge(x = j1, y = apc_end8, by = "spell_id", all.x = TRUE)

## Set 9
apc9 <- apc9 %>% select(spell_id, id)
apc_start9 <- apc_start9 %>% select(spell_id, EPISTART) %>% rename("spellstartdate" = "EPISTART")
apc_end9 <- apc_end9 %>% select(spell_id, EPIEND) %>% rename("spellenddate" = "EPIEND")
j1 <- merge(x = apc9, y = apc_start9, by = "spell_id", all.x = TRUE)
apc_all_9 <- merge(x = j1, y = apc_end9, by = "spell_id", all.x = TRUE)

## Set 10
apc10 <- apc10 %>% select(spell_id, id)
apc_start10 <- apc_start10 %>% select(spell_id, EPISTART) %>% rename("spellstartdate" = "EPISTART")
apc_end10 <- apc_end10 %>% select(spell_id, EPIEND) %>% rename("spellenddate" = "EPIEND")
j1 <- merge(x = apc10, y = apc_start10, by = "spell_id", all.x = TRUE)
apc_all_10 <- merge(x = j1, y = apc_end10, by = "spell_id", all.x = TRUE)

## Set 11
apc11 <- apc11 %>% select(spell_id, id)
apc_start11 <- apc_start11 %>% select(spell_id, EPISTART) %>% rename("spellstartdate" = "EPISTART")
apc_end11 <- apc_end11 %>% select(spell_id, EPIEND) %>% rename("spellenddate" = "EPIEND")
j1 <- merge(x = apc11, y = apc_start11, by = "spell_id", all.x = TRUE)
apc_all_11 <- merge(x = j1, y = apc_end11, by = "spell_id", all.x = TRUE)

## Set 12
apc12 <- apc12 %>% select(spell_id, id)
apc_start12 <- apc_start12 %>% select(spell_id, EPISTART) %>% rename("spellstartdate" = "EPISTART")
apc_end12 <- apc_end12 %>% select(spell_id, EPIEND) %>% rename("spellenddate" = "EPIEND")
j1 <- merge(x = apc12, y = apc_start12, by = "spell_id", all.x = TRUE)
apc_all_12 <- merge(x = j1, y = apc_end12, by = "spell_id", all.x = TRUE)

## Set 13
apc13 <- apc13 %>% select(spell_id, id)
apc_start13 <- apc_start13 %>% select(spell_id, EPISTART) %>% rename("spellstartdate" = "EPISTART")
apc_end13 <- apc_end13 %>% select(spell_id, EPIEND) %>% rename("spellenddate" = "EPIEND")
j1 <- merge(x = apc13, y = apc_start13, by = "spell_id", all.x = TRUE)
apc_all_13 <- merge(x = j1, y = apc_end13, by = "spell_id", all.x = TRUE)

## Set 14
apc14 <- apc14 %>% select(spell_id, id)
apc_start14 <- apc_start14 %>% select(spell_id, EPISTART) %>% rename("spellstartdate" = "EPISTART")
apc_end14 <- apc_end14 %>% select(spell_id, EPIEND) %>% rename("spellenddate" = "EPIEND")
j1 <- merge(x = apc14, y = apc_start14, by = "spell_id", all.x = TRUE)
apc_all_14 <- merge(x = j1, y = apc_end14, by = "spell_id", all.x = TRUE)

## Set 15
apc15 <- apc15 %>% select(spell_id, id)
apc_start15 <- apc_start15 %>% select(spell_id, EPISTART) %>% rename("spellstartdate" = "EPISTART")
apc_end15 <- apc_end15 %>% select(spell_id, EPIEND) %>% rename("spellenddate" = "EPIEND")
j1 <- merge(x = apc15, y = apc_start15, by = "spell_id", all.x = TRUE)
apc_all_15 <- merge(x = j1, y = apc_end15, by = "spell_id", all.x = TRUE)

## Set 16
apc16 <- apc16 %>% select(spell_id, id)
apc_start16 <- apc_start16 %>% select(spell_id, EPISTART) %>% rename("spellstartdate" = "EPISTART")
apc_end16 <- apc_end16 %>% select(spell_id, EPIEND) %>% rename("spellenddate" = "EPIEND")
j1 <- merge(x = apc16, y = apc_start16, by = "spell_id", all.x = TRUE)
apc_all_16 <- merge(x = j1, y = apc_end16, by = "spell_id", all.x = TRUE)

## Set 17
apc17 <- apc17 %>% select(spell_id, id)
apc_start17 <- apc_start17 %>% select(spell_id, EPISTART) %>% rename("spellstartdate" = "EPISTART")
apc_end17 <- apc_end17 %>% select(spell_id, EPIEND) %>% rename("spellenddate" = "EPIEND")
j1 <- merge(x = apc17, y = apc_start17, by = "spell_id", all.x = TRUE)
apc_all_17 <- merge(x = j1, y = apc_end17, by = "spell_id", all.x = TRUE)

## Set 18
apc18 <- apc18 %>% select(spell_id, id)
apc_start18 <- apc_start18 %>% select(spell_id, EPISTART) %>% rename("spellstartdate" = "EPISTART")
apc_end18 <- apc_end18 %>% select(spell_id, EPIEND) %>% rename("spellenddate" = "EPIEND")
j1 <- merge(x = apc18, y = apc_start18, by = "spell_id", all.x = TRUE)
apc_all_18 <- merge(x = j1, y = apc_end18, by = "spell_id", all.x = TRUE)

## Set 19
apc19 <- apc19 %>% select(spell_id, id)
apc_start19 <- apc_start19 %>% select(spell_id, EPISTART) %>% rename("spellstartdate" = "EPISTART")
apc_end19 <- apc_end19 %>% select(spell_id, EPIEND) %>% rename("spellenddate" = "EPIEND")
j1 <- merge(x = apc19, y = apc_start19, by = "spell_id", all.x = TRUE)
apc_all_19 <- merge(x = j1, y = apc_end19, by = "spell_id", all.x = TRUE)

## Set 20
apc20 <- apc20 %>% select(spell_id, id)
apc_start20 <- apc_start20 %>% select(spell_id, EPISTART) %>% rename("spellstartdate" = "EPISTART")
apc_end20 <- apc_end20 %>% select(spell_id, EPIEND) %>% rename("spellenddate" = "EPIEND")
j1 <- merge(x = apc20, y = apc_start20, by = "spell_id", all.x = TRUE)
apc_all_20 <- merge(x = j1, y = apc_end20, by = "spell_id", all.x = TRUE)

## Set 21
apc21 <- apc21 %>% select(spell_id, id)
apc_start21 <- apc_start21 %>% select(spell_id, EPISTART) %>% rename("spellstartdate" = "EPISTART")
apc_end21 <- apc_end21 %>% select(spell_id, EPIEND) %>% rename("spellenddate" = "EPIEND")
j1 <- merge(x = apc21, y = apc_start21, by = "spell_id", all.x = TRUE)
apc_all_21 <- merge(x = j1, y = apc_end21, by = "spell_id", all.x = TRUE)

## Set 22
apc22 <- apc22 %>% select(spell_id, id)
apc_start22 <- apc_start22 %>% select(spell_id, EPISTART) %>% rename("spellstartdate" = "EPISTART")
apc_end22 <- apc_end22 %>% select(spell_id, EPIEND) %>% rename("spellenddate" = "EPIEND")
j1 <- merge(x = apc22, y = apc_start22, by = "spell_id", all.x = TRUE)
apc_all_22 <- merge(x = j1, y = apc_end22, by = "spell_id", all.x = TRUE)

## Set 23
apc23 <- apc23 %>% select(spell_id, id)
apc_start23 <- apc_start23 %>% select(spell_id, EPISTART) %>% rename("spellstartdate" = "EPISTART")
apc_end23 <- apc_end23 %>% select(spell_id, EPIEND) %>% rename("spellenddate" = "EPIEND")
j1 <- merge(x = apc23, y = apc_start23, by = "spell_id", all.x = TRUE)
apc_all_23 <- merge(x = j1, y = apc_end23, by = "spell_id", all.x = TRUE)

## Set 24
apc24 <- apc24 %>% select(spell_id, id)
apc_start24 <- apc_start24 %>% select(spell_id, EPISTART) %>% rename("spellstartdate" = "EPISTART")
apc_end24 <- apc_end24 %>% select(spell_id, EPIEND) %>% rename("spellenddate" = "EPIEND")
j1 <- merge(x = apc24, y = apc_start24, by = "spell_id", all.x = TRUE)
apc_all_24 <- merge(x = j1, y = apc_end24, by = "spell_id", all.x = TRUE)

## Set 25
apc25 <- apc25 %>% select(spell_id, id)
apc_start25 <- apc_start25 %>% select(spell_id, EPISTART) %>% rename("spellstartdate" = "EPISTART")
apc_end25 <- apc_end25 %>% select(spell_id, EPIEND) %>% rename("spellenddate" = "EPIEND")
j1 <- merge(x = apc25, y = apc_start25, by = "spell_id", all.x = TRUE)
apc_all_25 <- merge(x = j1, y = apc_end25, by = "spell_id", all.x = TRUE)

## Set 26
apc26 <- apc26 %>% select(spell_id, id)
apc_start26 <- apc_start26 %>% select(spell_id, EPISTART) %>% rename("spellstartdate" = "EPISTART")
apc_end26 <- apc_end26 %>% select(spell_id, EPIEND) %>% rename("spellenddate" = "EPIEND")
j1 <- merge(x = apc26, y = apc_start26, by = "spell_id", all.x = TRUE)
apc_all_26 <- merge(x = j1, y = apc_end26, by = "spell_id", all.x = TRUE)

## Set 27
apc27 <- apc27 %>% select(spell_id, id)
apc_start27 <- apc_start27 %>% select(spell_id, EPISTART) %>% rename("spellstartdate" = "EPISTART")
apc_end27 <- apc_end27 %>% select(spell_id, EPIEND) %>% rename("spellenddate" = "EPIEND")
j1 <- merge(x = apc27, y = apc_start27, by = "spell_id", all.x = TRUE)
apc_all_27 <- merge(x = j1, y = apc_end27, by = "spell_id", all.x = TRUE)

## Set 28
apc28 <- apc28 %>% select(spell_id, id)
apc_start28 <- apc_start28 %>% select(spell_id, EPISTART) %>% rename("spellstartdate" = "EPISTART")
apc_end28 <- apc_end28 %>% select(spell_id, EPIEND) %>% rename("spellenddate" = "EPIEND")
j1 <- merge(x = apc28, y = apc_start28, by = "spell_id", all.x = TRUE)
apc_all_28 <- merge(x = j1, y = apc_end28, by = "spell_id", all.x = TRUE)

## Set 29
apc29 <- apc29 %>% select(spell_id, id)
apc_start29 <- apc_start29 %>% select(spell_id, EPISTART) %>% rename("spellstartdate" = "EPISTART")
apc_end29 <- apc_end29 %>% select(spell_id, EPIEND) %>% rename("spellenddate" = "EPIEND")
j1 <- merge(x = apc29, y = apc_start29, by = "spell_id", all.x = TRUE)
apc_all_29 <- merge(x = j1, y = apc_end29, by = "spell_id", all.x = TRUE)

## Set 30
apc30 <- apc30 %>% select(spell_id, id)
apc_start30 <- apc_start30 %>% select(spell_id, EPISTART) %>% rename("spellstartdate" = "EPISTART")
apc_end30 <- apc_end30 %>% select(spell_id, EPIEND) %>% rename("spellenddate" = "EPIEND")
j1 <- merge(x = apc30, y = apc_start30, by = "spell_id", all.x = TRUE)
apc_all_30 <- merge(x = j1, y = apc_end30, by = "spell_id", all.x = TRUE)



## Append all the sets together
apc_all_all <- rbind(apc_all_1, apc_all_2, apc_all_3, apc_all_4, apc_all_5, apc_all_6, apc_all_7, apc_all_8, apc_all_9, apc_all_10, 
                     apc_all_11, apc_all_12, apc_all_13, apc_all_14, apc_all_15, apc_all_16, apc_all_17
                     , apc_all_18
                     , apc_all_19
                     , apc_all_20
                     , apc_all_21
                     , apc_all_22
                     , apc_all_23
                     , apc_all_24
                     , apc_all_25
                     , apc_all_26
                     , apc_all_27
                     , apc_all_28
                     , apc_all_29
                     , apc_all_30
                     )

rm(apc_all_1, apc_all_2, apc_all_3, apc_all_4, apc_all_5, apc_all_6, apc_all_7, apc_all_8, apc_all_9, apc_all_10, 
     apc_all_11, apc_all_12, apc_all_13, apc_all_14, apc_all_15, apc_all_16, apc_all_17                     
   , apc_all_18
   , apc_all_19
   , apc_all_20
   , apc_all_21
   , apc_all_22
   , apc_all_23
   , apc_all_24
   , apc_all_25
   , apc_all_26
   , apc_all_27
   , apc_all_28
   , apc_all_29
   , apc_all_30)

gc()
rm(spell_id_id)
rm(j1)
gc()

## Create the base APCS table
apcs <- subset(apc, id %in% apc_all_all$id)

s3saveRDS(x = apcs
          ,object = 'all_HES/apcs_bu.rds'
          ,bucket = project_bucket
          ,multipart=TRUE)

rm(apcs)
gc()

s3saveRDS(x = apc_all_all
          ,object = 'all_HES/apc_all_bu.rds'
          ,bucket = project_bucket
          ,multipart=TRUE)


rm(list=ls())  # Clear work-space
gc()

# get project bucket again
proj_bucket <- ''  # Set project directory 

## Read the apcs data back in
apcs <- s3readRDS(object = 'all_HES/apcs_bu.rds',
                          bucket = project_bucket)

## Read in the spell derived variables back in
apc_all_all <- s3readRDS(object = 'all_HES/apc_all_bu.rds',
                  bucket = project_bucket)

## Merge on the spell fields
apcs2 <- merge(x = apcs, y = apc_all_all[, c("spell_id", "spellstartdate", "spellenddate")], by = "spell_id", all.x = TRUE)

rm(apcs)
rm(apc_all_all)
gc()

## Get all the other data requested for the analysis data set from the main APC data using the SUSRECID 
## 2021 data
APC_location <- '' #respecify location of 2021 data

open_dataset(file.path(APC_location)) %>% 
  select(SUSRECID, ADMINCAT, CLASSPAT, DIAG_01, DISMETH, DUPLICATE, ELECDATE, ENCRYPTED_HESID, ETHNIC5, ETHNOS,
         GPPRAC, INTMANIG, LSOA11, MAINSPEF, OPDATE_01, OPERTN_01, PROCODE3, SEX, STARTAGE, STARTAGE_CALC, TRETSPEF) %>%
  filter(SUSRECID %in% apcs2$SUSRECID) %>% #
  collect() %>%
  #  top_n(10000) %>% # only 10k for code testing
  setDT() -> apc_21

## 2022 data
APC_location <- '' #respecify location of 2022 data 

open_dataset(file.path(APC_location)) %>% 
  select(SUSRECID, ADMINCAT, CLASSPAT, DIAG_01, DISMETH, DUPLICATE, ELECDATE, ENCRYPTED_HESID, ETHNIC5, ETHNOS,
         GPPRAC, INTMANIG, LSOA11, MAINSPEF, OPDATE_01, OPERTN_01, PROCODE3, SEX, STARTAGE, STARTAGE_CALC, TRETSPEF) %>%
  filter(SUSRECID %in% apcs2$SUSRECID) %>% #
  collect() %>%
  #  top_n(10000) %>% # only 10k for code testing
  setDT() -> apc_22

APC_location <-'' #respecify location of 2018-2020 data

## 2018-2022 data
open_dataset(file.path(APC_location)) %>% 
  select(SUSRECID, ADMINCAT, CLASSPAT, DIAG_01, DISMETH, DUPLICATE, ELECDATE, ENCRYPTED_HESID, ETHNIC5, ETHNOS,
         GPPRAC, INTMANIG, LSOA11, MAINSPEF, OPDATE_01, OPERTN_01, PROCODE3, SEX, STARTAGE, STARTAGE_CALC, TRETSPEF) %>%
  filter(SUSRECID %in% apcs2$SUSRECID) %>% #
  collect() %>%
  #  top_n(10000) %>% # only 10k for code testing
  setDT() -> apc_18_to_20

## Bind the 2018-2022 data together
apc_missing_vars <- rbind(apc_18_to_20, apc_21, apc_22)

rm(apc_18_to_20)
rm(apc_21)
rm(apc_22)

gc()

## Merge the missing variables to the APCS table
apcs3 <- merge(x = apcs2, y = apc_missing_vars, by = "SUSRECID", all.x = TRUE)

s3saveRDS(x = apcs3
          ,object = 'all_HES/apcs_no_diag.rds'
          ,bucket = proj_bucket
          ,multipart=TRUE)

## Read in apcs3 if needed from bucket

# apcs3 <- ''

## Recreate the id variable (TOKEN_PERSON_ID_EPIKEY) in the APCS data - needed to link to diagnosis and procedure 'long' tables
apcs3 <- apcs3 %>% rename("apc_order_id" = "id") %>% 
  mutate(apcs3, id = paste0(TOKEN_PERSON_ID, "_", EPIKEY, sep = ""))

rm(apcs4)
gc()

ids <- apcs3$id

## Diagnosis data attempt all
APC_location <- '' #respecify location of data

open_dataset(APC_location) %>% 
  # head(10) %>% # only 10 for code testing
  select(ICD10code, id) %>%
  filter(id %in% ids) %>% #
  collect() %>%
  # top_n(10000) %>% # only 10k for code testing
  setDT() -> diag

## Get the data from the split tables
## Diagnosis data 2021-2022
APC_location <-'' #respecify location of 2021-22 data

open_dataset(APC_location) %>% 
  # head(10) %>% # only 10 for code testing
  select(ICD10code, id) %>%
  filter(id %in% ids) %>% #
  collect() %>%
# top_n(10000) %>% # only 10k for code testing
  setDT() -> diag_2122

## 2018-2020
APC_location <- '' #respecify location of 2018 data

open_dataset(file.path(APC_location)) %>%
  # head(100) %>% 
  select(ICD10code, id) %>%
  filter(id %in% ids) %>% #
  collect() %>%
  #  top_n(10000) %>% # only 10k for code testing
  setDT() -> diag_18

APC_location <- '' #respecify location of 2019 data

open_dataset(file.path(APC_location)) %>%
  select(ICD10code, id) %>%
  filter(id %in% ids) %>% #
  collect() %>%
  #  top_n(10000) %>% # only 10k for code testing
  setDT() -> diag_19

APC_location <- '' #respecify location of 2020 data

open_dataset(file.path(APC_location)) %>%
  select(ICD10code, id) %>%
  filter(id %in% ids) %>% #
  collect() %>%
  #  top_n(10000) %>% # only 10k for code testing
  setDT() -> diag_20



# Backup some data
s3saveRDS(x = apcs3
          ,object = 'all_HES/apcs_no_diag.rds'
          ,bucket = proj_bucket
          ,multipart=TRUE)

s3saveRDS(x = diag
          ,object = 'all_HES/diag_sel.rds'
          ,bucket = proj_bucket
          ,multipart=TRUE)

s3saveRDS(x = diag_2122
          ,object = 'all_HES/diag_sel_2122.rds'
          ,bucket = proj_bucket
          ,multipart=TRUE)

s3saveRDS(x = diag_18
          ,object = 'all_HES/diag_sel_18.rds'
          ,bucket = proj_bucket
          ,multipart=TRUE)

s3saveRDS(x = diag_19
          ,object = 'all_HES/diag_sel_19.rds'
          ,bucket = proj_bucket
          ,multipart=TRUE)

s3saveRDS(x = diag_20
          ,object = 'all_HES/diag_sel_20.rds'
          ,bucket = proj_bucket
          ,multipart=TRUE)

## Read the APCS data back in
apcs3 <- s3readRDS(object = 'all_HES/apcs_no_diag.rds',
                  bucket = proj_bucket)

## Filter the records by admission method and patient age  to make data set more manageable
apcs3 <- apcs3 %>% filter(ADMIMETH %in% c(11,12,13))
apcs3 <- apcs3 %>% filter(STARTAGE >= 17 & STARTAGE < 7000)

s3saveRDS(x = apcs3
          ,object = 'all_HES/apcs_17_admmthd.rds'
          ,bucket = proj_bucket
          ,multipart=TRUE)

## Procedures data was read in using arrow package here and saved as RDS file
APC_location <- '' #respecify location of data
  
open_dataset(file.path(APC_location)) %>% 
  filter(id %in% ids) %>% #
  collect() %>%
  #  top_n(10000) %>% # only 10k for code testing
  setDT() -> oper

s3saveRDS(x = oper
          ,object = 'all_HES/apcs_oper.rds'
          ,bucket = proj_bucket
          ,multipart=TRUE)

