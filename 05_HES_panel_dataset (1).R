## ==========================================================================##
# Project: GIRFT Elective Hubs Evaluation

# Team: Improvement Analytics Unit (IAU) at the Health Foundation

# Script: 05_HES_panel_dataset.R

# Corresponding author:Freya Tracey (freya.tracey@health.org.uk)

# Description:
# Make a panel dataset for each trust for each month. 

# Dependencies:
# '00_preamble.R'
# '04_hvlc_specialty_recipes_hes.R'

# Inputs:
# HES data set with HVLC specialties flagged, as from 04 script

# Outputs:
# HES panel data set, ready for gsynth process 

# Notes: To use, need to adjust locations of R scripts and csv files

## ==========================================================================##

## loading scripts and setting buckets -------
library(gtsummary) #not in preamble
library(gt) #not in preamble
library (arrow) #not in preamble

project_bucket <- '' # assign project directory

# read in the dataset 
eh_hes <- s3read_using(read_parquet, object = "hes_hvlc.parquet", bucket = project_bucket) 

# add in deprivation quintile
imd <- s3read_using(read_csv, object = "File_7_-_All_IoD2019_Scores__Ranks__Deciles_and_Population_Denominators_3.csv", bucket=project_bucket)%>% # source from gov.uk website
  select (1,7) %>%
  rename (imd_decile = "Index of Multiple Deprivation (IMD) Decile (where 1 is most deprived 10% of LSOAs)") %>%
  rename (LSOA11 = "LSOA code (2011)") %>%
  as.data.frame ()


eh_hes <- eh_hes %>%
  left_join (imd, by = "LSOA11")


#read in trust categories 

trust_look_up<-s3read_using(read_csv, object = "Trust look up for ITS 20230703.csv", bucket = project_bucket) %>% # this csv is an 
  #internal document which details which trusts have hubs, type of hub and hub start date. to replicate, analyst will need a csv containing this information
  select (trust_code, elective_hub_cat) %>%
  as.data.frame ()

trust_look_up <- trust_look_up %>%
  distinct() %>%
  rename (PROCODE3 = trust_code)


#read in trust demographic information to calculate overall catchment population, the original doc has catchment by age group 
#and ethnicity but have limited this to just overall catchment, option to add in if wanted 
#trust_ref_updated.csv is an internal document which includes publicly available information from Office for Health Improvement and Disparities
# on trust catchment populations 

trust_catchment <-s3read_using(read_csv, object = 'trust_ref_updated.csv', bucket =project_bucket) %>%
  as.data.frame()


trust_catchment <- trust_catchment%>%
  dplyr::rename(trust_code = procode,
                sex_male_pct = sex_m_cat,
                age_0_pct = age_00_04_cat,
                age_5_pct = age_05_14_cat,
                age_15_pct = age_15_24_cat,
                age_25_pct = age_25_64_cat,
                age_65_pct = age_65_74_cat,
                age_75_pct = age_75_plus_cat,
                white_pct = white,
                imd_decile = rank_1_is_most_deprived
                
  )

trust_catchment <- trust_catchment%>%
  rowwise () %>%
  mutate (age_65_plus_pct = sum(c_across (age_65_pct: age_75_pct), na.rm = TRUE ))%>% # want to know % aged 65+ 
  select(trust_code,year,catchment_cat, sex_male_pct, white_pct, age_65_plus_pct, imd_decile   ) %>%
  filter(year %in% c(2018,2019,2020))  %>%
  as.data.frame()

##count overall elective activity ---- 
eh_hes_activity <-eh_hes %>%
  separate(ADMIDATE,into=c("year", "month", "day"), sep="-") %>%
  mutate(year_month=paste0(year,'_',month)) %>%
  group_by(PROCODE3,year_month) %>% 
  summarise(n_activity=n()) %>%
  mutate(year_month2=year_month) %>%
  separate(year_month, c('year','month'), sep='_') %>%
  select(-month) %>%
  mutate(year2=as.integer(case_when(year %in% c(2021,2022)~'2020',
                                    TRUE ~ year))) %>%
  select(-year) %>%
  left_join(trust_catchment, by=c('PROCODE3'='trust_code', 'year2'='year')) %>%
  mutate(el_rate=n_activity/catchment_cat*1000) %>%
  select(-year2)
  

##los for overall elective activity ----

eh_hes_los <- eh_hes %>%
  mutate (los = as.numeric (difftime (DISDATE,ADMIDATE, units = "days" )))

eh_hes_los <- eh_hes_los %>%
  filter (los >=0) 

eh_hes_los <- eh_hes_los %>%
  mutate (elix_2_or_more = nr_elix_h36 >=2) # make a new variable with true false if person has 2 or more elixhauser conditions in the previous 36 months 

percentile_999 <- quantile (eh_hes_los $ los, probs = 0.999)
print (percentile_999) #37 so will cap at this

eh_hes_los <- eh_hes_los %>%
  mutate (los_new = ifelse (los >37,37, los)) # create a new variable which takes los value if its below 37 or makes the los 37 if greater than 37

eh_hes_los<-eh_hes_los %>%
  separate(ADMIDATE,into=c("year", "month", "day"), sep="-") %>%
  mutate(year_month=paste0(year,'_',month)) %>%
  filter(year_month>='2018_01') %>%
  group_by(PROCODE3,year_month) %>% 
  summarise(n=n(),
            los_avg=mean(los_new[los_new > 0]), # updated to mean and calculated from los_new which holds los values of 0-37, where true value >37 is replaced as 37
            dc=length(los[los==0])/length(los),
            sex_male_pct_ec=sum(SEX==1)/length(SEX), 
            age_65_plus_pct_ec = sum(STARTAGE >=65)/length (STARTAGE),
            white_pct_ec = sum (ETHNOS %in% c("A", "B", "C"))/ length (ETHNOS),
            quintile_1_pct_ec = sum(imd_decile %in% 1:2 / n()),
            quintile_2_pct_ec = sum(imd_decile %in% 3:4 / n()),
            quintile_3_pct_ec = sum(imd_decile %in% 5:6 / n()),
            quintile_4_pct_ec = sum(imd_decile %in% 7:8 / n()),
            quintile_5_pct_ec = sum(imd_decile %in% 9:10 / n()),
            comorb_pct = sum (elix_2_or_more == TRUE)/ length (elix_2_or_more) # proportion with 2 or more comorbidities 
            
            )


hes_data_set <- eh_hes_activity %>%
  full_join(eh_hes_los, by =c('PROCODE3', "year_month2" = "year_month"))%>%
  select (-n) 

## HVLC specialties   ---------
# uses same code as above to calculate activity but with additional filter step
#ort
eh_hes_hvlc_spec_ort<-eh_hes %>%
  separate(ADMIDATE,into=c("year", "month", "day"), sep="-") %>%
  mutate(year_month=paste0(year,'_',month)) %>%
  group_by(PROCODE3,year_month) %>% 
  filter(ort == TRUE) %>%
  summarise(n_activity_ort =n() ) %>%
  mutate(year_month2=year_month) %>%
  separate(year_month, c('year','month'), sep='_') %>%
  select(-month) %>%
  mutate(year2=as.integer(case_when(year %in% c(2021,2022)~'2020',
                                    TRUE ~ year))) %>%
  select(-year) %>%
  left_join(trust_catchment, by=c('PROCODE3'='trust_code', 'year2'='year')) %>%
  mutate(rate_ort=n_activity_ort/catchment_cat*1000) 

#ent
eh_hes_hvlc_spec_ent<-eh_hes %>%
  separate(ADMIDATE,into=c("year", "month", "day"), sep="-") %>%
  mutate(year_month=paste0(year,'_',month)) %>%
  group_by(PROCODE3,year_month) %>% 
  filter(ent == TRUE) %>%
  summarise(n_activity_ent =n() ) %>%
  mutate(year_month2=year_month) %>%
  separate(year_month, c('year','month'), sep='_') %>%
  select(-month) %>%
  mutate(year2=as.integer(case_when(year %in% c(2021,2022)~'2020',
                                    TRUE ~ year))) %>%
  select(-year) %>%
  left_join(trust_catchment, by=c('PROCODE3'='trust_code', 'year2'='year')) %>%
  mutate(rate_ent=n_activity_ent/catchment_cat*1000)

#gs
eh_hes_hvlc_spec_gs<-eh_hes %>%
  separate(ADMIDATE,into=c("year", "month", "day"), sep="-") %>%
  mutate(year_month=paste0(year,'_',month)) %>%
  group_by(PROCODE3,year_month) %>% 
  filter(gs == TRUE) %>%
  summarise(n_activity_gs =n() ) %>%
  mutate(year_month2=year_month) %>%
  separate(year_month, c('year','month'), sep='_') %>%
  select(-month) %>%
  mutate(year2=as.integer(case_when(year %in% c(2021,2022)~'2020',
                                    TRUE ~ year))) %>%
  select(-year) %>%
  left_join(trust_catchment, by=c('PROCODE3'='trust_code', 'year2'='year')) %>%
  mutate(rate_gs=n_activity_gs/catchment_cat*1000)

#gm
eh_hes_hvlc_spec_gm<-eh_hes %>%
  separate(ADMIDATE,into=c("year", "month", "day"), sep="-") %>%
  mutate(year_month=paste0(year,'_',month)) %>%
  group_by(PROCODE3,year_month) %>% 
  filter(gm == TRUE) %>%
  summarise(n_activity_gm =n() ) %>%
  mutate(year_month2=year_month) %>%
  separate(year_month, c('year','month'), sep='_') %>%
  select(-month) %>%
  mutate(year2=as.integer(case_when(year %in% c(2021,2022)~'2020',
                                    TRUE ~ year))) %>%
  select(-year) %>%
  left_join(trust_catchment, by=c('PROCODE3'='trust_code', 'year2'='year')) %>%
  mutate(rate_gm=n_activity_gm/catchment_cat*1000) 

#spn
eh_hes_hvlc_spec_spn<-eh_hes %>%
  separate(ADMIDATE,into=c("year", "month", "day"), sep="-") %>%
  mutate(year_month=paste0(year,'_',month)) %>%
  group_by(PROCODE3,year_month) %>% 
  filter(spn == TRUE) %>%
  summarise(n_activity_spn =n() ) %>%
  mutate(year_month2=year_month) %>%
  separate(year_month, c('year','month'), sep='_') %>%
  select(-month) %>%
  mutate(year2=as.integer(case_when(year %in% c(2021,2022)~'2020',
                                    TRUE ~ year))) %>%
  select(-year) %>%
  left_join(trust_catchment, by=c('PROCODE3'='trust_code', 'year2'='year')) %>%
  mutate(rate_spn=n_activity_spn/catchment_cat*1000)

#opht
eh_hes_hvlc_spec_opht<-eh_hes %>%
  separate(ADMIDATE,into=c("year", "month", "day"), sep="-") %>%
  mutate(year_month=paste0(year,'_',month)) %>%
  group_by(PROCODE3,year_month) %>% 
  filter(opht == TRUE) %>%
  summarise(n_activity_opht =n() ) %>%
  mutate(year_month2=year_month) %>%
  separate(year_month, c('year','month'), sep='_') %>%
  select(-month) %>%
  mutate(year2=as.integer(case_when(year %in% c(2021,2022)~'2020',
                                    TRUE ~ year))) %>%
  select(-year) %>%
  left_join(trust_catchment, by=c('PROCODE3'='trust_code', 'year2'='year')) %>%
  mutate(rate_opht=n_activity_opht/catchment_cat*1000) 

#uro
eh_hes_hvlc_spec_uro<-eh_hes %>%
  separate(ADMIDATE,into=c("year", "month", "day"), sep="-") %>%
  mutate(year_month=paste0(year,'_',month)) %>%
  group_by(PROCODE3,year_month) %>% 
  filter(uro == TRUE) %>%
  summarise(n_activity_uro =n() ) %>%
  mutate(year_month2=year_month) %>%
  separate(year_month, c('year','month'), sep='_') %>%
  select(-month) %>%
  mutate(year2=as.integer(case_when(year %in% c(2021,2022)~'2020',
                                    TRUE ~ year))) %>%
  select(-year) %>%
  left_join(trust_catchment, by=c('PROCODE3'='trust_code', 'year2'='year')) %>%
  mutate(rate_uro=n_activity_uro/catchment_cat*1000) 
# now create a combined dataset which has activity for each of the specialties 
eh_hes_hvlc_activity <- eh_hes_hvlc_spec_ort %>%
  full_join(eh_hes_hvlc_spec_ent, by=c('PROCODE3', "year_month2",  "catchment_cat", "white_pct", "age_65_plus_pct", "sex_male_pct", "year2" )) %>% 
  full_join(eh_hes_hvlc_spec_gs, by=c('PROCODE3', "year_month2" ,  "catchment_cat", "white_pct", "age_65_plus_pct", "sex_male_pct", "year2")) %>%
  full_join(eh_hes_hvlc_spec_gm, by=c('PROCODE3', "year_month2" ,  "catchment_cat", "white_pct", "age_65_plus_pct", "sex_male_pct", "year2")) %>%
  full_join(eh_hes_hvlc_spec_spn, by=c('PROCODE3', "year_month2" ,  "catchment_cat", "white_pct", "age_65_plus_pct", "sex_male_pct", "year2")) %>%
  full_join(eh_hes_hvlc_spec_opht, by=c('PROCODE3', "year_month2" ,  "catchment_cat", "white_pct", "age_65_plus_pct", "sex_male_pct", "year2")) %>%
  full_join(eh_hes_hvlc_spec_uro, by=c('PROCODE3', "year_month2" ,  "catchment_cat", "white_pct", "age_65_plus_pct", "sex_male_pct", "year2")) 


#check for duplicates

counted_combinations <- eh_hes_hvlc_activity %>%
  count (PROCODE3, year_month2)

#calculate n_activity_hvlc

eh_hes_hvlc_activity <- eh_hes_hvlc_activity %>%
  group_by(PROCODE3,year_month2) %>% 
   mutate (n_activity_hvlc = sum (c(n_activity_ort, n_activity_opht, n_activity_ent,
                                            n_activity_uro, n_activity_gs, n_activity_gm, n_activity_spn), na.rm =TRUE
  ))
  
eh_hes_hvlc_activity <- eh_hes_hvlc_activity %>%
  mutate(hvlc_rate=n_activity_hvlc/catchment_cat*1000)

##los for hvlc specialties ----
# again, repeats the same code with additional filter step 
eh_hes_los_hvlc <- eh_hes %>%
  mutate (los = as.numeric (difftime (DISDATE,ADMIDATE, units = "days" )))

eh_hes_los_hvlc <- eh_hes_los_hvlc %>%
  filter (los >=0)

eh_hes_los_hvlc <- eh_hes_los_hvlc %>%
  mutate (elix_2_or_more = nr_elix_h36 >=2) # make a new variable with true false if person has 2 or more elixhauser conditions in the previous 36 months 

eh_hes_los_hvlc <- eh_hes_los_hvlc %>%
  mutate (los_new = ifelse (los >37,37, los))

# for all hvlc procedures
eh_hes_hvlc_spec_los_all <- eh_hes_los_hvlc %>%
  filter (ort | opht | gm | gs | spn | ent |uro) %>%
  separate(ADMIDATE,into=c("year", "month", "day"), sep="-") %>%
  mutate(year_month=paste0(year,'_',month)) %>%
  group_by(PROCODE3,year_month) %>% 
  summarise(n=n(),
            los_avg_hvlc=mean(los_new[los_new > 0]),
            dc_prop_hvlc=length(los[which(los==0)])/length(los)
  )


#ort
eh_hes_hvlc_spec_los_ort<-eh_hes_los_hvlc %>%
  filter(ort == TRUE)%>%
  separate(ADMIDATE,into=c("year", "month", "day"), sep="-") %>%
  mutate(year_month=paste0(year,'_',month)) %>%
  group_by(PROCODE3,year_month) %>% 
  summarise(n=n(),
            los_avg_ort=mean(los_new[los_new > 0]),
            dc_prop_ort=length(los[which(los==0)])/length(los)
  )


#ent

eh_hes_hvlc_spec_los_ent<-eh_hes_los_hvlc %>%
  filter(ent == TRUE)%>%
  separate(ADMIDATE,into=c("year", "month", "day"), sep="-") %>%
  mutate(year_month=paste0(year,'_',month)) %>%
  group_by(PROCODE3,year_month) %>% 
  summarise(n=n(),
            los_avg_ent=mean(los[los > 0]),
            dc_prop_ent=length(los[which(los==0)])/length(los)
  )

#gm

eh_hes_hvlc_spec_los_gm<-eh_hes_los_hvlc %>%
  filter(gm == TRUE)%>%
  separate(ADMIDATE,into=c("year", "month", "day"), sep="-") %>%
  mutate(year_month=paste0(year,'_',month)) %>%
  group_by(PROCODE3,year_month) %>% 
  summarise(n=n(),
            los_avg_gm=mean(los_new[los_new > 0]),
            dc_prop_gm=length(los[which(los==0)])/length(los)
  )


#gs

eh_hes_hvlc_spec_los_gs<-eh_hes_los_hvlc %>%
  filter(gs == TRUE)%>%
  separate(ADMIDATE,into=c("year", "month", "day"), sep="-") %>%
  mutate(year_month=paste0(year,'_',month)) %>%
  group_by(PROCODE3,year_month) %>% 
  summarise(n=n(),
            los_avg_gs=mean(los_new[los_new > 0]),
            dc_prop_gs=length(los[which(los==0)])/length(los)
  )

#opht

eh_hes_hvlc_spec_los_opht<-eh_hes_los_hvlc %>%
  filter(opht == TRUE)%>%
  separate(ADMIDATE,into=c("year", "month", "day"), sep="-") %>%
  mutate(year_month=paste0(year,'_',month)) %>%
  group_by(PROCODE3,year_month) %>% 
  summarise(n=n(),
            los_avg_opht=mean(los_new[los_new > 0]),
            dc_prop_opht=length(los[which(los==0)])/length(los)
  )

#spn

eh_hes_hvlc_spec_los_spn<-eh_hes_los_hvlc %>%
  filter(spn == TRUE)%>%
  separate(ADMIDATE,into=c("year", "month", "day"), sep="-") %>%
  mutate(year_month=paste0(year,'_',month)) %>%
  group_by(PROCODE3,year_month) %>% 
  summarise(n=n(),
            los_avg_spn=mean(los_new[los_new > 0]),
            dc_prop_spn=length(los[which(los==0)])/length(los)
  )


#uro

eh_hes_hvlc_spec_los_uro<-eh_hes_los_hvlc %>%
  filter(uro == TRUE)%>%
  separate(ADMIDATE,into=c("year", "month", "day"), sep="-") %>%
  mutate(year_month=paste0(year,'_',month)) %>%
  group_by(PROCODE3,year_month) %>% 
  summarise(n=n(),
            los_avg_uro=mean(los_new[los_new > 0]),
            dc_prop_uro=length(los[which(los==0)])/length(los)
  )

# create a combined dataset which has los and %dc for each of the specialties 
eh_hes_hvlc_los_combo <- eh_hes_hvlc_spec_los_uro %>%
  full_join(eh_hes_hvlc_spec_los_ort, by=c('PROCODE3', "year_month" )) %>%
  full_join(eh_hes_hvlc_spec_los_opht, by=c('PROCODE3', "year_month" )) %>%
  full_join(eh_hes_hvlc_spec_los_gm, by=c('PROCODE3', "year_month" )) %>%
  full_join(eh_hes_hvlc_spec_los_gs, by=c('PROCODE3', "year_month" )) %>%
  full_join(eh_hes_hvlc_spec_los_ent, by=c('PROCODE3', "year_month" )) %>%
  full_join(eh_hes_hvlc_spec_los_spn, by=c('PROCODE3', "year_month" )) %>%
  full_join(eh_hes_hvlc_spec_los_all, by=c('PROCODE3', "year_month" ))%>%
  select (-n.x, -n.y, -n.x.x, -n.y.y, -n.x.x.x, -n.y.y.y, -n.x.x.x.x, -n.y.y.y.y)

#check for duplicates

counted_combinations <- eh_hes_hvlc_los_combo %>%
  count (PROCODE3, year_month)


#join together info for hvlc specialties from activity and los
hes_hvlc_data_set <- eh_hes_hvlc_activity %>%
  full_join(eh_hes_hvlc_los_combo, by =c('PROCODE3', "year_month2" = "year_month")) %>%
  select ( -imd_decile.x, -imd_decile.x.x, -imd_decile.x.x.x, -imd_decile.y, -imd_decile.y.y, -imd_decile.y.y.y)

##create final dataset ----

#join together the overall and hvlc specific data sets 
final_data_set <- hes_data_set %>%
  full_join(hes_hvlc_data_set, by =c('PROCODE3', "year_month2", "catchment_cat", "white_pct", "age_65_plus_pct", "sex_male_pct")) %>%
  rename (year_month = year_month2)

test1 <- final_data_set %>%
  distinct (PROCODE3) # total n providers for exclusion diagram in methods 

# only want to have NHS trusts

final_data_set <- final_data_set %>% 
  filter (startsWith(PROCODE3, "R")) #this limits to NHS trusts 

test2 <- final_data_set %>%
  distinct (PROCODE3) # NHS n providers for exclusion diagram in methods 


# only want to have trusts that feature in peer finder tool so read in the peer finder data and then get a list of trust codes 
# trust peer finder tool - appendix a is a publicly available document which can be used to identify peer catchemnt data 
 peer_dat <- s3read_using(read_xlsx, 
                                    sheet="Raw Data", 
                                    object="Trust Peer Finder Tool - Appendix A.xlsx", 
                                    bucket=project_bucket
)


 include <- peer_dat [1] %>% 
   rename (PROCODE3 = Procode) %>%
   distinct () # just a list of trust codes to use for filter based on them having peer data

 final_data_set <- final_data_set %>%
   inner_join(include, by = "PROCODE3") # now we only have trusts in the final dataset who also have peer catchment data
 
 test3 <- final_data_set %>%
   distinct (PROCODE3) # n providers for exclusion diagram in methods 
 

# this tells us which trusts have trust catchment data 
trust_include <- trust_catchment %>%
  rename (PROCODE3 = trust_code) %>%
  select (PROCODE3) %>% 
  distinct ()

final_data_set <- final_data_set %>%
  inner_join(trust_include, by = "PROCODE3") # now we only have trusts in the final dataset who also have trust catchment data 

test4 <- final_data_set %>%
  distinct (PROCODE3) # n providers for exclusion diagram in methods 

# list of trusts to exclude 
# this is an internal list of trusts to exclude for reasons including specialty trusts, trusts with known data quality issues, trusts with mergers
# more info on the exclusion process in methods of Co et al. 2024 paper
trusts_to_exclude <- s3read_using(read_csv,object='trusts to exclude.csv',bucket=project_bucket)%>%
  select (PROCODE3) %>%
  as.data.frame ()

final_data_set <- final_data_set %>%
  anti_join(trusts_to_exclude, by = "PROCODE3")%>%
  select (-imd_decile.x, -imd_decile.y)

test5 <- final_data_set %>%
  distinct (PROCODE3) # n providers for trust catchment data 

# add in elective_hub_cat

final_data_set <- final_data_set %>%
  left_join(trust_look_up, by = "PROCODE3")

# work out which trusts went at what point - have commented out but in case helpful 

# test1 <- test1 %>%
#   rename (test1 = PROCODE3)
# 
# test2 <- test2 %>%
#   rename (test2 = PROCODE3)
# 
# test3 <- test3 %>%
#   rename (test3 = PROCODE3)
# 
# test4 <- test4 %>%
#   rename (test4 = PROCODE3)
# 
# test5 <- test5 %>%
#   rename (test5 = PROCODE3)
# 
# s3write_using(test1, FUN=write.csv, object = "test1.csv", bucket = project_bucket)
# s3write_using(test2, FUN=write.csv, object = "test2.csv", bucket = project_bucket)
# s3write_using(test3, FUN=write.csv, object = "test3.csv", bucket = project_bucket)
# s3write_using(test4, FUN=write.csv, object = "test4.csv", bucket = project_bucket)
# s3write_using(test5, FUN=write.csv, object = "test5.csv", bucket = project_bucket)



s3write_using(final_data_set, FUN=write.csv, object = "HES - all elective and hvlc activity and los by provider and month.csv", bucket = project_bucket)


