## ==========================================================================##
# Project: GIRFT Elective Hubs Evaluation

# Team: Improvement Analytics Unit (IAU) at the Health Foundation

# Script: 04_hvlc_specialty_recipes_HES.R 

# Corresponding author: Stefano Conti (stefano.conti@nhs.net) and Freya Tracey (freya.tracey@health.org.uk)

# Description:
# Takes spells from 03 script and flags whether they are an HVLC procedure 
# Source: GIRFT HVLC coding recipes 
#                           (u. https://gettingitrightfirsttime.co.uk/cross_cutting_theme/clinical-coding)

# Dependencies:
# '03_HES_finish_dataset.R'

# Inputs:
# Takes spells from 03 script 

# Outputs:
# HES data set with flags to whether it is a HVLC procedure 

# Notes: To use, need to adjust locations of R scripts and csv files

## ==========================================================================##
library(arrow) # not in preamble 

project_bucket <- '' # assign project directory

hes_raw <- s3read_using(read_parquet, object = 'hes_intermediate_surg.parquet', bucket = project_bucket)

# variables to change for script to run
hes <- hes_raw %>% 
  rename(primproc = OPERTN_01
         , primdiag = DIAG_01
         , mainspeccode = MAINSPEF
         , treatfunctcode = TRETSPEF
  )

rm(hes_raw)
gc()

#####################
## Data processing ##
#####################


eh_dat <- within(hes, expr={

  # Specialty: Orthopaedics
  #
  # Pathway: Anterior Cruciate Ligament Reconstruction

  ort_aclr <- primproc %in% c("W723", "W742", "W752", "W748", "W758", "W841", "W842") &
    grepl("Z845|Z846", x=tnrprocall)  # Derive logical vector for cases undergoing Anterior Cruciate Ligament Reconstruction

  # Pathway: Total Hip Replacement

  ort_thr <- (primproc %in% c("W371", "W379", "W381", "W389", "W931", "W939", "W391", 
                              "W399", "W941", "W949", "W951", "W959") | 
                (primproc %in% c("W521", "W529", "W531", "W539", "W541", "W549") & 
                   grepl("Z843", x=tnrprocall)
                 )
              ) & 
    ! grepl("W05|Y713|Y716|Y717", x=tnrprocall) & 
    ! grepl("C402", x=tnrdiagall)  # Derive logical vector for cases undergoing Total Hip Replacement


  # Pathway: Total Knee Replacement

  ort_tkr <- primproc %in% c("O181", "O189", "W401", "W409", "W411", "W419", "W421",
                             "W429", "O188", "W408", "W418", "W428") &
    ! grepl("Y713|Y716|Y717|W05|Y031|Y032|Y033|Y034|Y035|Y036|Y037|Y038|Y039",
            x=tnrprocall) &
    ! grepl("M932|M939|Q774", x=tnrdiagall) &
    ! primdiag %in% c("C400", "C401", "C402", "C403", "C408", "C409", "C492")  # Derive logical vector for cases undergoing Total Knee Replacement

  # Pathway: Unicompartmental Knee Replacement

  ort_ukr <- primproc == "W581" &
    grepl("Z845|Z846|Z132", x=tnrprocall)  # Derive logical vector for cases undergoing Unicompartmental Hip Replacement

  # Pathway: Bunion Surgery

  ort_bs <- grepl("W791|W792|W151|W152|W153", x=tnrprocall)  # Derive logical vector for cases undergoing Bunion Surgery

  # Pathway: Therapeutic Shoulder Arthroscopy

  ort_tsa <- (primproc %in% c("W781", "T791", "O291", "T794") &
    grepl("Y767", x=tnrprocall)) |
    (primproc == "W844" &
       grepl("Z682|Z812|Z813|Z814|Z891", x=tnrprocall)) |
    (primproc %in% c("T645", "T702") &
       grepl("Z544", x=tnrprocall) &
       grepl("Y767", x=tnrprocall)) |
    (primproc == "W784" &
       grepl("Z812|Z813|Z814|Z891|Z681|Z682|Z683|Z684|Z685|Z688|Z689|Z691|Z693",
             x=tnrprocall))  # Derive logical vector for cases undergoing Therapeutic Shoulder Arthroscopy

  # Pathway: any

  ort <- ort_aclr | ort_thr | ort_tkr | ort_ukr | ort_bs | ort_tsa  # Derive logical vector for cases undergoing 'any' Orthopaedic procedure

  rm(list=ls(pattern="orth{0,1}_.+"))  # Drop superfluous variables


  # Specialty: Ophthalmology
  #
  # Pathway: Low Complexity Cataract Surgery

  opht_lccs <- grepl("C71|C73|C74", x=tnrprocall) &
    grepl("C751|C754|C758|C759", x=tnrprocall) &
    ! grepl("C647|C776|C792|C793|C795|C796|C797|C801|C802|C803|C804|C805|C806|C808|C809",
            x=tnrprocall) &
    ! grepl("F00|F01|F02|F03|G30|F051|G310|G311|F70|F71|F72|F73|F78|F79|H200|H201|H202|H208|H209|H220|H221|H300|H301|H302|H308|H309|H320",
            x=tnrdiagall)  # Derive logical vector for cases undergoing Low Complexity Cataract Surgery

  # Pathway: any

  opht <- opht_lccs  # Derive logical vector for cases undergoing 'any' Ophthalmology procedure

  rm(list=ls(pattern="opht_.+"))  # Drop superfluous variables


  # Specialty: General Surgery
  #
  # Pathway: Laparoscopic Cholecystectomy

  gs_lc <- primproc %in% c("J181", "J183") &
    grepl("Y752", x=tnrprocall) &
    ! grepl("J182", x=tnrprocall)  # Derive logical vector for cases undergoing Laparoscopic Cholecystectomy

  # Pathway: Primary Inguinal Hernia Repair

  gs_pihr <- grepl("T201|T202|T203|T204|T208|T209", x=tnrprocall) &
    ! grepl("Y713|Y716|Y717", x=tnrprocall) &
    ! grepl("C56|C570", x=tnrdiagall)  # Derive logical vector for cases undergoing Primary Inguinal Hernia Repair

  # Pathway: Para-Umbilical Hernia

  gs_puh <- primproc %in% paste0("T", c(240:243, 245:249)) &
    primproc != "T244"  # Derive logical vector for cases undergoing Para-Umbilical Hernia

  # Pathway: any

  gs <- gs_lc | gs_pihr | gs_puh  # Derive logical vector for cases undergoing 'any' General Surgery procedure

  rm(list=ls(pattern="gs_.+"))  # Drop superfluous variables


  # Specialty: Urology
  #
  # Pathway: Bladder Outflow Obstruction

  uro_boo <- (mainspeccode == 101 | treatfunctcode %in% c(101, 211)) &
        primproc %in% paste0("M", c(651, 653:656, 658:659, 662, 681, 683, 688:689))  # Derive logical vector for cases undergoing Bladder Outflow Obstruction

  # Pathway: Bladder Tumour Resection (TURBT)

  uro_btr <- (mainspeccode == 101 | treatfunctcode %in% c(101, 211)) &
        primproc == "M421"  # Derive logical vector for cases undergoing Bladder Tumour Resection

  # Pathway: Cytoscopy Plus

  uro_cp <- (mainspeccode == 101 | treatfunctcode %in% c(101, 211)) &
        primproc %in% paste0("M", c(451:455, 458:459, 441:442, 763:764, 792, 814))  # Derive logical vector for cases undergoing Cytoscopy Plus

  # Pathway: Ureteroscopy and Stent Management

  uro_usm <- (mainspeccode == 101 | treatfunctcode %in% c(101, 211)) &
        primproc %in% paste0("M", c(sprintf("%03d", c(71:72, 78:79)), 271:275, 277:279,
                                306, 281:283, 288:289, 274:275, 292:293, 295)
                         )  # Derive logical vector for cases undergoing Ureteroscopy and Stent Management

  # Pathway: Minor Peno-Scrotal Surgery

  uro_mpss <- (mainspeccode == 101 | treatfunctcode %in% c(101, 211)) &
        primproc %in% c(paste0("N", c(303, 284, 111:116, 118:119, 132, 321,
                                  sprintf("%03d", c(81:84, 88:89, 91:94, 98:99)),
                                  191, 198:199)
                           ),
                    "M731", "T193"
                    )  # Derive logical vector for cases undergoing Peno-Scrotal Surgery

  # Pathway: any

  uro <- uro_boo | uro_btr | uro_cp | uro_usm | uro_mpss  # Derive logical vector for cases undergoing 'any' Urology procedure

  rm(list=ls(pattern="uro_.+"))  # Drop superfluous variables


  # Specialty: Ear, Nose and Throat (ENT); see 09.02.23 e-mail from William Grey
  #            about head and neck cancer patients exclusion codes
  #
  # Pathway: Endo Sinus Surgery

  ent_ess <- (mainspeccode == 120 | treatfunctcode %in% c(120, 215)) &
        primproc %in% paste0("E", c(132:133, 142:143, sprintf("%03d", 81), 148)) &
    grepl("Y761", x=tnrprocall) &
    ! grepl("C01|C051|C052|C07|C080|C081|C089|C090|C091|C098|C099|C100|C101|C102|C103|C108|C109|C110|C111|C112|C113|C118|C119|C12|C130|C131|132|C138|C139|C320|C321|C322|C328|C329|C73",
            x=tnrprocall)  # Derive logical vector for cases undergoing Endo Sinus Surgery

  # Pathway: Tonsillectomy

  ent_tons <- (mainspeccode == 120 | treatfunctcode %in% c(120, 215)) &
        primproc %in% paste0("F", c(341:345, 347:349)) &
    ! grepl("D345|F346", x=tnrprocall) &
    ! grepl("C01|C051|C052|C07|C080|C081|C089|C090|C091|C098|C099|C100|C101|C102|C103|C108|C109|C110|C111|C112|C113|C118|C119|C12|C130|C131|132|C138|C139|C320|C321|C322|C328|C329|C73",
            x=tnrprocall)  # Derive logical vector for cases undergoing Tonsillectomy

  # Pathway: Myringoplasty

  ent_mir <- (mainspeccode == 120 | treatfunctcode %in% c(120, 215)) &
        primproc %in% paste0("D", c(141:142, 148:149)) &
    ! grepl("C01|C051|C052|C07|C080|C081|C089|C090|C091|C098|C099|C100|C101|C102|C103|C108|C109|C110|C111|C112|C113|C118|C119|C12|C130|C131|132|C138|C139|C320|C321|C322|C328|C329|C73",
            x=tnrprocall)  # Derive logical vector for cases undergoing Myringoplasty

  # Pathway: Septoplasty and Turbinate Surgery

  ent_sts <- (mainspeccode == 120 | treatfunctcode %in% c(120, 215)) &
        primproc %in% paste0("E", sprintf("%03d", c(36, 41:49))) &
    ! grepl("C01|C051|C052|C07|C080|C081|C089|C090|C091|C098|C099|C100|C101|C102|C103|C108|C109|C110|C111|C112|C113|C118|C119|C12|C130|C131|132|C138|C139|C320|C321|C322|C328|C329|C73",
            x=tnrprocall)  # Derive logical vector for cases undergoing Septoplasty and Turbinate Surgery

  # Pathway: Septorhinoplasty

  ent_srp <- (mainspeccode == 120 | treatfunctcode %in% c(120, 215)) &
        primproc %in% paste0("E", sprintf("%03d", c(23:24, 73))) &
    ! grepl("C01|C051|C052|C07|C080|C081|C089|C090|C091|C098|C099|C100|C101|C102|C103|C108|C109|C110|C111|C112|C113|C118|C119|C12|C130|C131|132|C138|C139|C320|C321|C322|C328|C329|C73",
            x=tnrprocall)  # Derive logical vector for cases undergoing Septorhinoplasty

  # Pathway: any

  ent <- ent_ess | ent_tons | ent_mir | ent_sts | ent_srp  # Derive logical vector for cases undergoing 'any' Ear | Nose and Throat procedure

  rm(list=ls(pattern="ent_.+"))  # Drop superfluous variables


  # Specialty: Gynaecology and Maternity
  #
  # Pathway: Operative Laparoscopy

  gm_ol <- (mainspeccode %in% c(500, 502) | treatfunctcode %in% 502:503) & 
    (primproc %in% paste0("Q", c(201, 362, 381:382, 388:389, 413, 521:522)) | 
       grepl("^Q39|Q49|Q50|T42", x=primproc)
     ) & 
    ! grepl("Q383", x=tnrprocall) & 
    ! grepl("C51|C52|C53|C54|C55|C56|C57|C796|D06|D070|D071|D072|D073", 
            x=tnrdiagall
            )  # Derive logical vector for cases undergoing Operative Laparoscopy

  # Pathway: Laparoscopic Hysterectomy

  gm_lh <- (mainspeccode %in% c(500, 502) | treatfunctcode %in% 502:503) &
        grepl("Q07|Q08", x=tnrprocall) &
    grepl("Y751|Y752", x=tnrprocall) &
    ! grepl("C51|C52|C53|C54|C55|C56|C57|C796|D06|D070|D071|D072|D073",
            x=tnrdiagall)  # Derive logical vector for cases undergoing Laparoscopic Hysterectomy

  # Pathway: Endometrial Ablation

  gm_ea <- (mainspeccode %in% c(500, 502) | treatfunctcode %in% 502:503) &
        grepl("Q162|Q163|Q164|Q165|Q166|Q176|Q177", x=tnrprocall) &
    ! grepl("C51|C52|C53|C54|C55|C56|C57|C796|D06|D070|D071|D072|D073",
            x=tnrdiagall)  # Derive logical vector for cases undergoing Endometrial Ablation

  # Pathway: Hysteroscopy

  gm_hs <- (mainspeccode %in% c(500, 502) | treatfunctcode %in% 502:503) & 
    (primproc %in% paste0("Q", c(161, 167:169, 171:175, 178:179)) | 
       grepl("^Q18", x=primproc)
     ) & 
    ! grepl("Y751|Y752|Y755|Q413", x=tnrprocall) & 
    ! grepl("C51|C52|C53|C54|C55|C56|C57|C796|D06|D070|D071|D072|D073", 
            x=tnrdiagall
            )  # Derive logical vector for cases undergoing Hysteroscopy

  # Pathway: Vaginal Hysterectomy and / or Vaginal Wall Repair

  gm_vh <- (mainspeccode %in% c(500, 502) | treatfunctcode %in% 502:503) &
        grepl("Q08|P22|P23|P24", x=tnrprocall) &
    ! grepl("C51|C52|C53|C54|C55|C56|C57|C796|D06|D070|D071|D072|D073",
            x=tnrdiagall)  # Derive logical vector for cases undergoing Vaginal Hysterectomy and / or Vaginal Wall Repair

  # Pathway: any

  gm <- gm_ol | gm_lh | gm_ea | gm_hs | gm_vh  # Derive logical vector for cases undergoing 'any' Gynaecology and Maternity procedure

  rm(list=ls(pattern="gm_.+"))  # Drop superfluous variables


  # Specialty: Spinal
  #
  # Pathway: Lumbar Decompression / Discectomy

  spn_ld <- (grepl("V252|V254|V255|V256|V258|V259|V671|V672|V331|V332|V337|V338|V339", 
                   x=tnrprocall
                   ) | 
               (grepl("V351|V358|V359", x=tnrprocall) & 
                  grepl("Z063|Z073|Z665|Z675|Z676|Z993", x=tnrprocall)
                )
             ) &
    ! grepl("V262|V264|V265|V266|V268|V269|V681|V682|V688|V689|V341|V342|V347|V348|V349|V553", 
            x=tnrprocall
            ) & 
    ! grepl("V251|V253|V382|V383|V384|V385|V386|V404", 
            x=tnrprocall
            )  # Derive logical vector for cases undergoing Lumbar Decompression / Discectomy

  # Pathway: One or Two Level Posterior Fusion Surgery

  spn_pfs1 <- grepl("V252|V254|V255|V256|V258|V259|V671|V672|V331|V332|V337|V338|V339", x=tnrprocall) |
    (grepl("V351|V358|V359", x=tnrprocall) & grepl("Z063|Z073|Z665|Z675|Z676|Z993", x=tnrprocall)) &
    ! grepl("V262|V264|V265|V266|V268|V269|V681|V682|V688|V689|V341|V342|V347|V348|V349|V553",
            x=tnrprocall) &
    grepl("V251|V253|V382|V383|V384|V385|V386|V404",
          x=tnrprocall)  # Derive logical vector for cases undergoing One or Two Level Posterior Fusion Surgery

  # Pathway: Cervical Spine Decompression / Fusion

  spn_csd <- (grepl("V221|V222|V294|V295|V361", x=tnrprocall) | 
                (grepl("V368|V369", x=tnrprocall) & 
                   grepl("Z673|Z991", x=tnrprocall)
                 )
              ) & 
    ! grepl("V224|V225|V226|V231|V232|V233|V234|V235|V236|V237|V238|V239|V301|V302|V303|V304|V305|V306|V308|V309|V371|V373|V374|V375|V376|V377|V391|V553|V41|V42", 
            x=tnrprocall)  # Derive logical vector for cases undergoing Cervical Spine Decompression / Fusion

  # Pathway: Lumbar Media Branch Block / Facet Joint Injections

  spn_lmbb <- primproc == "V544"  # Derive logical vector for cases undergoing Lumbar Media Branch Block / Facet Joint Injections

  # Pathway: Lumbar Nerve Root Block / Therapeutic Epidural Injection

  spn_lnrb <- primproc %in% paste0("A", c(521:522, 528:529, 577)) |
    primproc == "A735" & grepl("Z07", x=tnrprocall)  # Derive logical vector for cases undergoing Lumbar Nerve Root Block / Therapeutic Epidural Injection

  # Pathway: One or Two Level Posterior Fusion Surgery (PLF, TLIF, PLIF)

  spn_pfs2 <- grepl("V251|V253|V382|V383|V384|V385|V386|V404", x=tnrprocall) &
    ! grepl("V261|V263|V267|V343|V344|V345|V346|V393|V394|V395|V396|V397|V262|V264|V265|V266|V268|V269|V681|V682|V688|V689|V553",
            x=tnrprocall)  # Derive logical vector for cases undergoing One or Two Level Posterior Fusion Surgery (PLF, TLIF, PLIF)

  # Pathway: any

  spn <- spn_ld | spn_pfs1 | spn_csd | spn_lmbb | spn_lnrb | spn_pfs2  # Derive logical vector for cases undergoing 'any' Spinal procedure

 # rm(tnrmngmttype, list=ls(pattern="spn_.+|diag|proc|fun"))  # Drop superfluous variables


  # Specialty: any HVLC
  #
  # Pathway: any

  # hvlc <- ort | opht | gs | uro | ent | gm | spn  # Derive logical vector for cases undergoing 'any' Spinal procedure
  })


s3write_using(eh_dat, FUN = write_parquet, object = 'hes_hvlc.parquet', bucket = project_bucket)
