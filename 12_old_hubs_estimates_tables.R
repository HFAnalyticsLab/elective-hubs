## ========================================================================== ##
# Project: GIRFT Elective Hubs Evaluation 

# Team: Improvement Analytics Unit (IAU) at the Health Foundation

# Script: 12_old_hubs_estimates_tables.R

# Corresponding author: Stefano Conti (e. stefano.conti@nhs.net)

# Description: 
# Fit GSynth model to elective / HVLC activity data-set to contrast established hub trusts
# (jointly considered) with no-hub trusts

# Dependencies: 
# '00_preamble.R', 'correct_gsynth.r'

# Inputs: 
# Data-sets: 'lm_dat_old.csv', 'lm_dat_old_ord.csv', 'lm_dat_old_opht.csv', 'lm_dat_old_gs.csv', 
#            'lm_dat_old_uro.csv', 'lm_dat_old_ent.csv', 'lm_dat_old_ord.gm'

# Outputs:
# All outputs saved into global environment (not externally)
# old_el.hvlc_att.delta_inf.ls2 (list by elective activity type of lists by outcome metric of 
#                                GSYnth effect and counterfactual inferences data-frames)
# old_el.hvlc_gsynth.mdl.ls2 (list by elective activity type of lists by outcome metric of GSynth 
#                             model outputs)
# old_spc_att.delta_inf.ls (list by HVLC specialty - outcome metric combination of GSynth 
#                           effect and counterfactual inferences data-frames)
# old_spc_gsynth.mdl.ls (list by HVLC specialty - outcome metric combination of GSynth 
#                        model outputs)

# Notes:
# Some naming conventions are leftovers from previous modelling attempts, e.g. use of "lm" in name of datasets
#
# Generally may need to adjust file paths as currently assumed S3 bucket system and GitHub structure
## ========================================================================== ##


##############
## Preamble ##
##############

source("~/iaelecthubs1/hes_did/pipeline/00_preamble.R")  # Source project preamble script

source("~/iaelecthubs1/hes_did/pipeline/correct_gsynth.r")  # Source auxiliary and staggered GSynth implementation routines


el_old.no.dat <- s3read_using(read.table, 
                              header=TRUE, sep=",", quote="\"", row.names=1, 
                              check.names=FALSE, fill=TRUE, comment.char="", stringsAsFactors=FALSE, 
                              object="gsynth results/gsynth input datasets/lm_dat_old.csv", 
                              bucket=project_bucket
                              )  # Load established hub trusts vs no-hub trusts analysis data-frame


spc_old.no.dat.ls <- sapply(setdiff(hvlc_names.vec, y="spn"), 
                            FUN=function(spc) 
                              s3read_using(read.table, 
                                           header=TRUE, sep=",", quote="\"", row.names=1, 
                                           check.names=FALSE, fill=TRUE, comment.char="", stringsAsFactors=FALSE, 
                                           object=file.path("gsynth results/gsynth input datasets", 
                                                            paste0("lm_dat_old_", spc, ".csv")
                                                            ), 
                                           bucket=project_bucket
                                           ), 
                            simplify=FALSE
                            )  # Load list by specialty established hub trusts vs no-hub trusts analysis data-frames


#################################
## Elective / HVLC outcomes    ##
## for established hub trusts  ##
#################################

old_el.hvlc_gsynth.mdl.ls2 <- sapply(c("el", "hvlc"), 
                                     FUN=function(typ) 
                                       sapply(c("act", "lhs", "dcr"), 
                                              FUN=function(out) 
                                                s3readRDS(file.path("gsynth results/gsynth objects/pooled", 
                                                                    paste(paste("old_hub", 
                                                                                c("elect", "hvlc")[match(typ, table=c("el", "hvlc"))], 
                                                                                c("rate", "los.ip_avg", "los.dc_ratio")[match(out, table=c("act", "lhs", "dcr"))], 
                                                                                sep="_"
                                                                                ), 
                                                                          "rds", 
                                                                          sep="."
                                                                          )
                                                                    ), 
                                                          bucket=project_bucket
                                                          ), 
                                              simplify=FALSE
                                              ), 
                                     simplify=FALSE
                                     )  # Load list by outcome type of lists by outcome of GSynth model outputs


old_el.hvlc_att.delta_inf.ls2 <- sapply(names(old_el.hvlc_gsynth.mdl.ls2), 
                                        FUN=function(typ) 
                                          sapply(names(old_el.hvlc_gsynth.mdl.ls2[[typ]]), 
                                                 FUN=function(out) 
                                                   correct_gsynth.fn(old_el.hvlc_gsynth.mdl.ls2[[typ]][[out]], 
                                                                     data.dat=el_old.no.dat, 
                                                                     time.vec=pre.post_key.vec.ls$post, 
                                                                     alpha_ci=.05, 
                                                                     dest.dir=file.path(output_internal_dir, 
                                                                                        "GSynth analysis/Inference/Established hub"
                                                                                        ), 
                                                                     out.prf=paste("old_hub", 
                                                                                   paste(typ, out, sep="."), 
                                                                                   sep="_"
                                                                                   )
                                                                     ), 
                                                 simplify=FALSE
                                                 ), 
                                        simplify=FALSE
                                        )  # Derive list by outcome type of lists by outcome of ATT and outcome change inferences data-frames


#################################
## HVLC outcomes by specialty  ##
## for established hub trusts  ##
#################################

old_spc_mdl.vec <- outer(setdiff(hvlc_names.vec, y="spn"), 
                         c("rate", "los.ip_avg", "los.dc_ratio"), 
                         FUN=function(spc, out) 
                           paste0(paste(spc, "old_hub", spc, out, sep="_"), ".rds")
                         )  # Set vector by specialty, outcome of available GSynth model dumps in .rds format


old_spc_gsynth.mdl.ls <- sapply(old_spc_mdl.vec, 
                                FUN=function(fl) 
                                  s3readRDS(file.path("gsynth results/gsynth objects/secondary and sensitivity analyses/specialities", 
                                                      fl
                                                      ), 
                                            bucket=project_bucket
                                            ), 
                                simplify=FALSE
                                )  # Load list by specialty, outcome GSynth model outputs for HVLC specialties

names(old_spc_gsynth.mdl.ls) <- sub("rate", replacement="act", 
                                    x=sub("los.ip_avg", replacement="los", 
                                          x=sub("los.dc_ratio", replacement="dcr", 
                                                x=sub("(^.+hub_)([[:alpha:]]+)(_)([[:alpha:]]+)(.*)(\\.rds$)", replacement="\\2.\\4", 
                                                      x=names(old_spc_gsynth.mdl.ls)
                                                      )
                                                )
                                          )
                                    )  # Rename HVLC specialties GSynth model outputs list entries


old_spc_att.delta_inf.ls <- sapply(names(old_spc_gsynth.mdl.ls), 
                                   FUN=function(spc.out) 
                                     correct_gsynth.fn(old_spc_gsynth.mdl.ls[[spc.out]], 
                                                       data.dat=spc_old.no.dat.ls[[sub("(^[[:alpha:]]+)(\\.[[:alpha:]]+$)", 
                                                                                       replacement="\\1", 
                                                                                       x=spc.out
                                                                                       )]], 
                                                       time.vec=pre.post_key.vec.ls$post, 
                                                       alpha_ci=.05, 
                                                       dest.dir=file.path(output_internal_dir, 
                                                                          "GSynth analysis/Inference/Established hub"
                                                                          ), 
                                                       out.prf=paste("old_hub", spc.out, sep="_")
                                                       ), 
                                   simplify=FALSE
                                   )  # Derive list by post-intervention time, intervention unit of ATT and percent outcome change for HVLC specialties


save(old_el.hvlc_att.delta_inf.ls2, old_el.hvlc_gsynth.mdl.ls2, 
     old_spc_att.delta_inf.ls, old_spc_gsynth.mdl.ls, 
     file=file.path(output_internal_dir, 
                    "GSynth analysis/Inference/Established hub/old_att.delta_inf.RData"
                    )
     )  # Save ATT and outcome change inferences data-frames for established hub trusts as .RData dump