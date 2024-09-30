## ========================================================================== ##
# Project: GIRFT Elective Hubs Evaluation 

# Team: Improvement Analytics Unit (IAU) at the Health Foundation

# Script: 11_-_new_hubs_estimates_tables.R

# Corresponding author: Stefano Conti (e. stefano.conti@nhs.net)

# Description: 
# Fit GSynth model to elective / HVLC activity data-set to contrast new hub trusts
# (jointly considered) with no-hub trusts

# Dependencies: 
# '00_preamble.R', 'correct_gsynth.r'

# Inputs: 
# Data-sets: 'lm_dat_new.csv', 'lm_dat_new_ord.csv', 'lm_dat_new_opht.csv', 'lm_dat_new_gs.csv', 
#            'lm_dat_new_uro.csv', 'lm_dat_new_ent.csv', 'lm_dat_new_ord.gm'

# Outputs:
# All outputs saved into global environment (not externally)
# gsynth_new.mdl.ls2 (list by elective activity type of lists by outcome metric of GSynth 
#                     model outputs)
# new_inf.dat.ls2 (list by elective activity type of lists by outcome metric of corrected 
#                  GSYnth effect and counterfactual inferences data-frames)
# spc_gsynth.mdl.ls (list by HVLC specialty - outcome metric combination of GSynth 
#                    model outputs)
# spc.out_new.no.dat.ls (list by HVLC specialty - outcome metric combination of corrected 
#                        GSYnth effect and counterfactual inferences data-frames)

# Notes:
# Some naming conventions are leftovers from previous modelling attempts, e.g. use of "lm" in name of datasets
#
# Generally may need to adjust file paths as currently assumed S3 bucket system and GitHub structure
## ==========================================================================##


##############
## Preamble ##
##############

source("~/iaelecthubs1/hes_did/pipeline/00_preamble.R")  # Source project preamble script

source("~/iaelecthubs1/hes_did/pipeline/correct_gsynth.r")  # Source auxiliary and staggered GSynth implementation routines


el_new.no.dat <- s3read_using(read.table, 
                              header=TRUE, sep=",", quote="\"", row.names=1, 
                              check.names=FALSE, fill=TRUE, comment.char="", stringsAsFactors=FALSE, 
                              object="gsynth results/gsynth input datasets/lm_dat_new.csv", 
                              bucket=project_bucket
                              )  # Load new hub trusts vs no-hub trusts analysis data-frame


spc_new.no.dat.ls <- sapply(setdiff(hvlc_names.vec, y="spn"), 
                            FUN=function(spc) 
                              s3read_using(read.table, 
                                           header=TRUE, sep=",", quote="\"", row.names=1, 
                                           check.names=FALSE, fill=TRUE, comment.char="", stringsAsFactors=FALSE, 
                                           object=file.path("gsynth results/gsynth input datasets", 
                                                            paste0("lm_dat_new_", spc, ".csv")
                                                            ), 
                                           bucket=project_bucket
                                           ), 
                            simplify=FALSE
                            )  # Load into list by specialty new hub trusts vs no-hub trusts analysis data-frames


###################
## Elective and  ##
## HVLC outcomes ##
###################

gsynth_new.mdl.ls2 <- sapply(c("el", "hvlc"), 
                             FUN=function(typ) 
                               sapply(c("act", "los", "dcr"), 
                                      FUN=function(out) 
                                        s3readRDS(file.path("gsynth results/gsynth objects/pooled", 
                                                            paste(paste("new_hub", 
                                                                        c("elect", "hvlc")[match(typ, table=c("el", "hvlc"))], 
                                                                        c("rate", "los.ip_avg", "los.dc_ratio")[match(out, table=c("act", "los", "dcr"))], 
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


new_inf.dat.ls2 <- sapply(names(gsynth_new.mdl.ls2), 
                          FUN=function(typ) 
                            sapply(names(gsynth_new.mdl.ls2[[typ]]), 
                                   FUN=function(out) 
                                     correct_gsynth.fn(gsynth_new.mdl.ls2[[typ]][[out]], 
                                                       data.dat=el_new.no.dat, 
                                                       time.vec=pre.post_key.vec.ls$post, 
                                                       alpha_ci=.05, 
                                                       dest.dir=file.path(output_internal_dir, 
                                                                          "GSynth analysis/Inference/New hub/"
                                                                          ), 
                                                       out.prf=paste("new_hub", 
                                                                     paste(typ, out, sep="."), 
                                                                     sep="_"
                                                                     )
                                                       ), 
                                   simplify=FALSE
                                   ), 
                          simplify=FALSE
                          )  # Derive list by outcome type of lists by outcome of ATT and outcome change inferences data-frames

save(gsynth_new.mdl.ls2, new_inf.dat.ls2,
     file=file.path(output_internal_dir, 
                    "GSynth analysis/Inference/New hub/new_inf.RData"
                    )
     )  # Save ATT and outcome change inferences data-frames for new hub trusts vs non-hub trusts as .RData dump


###################
## HVLC outcomes ##
## by specialty  ##
###################

spc_mdl.vec <- outer(setdiff(hvlc_names.vec, y="spn"), 
                     c("rate", "los.ip_avg", "los.dc_ratio"), 
                     FUN=function(spc, out) 
                       paste0(paste(spc, "new_hub", spc, out, sep="_"), ".rds")
                     )  # Set vector by specialty, outcome of available GSynth model dumps in .rds format


spc_gsynth.mdl.ls <- sapply(spc_mdl.vec, 
                            FUN=function(fl) 
                              s3readRDS(file.path("gsynth results/gsynth objects/secondary and sensitivity analyses/specialities", 
                                                  fl
                                                  ), 
                                        bucket=project_bucket
                                        ), 
                            simplify=FALSE
                            )  # Load list by specialty, outcome GSynth model outputs for HVLC specialties

names(spc_gsynth.mdl.ls) <- sub("rate", replacement="act", 
                                x=sub("los.ip_avg", replacement="los", 
                                      x=sub("los.dc_ratio", replacement="dcr", 
                                            x=sub("(^.+hub_)([[:alpha:]]+)(_)([[:alpha:]]+)(.*)(\\.rds$)", replacement="\\2.\\4", 
                                            # x=sub("(^.+hub_)(.+)(\\.rds$)", replacement="\\2", 
                                                  x=names(spc_gsynth.mdl.ls)
                                                  )
                                            )
                                      )
                                )  # Rename HVLC specialties GSynth model outputs list entries


spc.out_new.no.dat.ls <- sapply(names(spc_gsynth.mdl.ls), 
                                FUN=function(nm) 
                                  correct_gsynth.fn(spc_gsynth.mdl.ls[[nm]], 
                                                    data.dat=spc_new.no.dat.ls[[sub("(^[[:alpha:]]+)(\\.[[:alpha:]]+$)", 
                                                                                    replacement="\\1", 
                                                                                    x=nm
                                                                                    )]], 
                                                    time.vec=pre.post_key.vec.ls$post, 
                                                    alpha_ci=.05, 
                                                    dest.dir=file.path(output_internal_dir, 
                                                                       "GSynth analysis/Inference/New hub/"
                                                                       ), 
                                                    out.prf=paste("new_hub", nm, sep="_")
                                                    ), 
                                simplify=FALSE
                                )  # Derive list by post-intervention time, intervention unit of ATT and percent outcome change for HVLC specialties

save(spc_gsynth.mdl.ls, spc.out_new.no.dat.ls, 
     file=file.path(output_internal_dir, 
                    "GSynth analysis/Inference/New hub/spc_att.delta.RData"
                    )
     )  # Save HVLC specialty ATT and outcome change inferences data-frames as .RData dump