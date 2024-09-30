## ========================================================================== ##
# Project: GIRFT Elective Hubs Evaluation 

# Team: Improvement Analytics Unit (IAU) at the Health Foundation

# Script: 14_-_old_hubs_estimates_plots.R

# Corresponding author: Stefano Conti (e. stefano.conti@nhs.net)

# Description: 
# Generate effect and outcome time-series plots to illustrate an established hub 
# trusts (jointly considered) vs no-hub trusts impact assessment

# Dependencies: 
# '00_preamble.R'

# Inputs: 
# Inferences: 'old_att.delta_inf.RData'

# Outputs:
# All outputs saved into global environment (not externally)
# old_hub_TYP.OUT_TRS_att.delta.png (time-series plot of ATT estimates and 95% confidence 
#                                    intervals throughout the study period, where TYP, OUT and 
#                                    TRS respectively indicate elective activity type, outcome 
#                                    metric and NHS trust code)
# old_hub_TYP.OUT_TRS_out.delta.png (time-series plot of observed intervention vs estimated counterfactual 
#                                    values and 95% confidence intervals throughout the study period, 
#                                    where TYP, OUT and TRS respectively indicate elective activity type, 
#                                    outcome metric and NHS trust code)


# All model defaults used across all analyses

# Notes:
# Some naming conventions are leftovers from previous modelling attempts, e.g. use of "lm" in name of datasets
#
# Generally may need to adjust file paths as currently assumed S3 bucket system and GitHub structure 
## ========================================================================== ##


##############
## Preamble ##
##############

source("~/iaelecthubs1/hes_did/pipeline/00_preamble.R")  # Source project preamble script


#######################
## Time-series plots ##
## of ATT trends     ##
#######################

load(file.path(output_internal_dir, 
               "GSynth analysis/Inference/Established hub/old_att.delta_inf.RData"
               )
     )  # Load ATT and outcome change inferences data-frames for established hub trusts vs non-hub trusts as .RData dump


att.delta.frm <- as.formula(paste(paste0("cbind(", 
                                         paste(c(paste("att", c("mean", "ci.lower", "ci.upper"), sep="_"), 
                                                 paste("delta", c("mean", "ci.lower", "ci.upper"), sep="_"), 
                                                 "pval"
                                                 ), 
                                               collapse=", "
                                               ), 
                                         ")"
                                         ), 
                                  paste(c("time", "unit", "outcome"), collapse=" + "), 
                                  sep=" ~ "
                                  )
                            )  # Set formula to format ATT and outcome change inferences data-frames into 4d-arrays by time, unit, outcome, statistic


lapply(names(old_el.hvlc_att.delta_inf.ls2), 
       FUN=function(typ)
         {
         # typ <- c("el", "hvlc")[2]; typ
         # out <- dimnames(att.delta.arr)$outcome[2]; out
         # trs <- dimnames(att.delta.arr)$unit[5]; trs
         # rm(typ, out, trs)
         
         att.delta.dat <- do.call(rbind, 
                                  args=old_el.hvlc_att.delta_inf.ls2[[typ]]
                                  )  # Bind by row ATT inferences data-frames across outcomes
         
         att.delta.dat$outcome <- factor(att.delta.dat$outcome, 
                                         labels=c("dcr", "lhs", "act")
                                         )  # Format "outcome" as factor and rename factor levels
         
         att.delta.dat$outcome <- factor(att.delta.dat$outcome, 
                                         levels=names(old_el.hvlc_gsynth.mdl.ls2[[typ]])
                                         )  # Rearrange "outcome" factor levels
         
         
         att.delta.arr <- xtabs(att.delta.frm, 
                                data=att.delta.dat
                                )  # Format outcome inferences data-frame into 4d-array by time, unit, outcome, statistic
         
         names(dimnames(att.delta.arr))[names(dimnames(att.delta.arr)) == ""] <- "statistic"  # Rename blank "statistic" margin of outcome inferences 4d-array
         
         
         lapply(dimnames(att.delta.arr)$outcome, 
                FUN=function(out)
                  lapply(dimnames(att.delta.arr)$unit, 
                         FUN=function(trs)
                           {
                           pre.post.vec.ls <- with(old_el.hvlc_gsynth.mdl.ls2[[typ]][[out]], 
                                                   expr=list(pre=-rev(seq.int(ifelse(trs == "Pooled", max(T0), T0[trs == id.tr]))), 
                                                             post=seq.int(T - ifelse(trs == "Pooled", min(T0), T0[trs == id.tr]))
                                                             )
                                                   )  # Derive list by study period of study times vectors
                           
                           pre.post_span.vec.ls <- with(old_el.hvlc_gsynth.mdl.ls2[[typ]][[out]], 
                                                        expr=list(pre=c(overall=paste(-1, -max(T0), sep=" - "), 
                                                                        trs=paste(rev(range(pre.post.vec.ls$pre)), collapse=" - "), 
                                                                        key=paste(rev(range(pre.post_key.vec.ls$pre)), collapse=" - ")
                                                                        ), 
                                                                  post=c(overall=paste(1, T - min(T0), sep=" - "), 
                                                                         trs=paste(range(pre.post.vec.ls$post), collapse=" - "), 
                                                                         key=paste(range(pre.post_key.vec.ls$post), collapse=" - ")
                                                                         )
                                                                  )
                                                        )  # Derive list by study period of study time spans of interest
                           
                           y.arr <- att.delta.arr[c(pre.post_span.vec.ls$pre[c("overall", "key")], 
                                                    as.character(unlist(pre.post.vec.ls)), 
                                                    pre.post_span.vec.ls$post[c("overall", "key")]), 
                                                  , , 
                                                  c(paste("att", 
                                                          c("mean", paste("ci", c("lower", "upper"), sep=".")), 
                                                          sep="_"
                                                          ), 
                                                    "delta_mean"
                                                    )]   # Subset ATT inferences 4d-array to key entries
                           
                           
                           matplot(unlist(pre.post.vec.ls), 
                                   y.arr[as.character(unlist(pre.post.vec.ls)), trs, out, "att_mean"], 
                                   type="b", axes=FALSE, lwd=2, cex=1.25, cex.main=1.25, cex.lab=1, 
                                   col=1, lty=1, pch=19, 
                                   main=paste("Impact on", 
                                              c("Total", "HVLC")[match(typ, table=c("el", "hvlc"))], 
                                              c("Activity Rate", "In-Patient LOS", "Day-Case Ratio"
                                                )[match(out, table=dimnames(y.arr)$outcome)], 
                                              "\nAmong 17+ Year-Old Patients\n", 
                                              ifelse(trs == "Pooled", "Across NHS Trusts", 
                                                     paste("in NHS Trust", trs, sep=" ")
                                                     ), 
                                              sep=" "
                                              ), 
                                   sub=paste(trs, "NHS", ifelse(trs == "Pooled", "Trusts", "Trust"), sep=" "), 
                                   xlab="Months since intervention start", 
                                   ylab=c(bquote("Difference in Activity Rate" %*% "1,000"), 
                                          paste("Difference in", 
                                                c("In-Patient LOS (Days)", "in Day-Case Ratio (%)"), 
                                                sep=" "
                                                )
                                          )[match(out, table=dimnames(y.arr)$outcome)], 
                                   xlim=range(unlist(pre.post.vec.ls)), 
                                   ylim=range(pretty(y.arr[as.character(unlist(pre.post.vec.ls)), trs, out, 
                                                           paste("att_ci", c("lower", "upper"), sep=".")], 
                                                     n=8
                                                     )
                                              )
                                   )  # Time-series plot of ATT
                           
                           axis(1, 
                                at=unlist(pre.post.vec.ls), 
                                labels=c(ifelse((unlist(pre.post.vec.ls)[unlist(pre.post.vec.ls) < 0] + 1) %% 2 == 0, 
                                                sub("(^20)([[:digit:]]{2})(-)([[:digit:]]{2}$)", 
                                                    replacement="\\4-\\2", 
                                                    x=unlist(pre.post.vec.ls)[unlist(pre.post.vec.ls) < 0]
                                                    ), NA
                                                ), 
                                         ifelse((unlist(pre.post.vec.ls)[unlist(pre.post.vec.ls) > 0] - 1) %% 2 == 0, 
                                                sub("(^20)([[:digit:]]{2})(-)([[:digit:]]{2}$)", 
                                                    replacement="\\4-\\2", 
                                                    x=unlist(pre.post.vec.ls)[unlist(pre.post.vec.ls) > 0], 
                                                    ), NA
                                                )
                                         ), 
                                cex.axis=.8
                                )  # Overlay x-axis labels onto time-series plot
                           
                           axis(2, 
                                at=pretty(y.arr[as.character(unlist(pre.post.vec.ls)), trs, out, 
                                                paste("att_ci", c("lower", "upper"), sep=".")], 
                                          n=8
                                          ), 
                                cex.axis=.8
                                )  # Overlay y-axis labels onto time-series plot
                           
                           abline(h=0, col=8, lty=4, lwd=2)  # Overlay ineffectiveness bisector
                           
                           abline(v=0, col=8, lty=4, lwd=2)  # Overlay study period bisector
                           
                           text(-1, 
                                max(pretty(y.arr[as.character(unlist(pre.post.vec.ls)), trs, out, 
                                                 paste("att_ci", c("lower", "upper"), sep=".")], 
                                           n=8
                                           )
                                    ), 
                                labels="Pre", adj=c(.75, 0), cex=1, col=8
                                )  # Overlay pre-intervention period label
                           
                           text(1, 
                                max(pretty(y.arr[as.character(unlist(pre.post.vec.ls)), trs, out, 
                                                 paste("att_ci", c("lower", "upper"), sep=".")], 
                                           n=8
                                           )
                                    ), 
                                labels="Post", adj=c(.25, 0), cex=1, col=8
                                )  # Overlay post-intervention period label
                           
                           polygon(c(unlist(pre.post.vec.ls), rev(unlist(pre.post.vec.ls))), 
                                   c(y.arr[as.character(unlist(pre.post.vec.ls)), trs, out, "att_ci.upper"], 
                                     y.arr[rev(as.character(unlist(pre.post.vec.ls))), trs, out, "att_ci.lower"]
                                     ), 
                                   density=10, angle=45, col=1, border=1
                                   )  # Overlay ATT confidence bounds onto time-series plot
                           
                           segments(pre.post.vec.ls$pre[1], 
                                    y.arr[pre.post_span.vec.ls$pre["overall"], trs, out, "att_mean"], 
                                    pre.post.vec.ls$pre[length(pre.post.vec.ls$pre)], 
                                    y.arr[pre.post_span.vec.ls$pre["overall"], trs, out, "att_mean"], 
                                    col=1, lty=2, lwd=2
                                    )  # Overlay ATT across pre-intervention times onto time-series plot
                           
                           segments(pre.post.vec.ls$post[1], 
                                    y.arr[pre.post_span.vec.ls$post["overall"], trs, out, "att_mean"], 
                                    pre.post.vec.ls$post[length(pre.post.vec.ls$post)], 
                                    y.arr[pre.post_span.vec.ls$post["overall"], trs, out, "att_mean"], 
                                    col=1, lty=2, lwd=2
                                    )  # Overlay ATT across post-intervention times onto time-series plot
                           
                           segments(pre.post_key.vec.ls$pre[length(pre.post_key.vec.ls$pre)], 
                                    y.arr[pre.post_span.vec.ls$pre["key"], trs, out, "att_mean"], 
                                    pre.post_key.vec.ls$pre[1], 
                                    y.arr[pre.post_span.vec.ls$pre["key"], trs, out, "att_mean"], 
                                    col=1, lty=3, lwd=2
                                    )  # Overlay ATT across key pre-intervention times onto time-series plot
                           
                           segments(pre.post_key.vec.ls$post[1], 
                                    y.arr[pre.post_span.vec.ls$post["key"], trs, out, "att_mean"], 
                                    pre.post_key.vec.ls$post[length(pre.post_key.vec.ls$post)], 
                                    y.arr[pre.post_span.vec.ls$post["key"], trs, out, "att_mean"], 
                                    col=1, lty=3, lwd=2
                                    )  # Overlay ATT across key post-intervention times onto time-series plot
                           
                           legend("topright", 
                                  legend=apply(rbind(pre.post_span.vec.ls$pre[c("trs", "key")], 
                                                     pre.post_span.vec.ls$post[c("trs", "key")]
                                                     ), 
                                               MARGIN=2, 
                                               FUN=paste, collapse=", "
                                               ), 
                                  bty="o", lty=2:3, cex=1
                                  )  # Overlay legend with "study period" key onto time-series plot
                           
                           legend("bottomleft", 
                                  legend=as.expression(sapply(pre.post_span.vec.ls$pre[c("trs", "key")], 
                                                              FUN=function(pre) 
                                                                bquote(Delta[.(pre)] == 
                                                                         .(ifelse(y.arr[ifelse(pre == pre.post_span.vec.ls$pre["trs"], 
                                                                                               pre.post_span.vec.ls$pre["overall"], 
                                                                                               pre.post_span.vec.ls$pre["key"]
                                                                                               ), 
                                                                                        trs, out, "delta_mean"] > 0, 
                                                                                  paste0("+", 
                                                                                         sprintf("%1.1f%%", y.arr[ifelse(pre == pre.post_span.vec.ls$pre["trs"], 
                                                                                                                         pre.post_span.vec.ls$pre["overall"], 
                                                                                                                         pre.post_span.vec.ls$pre["key"]
                                                                                                                         ), 
                                                                                                                  trs, out, "delta_mean"]
                                                                                                 )
                                                                                         ), 
                                                                                  sprintf("%1.1f%%", y.arr[ifelse(pre == pre.post_span.vec.ls$pre["trs"], 
                                                                                                                  pre.post_span.vec.ls$pre["overall"], 
                                                                                                                  pre.post_span.vec.ls$pre["key"]
                                                                                                                  ), 
                                                                                                           trs, out, "delta_mean"]
                                                                                          )
                                                                                  )
                                                                           )
                                                                       )
                                                              )
                                                       ), 
                                  bty="o", cex=1
                                  )  # Overlay legend with pre-intervention outcome change keys onto time-series plot
                           
                           legend("bottomright", 
                                  legend=as.expression(sapply(pre.post_span.vec.ls$post[c("trs", "key")], 
                                                              FUN=function(post) 
                                                                bquote(Delta[.(post)] == 
                                                                         .(ifelse(y.arr[ifelse(post == pre.post_span.vec.ls$post["trs"], 
                                                                                               pre.post_span.vec.ls$post["overall"], 
                                                                                               pre.post_span.vec.ls$post["key"]
                                                                                               ), 
                                                                                        trs, out, "delta_mean"] > 0, 
                                                                                  paste0("+", 
                                                                                         sprintf("%1.1f%%", y.arr[ifelse(post == pre.post_span.vec.ls$post["trs"], 
                                                                                                                         pre.post_span.vec.ls$post["overall"], 
                                                                                                                         pre.post_span.vec.ls$post["key"]
                                                                                                                         ), 
                                                                                                                  trs, out, "delta_mean"]
                                                                                                 )
                                                                                         ), 
                                                                                  sprintf("%1.1f%%", y.arr[ifelse(post == pre.post_span.vec.ls$post["trs"], 
                                                                                                                  pre.post_span.vec.ls$post["overall"], 
                                                                                                                  pre.post_span.vec.ls$post["key"]
                                                                                                                  ), 
                                                                                                           trs, out, "delta_mean"]
                                                                                          )
                                                                                  )
                                                                           )
                                                                       )
                                                              )
                                                       ), 
                                  bty="o", cex=1
                                  )  # Overlay legend with post-intervention outcome change keys onto time-series plot
                           
                           dev.print(png, 
                                     file=file.path(output_internal_dir, 
                                                    "GSynth analysis/Inference/Established hub", 
                                                    paste("old_hub", 
                                                          paste(typ, out, sep="."), 
                                                          trs, "att.delta.png", 
                                                          sep="_"
                                                          )
                                                    ), 
                                     width=1024, height=768
                                     )  # Export time-series plot in .png format
                           
                           # unlink(file.path(output_internal_dir, 
                           #                  "GSynth%20analysis/Inference/Established%20hub", 
                           #                  paste("old_hub", 
                           #                        paste(typ, out, sep="."), 
                           #                        trs, "att.delta.png", 
                           #                        sep="_"
                           #                        ), 
                           #                  "att.delta.png",
                           #                  sep="_"
                           #                  )
                           #        )  # Remove .png file of time-series plots
                           }
                         )
                )
         }
       )


#########################
## Time-series plots   ##
## of key intervention ##
## vs counterfactual   ##
## outcome trends      ##
#########################

## Elective and HVLC 
## outcomes

load(file.path(output_internal_dir, 
               "GSynth analysis/Inference/Established hub/old_att.delta_inf.RData"
               )
     )  # Load ATT and outcome change inferences data-frames for established hub trusts vs non-hub trusts as .RData dump


out.delta.frm <- as.formula(paste(paste0("cbind(", 
                                         paste(c(paste("out", c("tr", "ct"), sep="_"), 
                                                 paste("out_ct", c("ci.lower", "ci.upper"), sep="_"), 
                                                 "delta_mean"
                                                 ), 
                                               collapse=", "
                                               ), 
                                         ")"
                                         ), 
                                  paste(c("time", "unit", "outcome"), collapse=" + "), 
                                  sep=" ~ "
                                  )
                            )  # Set formula to format outcomes inferences data-frames into 4d-arrays by time, unit, outcome, statistic


lapply(names(old_el.hvlc_att.delta_inf.ls2), 
       FUN=function(typ)
         {
         # typ <- c("el", "hvlc")[2]; typ
         # out <- dimnames(out.delta.arr)$outcome[2]; out
         # trs <- dimnames(out.delta.arr)$unit[5]; trs
         # rm(typ, out, trs)
         
         out.delta.dat <- do.call(rbind, 
                                  args=old_el.hvlc_att.delta_inf.ls2[[typ]]
                                  )  # Bind by row outcome inferences data-frames across outcomes
         
         out.delta.dat$outcome <- factor(out.delta.dat$outcome, 
                                         labels=c("dcr", "lhs", "act")
                                         )  # Format "outcome" as factor and rename factor levels
         
         out.delta.dat$outcome <- factor(out.delta.dat$outcome, 
                                         levels=c("act", "lhs", "dcr")
                                         )  # Rearrange "outcome" factor levels
         
         
         out.delta.arr <- xtabs(out.delta.frm, 
                                data=out.delta.dat
                                )  # Format outcome inferences data-frame into 4d-array by time, unit, outcome, statistic
         
         names(dimnames(out.delta.arr))[names(dimnames(out.delta.arr)) == ""] <- "statistic"  # Rename blank "statistic" margin of outcome inferences 4d-array
         
         
         lapply(dimnames(out.delta.arr)$outcome, 
                FUN=function(out)
                  {
                  lapply(dimnames(out.delta.arr)$unit, 
                         FUN=function(trs)
                           {
                           pre.post.vec.ls <- with(old_el.hvlc_gsynth.mdl.ls2[[typ]][[out]], 
                                                   expr=list(pre=-rev(seq.int(ifelse(trs == "Pooled", max(T0), T0[trs == id.tr]))), 
                                                             post=seq.int(T - ifelse(trs == "Pooled", min(T0), T0[trs == id.tr]))
                                                             )
                                                   )  # Derive list by study period of study times vectors
                           
                           pre.post_span.vec.ls <- with(old_el.hvlc_gsynth.mdl.ls2[[typ]][[out]], 
                                                        expr=list(pre=c(overall=paste(-1, -max(T0), sep=" - "), 
                                                                        trs=paste(rev(range(pre.post.vec.ls$pre)), collapse=" - "), 
                                                                        key=paste(rev(range(pre.post_key.vec.ls$pre)), collapse=" - ")
                                                                        ), 
                                                                  post=c(overall=paste(1, T - min(T0), sep=" - "), 
                                                                         trs=paste(range(pre.post.vec.ls$post), collapse=" - "), 
                                                                         key=paste(range(pre.post_key.vec.ls$post), collapse=" - ")
                                                                         )
                                                                  )
                                                        )  # Derive list by study period of study time spans of interest
                           
                           y.arr <- out.delta.arr[c(pre.post_span.vec.ls$pre[c("overall", "key")], 
                                                    as.character(unlist(pre.post.vec.ls)), 
                                                    pre.post_span.vec.ls$post[c("overall", "key")]), 
                                                  , , ]   # Subset outcome inferences 4d-array to key entries
                           
                           
                           matplot(unlist(pre.post.vec.ls), 
                                   y.arr[as.character(unlist(pre.post.vec.ls)), trs, out, 
                                         paste("out", c("tr", "ct"), sep="_")
                                         ], 
                                   type="b", axes=FALSE, lwd=2, cex=1.25, cex.main=1.25, cex.lab=1, 
                                   col=c(4, 2), lty=1, pch=19, 
                                   main=paste(c("Total", "HVLC"
                                                )[match(typ, table=c("el", "hvlc"))], 
                                              c("Activity Rate", "In-Patient LOS", "Day-Case Ratio"
                                                )[match(out, table=dimnames(y.arr)$outcome)], 
                                              "\nAmong 17+ Year-Old Patients\n", 
                                              ifelse(trs == "Pooled", "Across NHS Trusts", 
                                                     paste("in NHS Trust", trs, sep=" ")
                                                     ), 
                                              sep=" "
                                              ), 
                                   sub=paste(trs, "NHS", ifelse(trs == "Pooled", "Trusts", "Trust"), sep=" "), 
                                   xlab="Months since intervention start", 
                                   ylab=c(bquote("Activity Rate" %*% "1,000"), 
                                          "In-Patient LOS (Days)", 
                                          "Day-Case Ratio (%)"
                                          )[match(out, table=dimnames(y.arr)$outcome)], 
                                   xlim=range(unlist(pre.post.vec.ls)), 
                                   ylim=range(pretty(y.arr[as.character(unlist(pre.post.vec.ls)), trs, out, 
                                                           c("out_tr", 
                                                             paste("out_ct_ci", c("lower", "upper"), sep=".")
                                                             )
                                                           ], 
                                                     n=8
                                                     )
                                              )
                                   )  # Time-series plot of intervention vs counterfactual outcomes
                           
                           axis(1, 
                                at=unlist(pre.post.vec.ls), 
                                labels=c(ifelse((unlist(pre.post.vec.ls)[unlist(pre.post.vec.ls) < 0] + 1) %% 2 == 0, 
                                                sub("(^20)([[:digit:]]{2})(-)([[:digit:]]{2}$)", 
                                                    replacement="\\4-\\2", 
                                                    x=unlist(pre.post.vec.ls)[unlist(pre.post.vec.ls) < 0]
                                                    ), NA
                                                ), 
                                         ifelse((unlist(pre.post.vec.ls)[unlist(pre.post.vec.ls) > 0] - 1) %% 2 == 0, 
                                                sub("(^20)([[:digit:]]{2})(-)([[:digit:]]{2}$)", 
                                                    replacement="\\4-\\2", 
                                                    x=unlist(pre.post.vec.ls)[unlist(pre.post.vec.ls) > 0], 
                                                    ), NA
                                                )
                                         ), 
                                cex.axis=.8
                                )  # Overlay x-axis labels onto time-series plot
                           
                           axis(2, 
                                at=pretty(y.arr[as.character(unlist(pre.post.vec.ls)), trs, out, 
                                                c("out_tr", 
                                                  paste("out_ct_ci", c("lower", "upper"), sep=".")
                                                  )
                                                ], 
                                          n=8
                                          ), 
                                cex.axis=.8
                                )  # Overlay y-axis labels onto time-series plot
                           
                           abline(v=0, col=8, lty=4, lwd=2)  # Overlay study period bisector
                           
                           text(-1, 
                                max(pretty(y.arr[as.character(unlist(pre.post.vec.ls)), trs, out, 
                                                 c("out_tr", 
                                                   paste("out_ct_ci", c("lower", "upper"), sep=".")
                                                   )
                                                 ], 
                                           n=8
                                           )
                                    ), 
                                labels="Pre", adj=c(.75, 0), cex=1, col=8
                                )  # Overlay pre-intervention period label
                           
                           text(1, 
                                max(pretty(y.arr[as.character(unlist(pre.post.vec.ls)), trs, out, 
                                                 c("out_tr", 
                                                   paste("out_ct_ci", c("lower", "upper"), sep=".")
                                                   )
                                                 ], 
                                           n=8
                                           )
                                    ), 
                                labels="Post", adj=c(.25, 0), cex=1, col=8
                                )  # Overlay post-intervention period label
                           
                           polygon(c(unlist(pre.post.vec.ls), rev(unlist(pre.post.vec.ls))), 
                                   c(y.arr[as.character(unlist(pre.post.vec.ls)), trs, out, "out_ct_ci.upper"], 
                                     y.arr[rev(as.character(unlist(pre.post.vec.ls))), trs, out, "out_ct_ci.lower"]
                                     ), 
                                   density=10, angle=45, col=2, border=2
                                   )  # Overlay counterfactual outcome confidence bounds onto time-series plot
                           
                           segments(pre.post.vec.ls$pre[1], 
                                    y.arr[pre.post_span.vec.ls$pre["overall"], trs, out, paste("out", c("tr", "ct"), sep="_")], 
                                    pre.post.vec.ls$pre[length(pre.post.vec.ls$pre)], 
                                    y.arr[pre.post_span.vec.ls$pre["overall"], trs, out, paste("out", c("tr", "ct"), sep="_")], 
                                    col=c(4, 2), lty=2, lwd=2
                                    )  # Overlay mean outcomes across pre-intervention times onto time-series plot
                           
                           segments(pre.post.vec.ls$post[1], 
                                    y.arr[pre.post_span.vec.ls$post["overall"], trs, out, paste("out", c("tr", "ct"), sep="_")], 
                                    pre.post.vec.ls$post[length(pre.post.vec.ls$post)], 
                                    y.arr[pre.post_span.vec.ls$post["overall"], trs, out, paste("out", c("tr", "ct"), sep="_")], 
                                    col=c(4, 2), lty=2, lwd=2
                                    )  # Overlay mean outcomes across post-intervention times onto time-series plot
                           
                           segments(pre.post_key.vec.ls$pre[length(pre.post_key.vec.ls$pre)], 
                                    y.arr[pre.post_span.vec.ls$pre["key"], trs, out, paste("out", c("tr", "ct"), sep="_")], 
                                    pre.post_key.vec.ls$pre[1], 
                                    y.arr[pre.post_span.vec.ls$pre["key"], trs, out, paste("out", c("tr", "ct"), sep="_")], 
                                    col=c(4, 2), lty=3, lwd=2
                                    )  # Overlay mean outcomes across key pre-intervention times onto time-series plot
                           
                           segments(pre.post_key.vec.ls$post[1], 
                                    y.arr[pre.post_span.vec.ls$post["key"], trs, out, paste("out", c("tr", "ct"), sep="_")], 
                                    pre.post_key.vec.ls$post[length(pre.post_key.vec.ls$post)], 
                                    y.arr[pre.post_span.vec.ls$post["key"], trs, out, paste("out", c("tr", "ct"), sep="_")], 
                                    col=c(4, 2), lty=3, lwd=2
                                    )  # Overlay mean outcomes across key post-intervention times onto time-series plot
                           
                           legend("topleft", 
                                  legend=c("Trust hub", "Counterfactual"), 
                                  col=c(4, 2), lty=1, pch=19, 
                                  bty="n", cex=1
                                  )  # Overlay legend with "type" key onto time-series plot
                           
                           legend("topright", 
                                  legend=apply(rbind(pre.post_span.vec.ls$pre[c("trs", "key")], 
                                                     pre.post_span.vec.ls$post[c("trs", "key")]
                                                     ), 
                                               MARGIN=2, 
                                               FUN=paste, collapse=", "
                                               ), 
                                  bty="o", lty=2:3, cex=1
                                  )  # Overlay legend with "study period" key onto time-series plot
                           
                           legend("bottomleft", 
                                  legend=as.expression(sapply(pre.post_span.vec.ls$pre[c("trs", "key")], 
                                                              FUN=function(pre) 
                                                                bquote(Delta[.(pre)] == 
                                                                         .(ifelse(y.arr[ifelse(pre == pre.post_span.vec.ls$pre["trs"], 
                                                                                               pre.post_span.vec.ls$pre["overall"], 
                                                                                               pre.post_span.vec.ls$pre["key"]
                                                                                               ), 
                                                                                        trs, out, "delta_mean"] > 0, 
                                                                                  paste0("+", 
                                                                                         sprintf("%1.1f%%", y.arr[ifelse(pre == pre.post_span.vec.ls$pre["trs"], 
                                                                                                                         pre.post_span.vec.ls$pre["overall"], 
                                                                                                                         pre.post_span.vec.ls$pre["key"]
                                                                                                                         ), 
                                                                                                                  trs, out, "delta_mean"]
                                                                                                 )
                                                                                         ), 
                                                                                  sprintf("%1.1f%%", y.arr[ifelse(pre == pre.post_span.vec.ls$pre["trs"], 
                                                                                                                  pre.post_span.vec.ls$pre["overall"], 
                                                                                                                  pre.post_span.vec.ls$pre["key"]
                                                                                                                  ), 
                                                                                                           trs, out, "delta_mean"]
                                                                                          )
                                                                                  )
                                                                           )
                                                                       )
                                                              )
                                                       ), 
                                  bty="o", cex=1
                                  )  # Overlay legend with pre-intervention outcome change keys onto time-series plot
                           
                           legend("bottomright", 
                                  legend=as.expression(sapply(pre.post_span.vec.ls$post[c("trs", "key")], 
                                                              FUN=function(post) 
                                                                bquote(Delta[.(post)] == 
                                                                         .(ifelse(y.arr[ifelse(post == pre.post_span.vec.ls$post["trs"], 
                                                                                               pre.post_span.vec.ls$post["overall"], 
                                                                                               pre.post_span.vec.ls$post["key"]
                                                                                               ), 
                                                                                        trs, out, "delta_mean"] > 0, 
                                                                                  paste0("+", 
                                                                                         sprintf("%1.1f%%", y.arr[ifelse(post == pre.post_span.vec.ls$post["trs"], 
                                                                                                                         pre.post_span.vec.ls$post["overall"], 
                                                                                                                         pre.post_span.vec.ls$post["key"]
                                                                                                                         ), 
                                                                                                                  trs, out, "delta_mean"]
                                                                                                 )
                                                                                         ), 
                                                                                  sprintf("%1.1f%%", y.arr[ifelse(post == pre.post_span.vec.ls$post["trs"], 
                                                                                                                  pre.post_span.vec.ls$post["overall"], 
                                                                                                                  pre.post_span.vec.ls$post["key"]
                                                                                                                  ), 
                                                                                                           trs, out, "delta_mean"]
                                                                                          )
                                                                                  )
                                                                           )
                                                                       )
                                                              )
                                                       ), 
                                  bty="o", cex=1
                                  )  # Overlay legend with post-intervention outcome change keys onto time-series plot
                           
                           dev.print(png, 
                                     file=file.path(output_internal_dir, 
                                                    "GSynth analysis/Inference/Established hub", 
                                                    paste(paste(typ, out, sep="."), 
                                                          trs, "out.delta.png", 
                                                          sep="_"
                                                          )
                                                    ), 
                                     width=1024, height=768
                                     )  # Export time-series plot in .png format
                           
                           # unlink(file.path(output_internal_dir,
                           #                  "GSynth%20analysis/Inference/Established%20hub",
                           #                  paste(paste(typ, out, sep="."), 
                           #                        trs, "out.delta.png", 
                           #                        sep="_"
                           #                        )
                           #                  )
                           #        )  # Remove .png file of time-series plots
                           }
                         )
                  }
                )
         }
       )