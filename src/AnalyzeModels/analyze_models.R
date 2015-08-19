# set working directory to source file location before running
tagData <- read.csv("result_summary.csv")
tagData$AUPRC <- sapply(tagData$AUPRC, round, digits=2)
tagData$AUROC <- sapply(tagData$AUROC, round, digits=2)
summary(tagData, digits=2)
summary(tagData[,c("AUROC","AUPRC","Time.to.Train")], digits=2)
write.csv(tagData, "result_summary_rounded.csv", quote=F, row.names=F)
