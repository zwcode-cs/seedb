library(dplyr);
library(ggplot2);
library(reshape2);

dirname = "/Users/manasi/Public/seedb_results/";
# read all files in directory
files = list.files("/Users/manasi/Public/seedb_results/", pattern="*.txt", recursive=FALSE);

for (file in files) {
  tmp = read.csv(paste(dirname, file, sep=""), header=T, row.names=NULL);
  id = substr(file, 0, nchar(file)-4);
  dimension = strsplit(id, "__")[[1]][1]; # populate dimension
  measure = strsplit(id, "__")[[1]][2]; # populate measure
  tmp_melted = melt(tmp, dimension);
  tmp_melted$gb = tmp_melted[, dimension];
  ggplot(tmp_melted, aes(x = variable, y = value, fill=variable)) + geom_bar(stat="identity") + facet_grid(. ~ gb) + theme(axis.text.x = element_blank(), axis.ticks = element_blank(), strip.text.x = element_text(size=8, angle=75));
  ggsave(paste("seedb_", dimension, "_", measure, ".png", sep=""));
}
