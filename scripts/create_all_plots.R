library(dplyr);
library(ggplot2);
library(reshape2);

dirname = "/Users/manasi/Public/seedb_results/gaussian_test1/";
# read all files in directory
files = list.files(dirname, pattern="*.txt", recursive=FALSE);

for (file in files) {
  tmp = read.csv(paste(dirname, file, sep=""), header=T, row.names=NULL);
  id = substr(file, 0, nchar(file)-4);
  dimension = strsplit(id, "__")[[1]][1]; # populate dimension
  measure = strsplit(id, "__")[[1]][2]; # populate measure
  tmp_melted = melt(tmp, dimension);
  tmp_melted$gb = tmp_melted[, dimension];
  #ggplot(tmp_melted, aes(x = variable, y = value, fill=variable)) + ylab(substr(measure, 9, nchar(measure))) + xlab(substr(dimension, 5, nchar(dimension))) + geom_bar(stat="identity") + facet_grid(. ~ gb) + theme(axis.text.x = element_blank(), axis.ticks = element_blank(), strip.text.x = element_text(size=8, angle=90)) + guides(fill=guide_legend(title=NULL));
  ggplot(tmp_melted, aes(x = variable, y = value, fill=variable)) + ylab("fraction of SUM(Y)") + xlab("X") + 
  	geom_bar(stat="identity") + facet_grid(. ~ gb) + theme(axis.text.x = element_blank(), axis.ticks = element_blank(),
  	strip.text.x = element_blank()) + guides(fill=guide_legend(title=NULL));
  ggsave(paste("seedb_", dimension, "_", measure, ".png", sep=""));
}
