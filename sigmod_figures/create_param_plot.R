
plot_params <- function(filename) {
	tmp=read.table(paste(dirname, filename, ".txt", sep=""), sep="\t");
	colnames(tmp) = c("dataset", "k", "ci", "latency", "accuracy");
	ggplot(tmp, aes(x=accuracy, y=latency/1000, label=ci)) + 
		geom_text(size=8,hjust=0.4, vjust=-0.2) + geom_point() + 
		theme(text = element_text(size=24)) + ylab("latency(s)");
	ggsave(paste(outdirname, filename, ".pdf", sep=""));
}
