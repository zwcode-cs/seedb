
plot_params <- function(filename,outfile,xmin,xmax,ymin,ymax) {
    outdirname="../full-paper/Images/";
    dirname="";
	tmp=read.table(paste(dirname, filename, ".txt", sep=""), sep="\t");
	colnames(tmp) = c("dataset", "k", "ci", "latency", "accuracy");
	ggplot(tmp, aes(x=accuracy, y=latency/1000, label=ci)) +  theme_bw() + 
		geom_text(size=8,hjust=0.4, vjust=1.5) + geom_point(size=4) + 
		theme(text = element_text(size=24)) + ylab("latency(s)") + ylim(ymin,ymax) + xlim(xmin,xmax);
	ggsave(file=paste(outdirname, outfile, ".pdf", sep=""));
}
plot_params("dia_ci_heuristic","latency_vs_accuracy_ci",.5,.75,10,13);
plot_params("dia_mab_heuristic","latency_vs_accuracy_mab",.1,1,4,18);

