all_opt_real <- function(filename) {
        dirname="";
        outdirname="../full-paper/Images/"
        tmp = read.table(paste(dirname, filename, ".txt", sep=""), sep="\t");
        colnames(tmp) = c("dataset", "opt", "latency");
	tmp$dataset = factor(tmp$dataset);
	tmp$dataset = ordered(tmp$dataset, levels = c("BANK", "DIAB", "AIR", "AIR10"));
	tmp$opt = factor(tmp$opt)
	tmp$opt = ordered(tmp$opt, levels = c("NO_OPT", "SHARING", "SH_PR_FULL", "SH_PR_EARLY"))
        ggplot(tmp, aes(dataset, latency)) + 
		geom_bar(aes(fill=opt), position="dodge", stat="identity") + 
		ylab("latency (s)") +  theme_bw() + xlab("Datasets") + scale_y_log10() + 
		theme(text = element_text(size=24))  + scale_fill_brewer(palette="Paired");
		ggsave(file=paste(outdirname, filename, ".pdf", sep=""),width=7,height=5);
}

all_opt_real("all_opt_real_data_row");
all_opt_real("all_opt_real_data_col");
