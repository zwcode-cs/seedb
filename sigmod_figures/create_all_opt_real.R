all_opt_real2 <- function(filename) {
        dirname="";
        outdirname="../full-paper/Images/"
        tmp = read.table(paste(dirname, filename, ".txt", sep=""), sep="\t");
        colnames(tmp) = c("dataset", "opt", "latency");
	tmp$dataset = factor(tmp$dataset);
	tmp$dataset = ordered(tmp$dataset, levels = c("BANK", "DIAB", "AIR", "AIR10"));
	tmp$opt = factor(tmp$opt)
	tmp$opt = ordered(tmp$opt, levels = c("NO_OPT", "SHARING", "SH_PR_FULL", "SH_PR_EARLY"))
    ggplot(tmp, aes(opt, latency)) + 
		geom_bar(aes(fill=opt), position="dodge", stat="identity") + 
		ylab("latency (ms)") +  theme_bw() + xlab("Datasets") + scale_y_log10() + facet_grid(. ~ dataset) +
		theme(axis.text.x = element_blank(), axis.ticks = element_blank()) +
		theme(text = element_text(size=24))  + scale_fill_brewer(palette="Paired");
		ggsave(file=paste(outdirname, filename, ".pdf", sep=""),width=7,height=5);
}

all_opt_real <- function(filename) {
    dirname="";
    outdirname="../full-paper/Images/"
    tmp = read.table(paste(dirname, filename, ".txt", sep=""), sep="\t");
    colnames(tmp) = c("dataset", "opt", "latency");
	tmp$dataset = factor(tmp$dataset);
	tmp$dataset = ordered(tmp$dataset, levels = c("BANK", "DIAB", "AIR", "AIR10"));
	tmp$opt = factor(tmp$opt)
	tmp$opt = ordered(tmp$opt, levels = c("NO_OPT", "SHARING", "COMB", "COMB_EARLY"))
    
    helper(tmp, "BANK", outdirname, filename);
    helper(tmp, "DIAB", outdirname, filename);
    helper(tmp, "AIR", outdirname, filename);
    helper(tmp, "AIR10", outdirname, filename);
    #multiplot(p1, p2, cols=2);
}

helper <- function(tmp, dataset, outdirname, filename) {
	tmp = tmp[tmp$dataset==dataset,];
	tmp
	p = ggplot(tmp, aes(opt, latency/1000)) + 
		geom_bar(aes(fill=opt), position="dodge", stat="identity") + guides(fill=FALSE) +
		ylab("latency (s)") +  theme_bw() + xlab("") + #scale_y_log10() +
		theme(text = element_text(size=24))  + scale_fill_brewer(palette="Paired");
		ggsave(file=paste(outdirname, filename, "_", dataset, ".pdf", sep=""),width=7,height=5);
	p;
}

all_opt_real("all_opt_real_data_row");
all_opt_real("all_opt_real_data_col");
