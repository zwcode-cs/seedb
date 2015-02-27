all_opt_real <- function(filename) {
        dirname="";
        outdirname="../full-paper/Images/"
        tmp = read.table(paste(dirname, filename, ".txt", sep=""), sep="\t");
        colnames(tmp) = c("rows", "views", "latency", "dbms", "dataset");
        tmp$dbms = factor(tmp$dbms);
	tmp$dataset = factor(tmp$dataset);
	tmp$dataset = ordered(tmp$dataset, levels = c("BANK", "DIAB", "AIR", "AIR10"));
        ggplot(tmp, aes(dataset, latency/1000)) + 
		geom_bar(aes(fill=dbms), position="dodge", stat="identity") + 
		ylab("latency (s)") +  theme_bw() + xlab("Datasets") +
		theme(text = element_text(size=24))  + scale_fill_brewer(palette="Paired");
		ggsave(file=paste(outdirname, filename, ".pdf", sep=""),width=7,height=5);
}

all_opt_real("all_opt_real_data");
