old <- function() {
	tmp = read.table("/Users/manasi/Documents/workspace/seedb/sigmod_figures/multiagg.txt", sep="\t")
	colnames(tmp) = c("dataset", "size", "views", "x", "n_agg", "latency", "dbms")
	tmp$views = tmp$views * tmp$x
	tmp$dbms = factor(tmp$dbms)
	tmp$views = factor(tmp$views)
	tmp$n_agg = factor(tmp$n_agg)
	ggplot(tmp, aes(n_agg, latency/1000)) + geom_bar(aes(fill=dbms, ordered=TRUE), position="dodge", stat="identity") + ylab("latency (s)")+geom_hline(yintercept=3600)
	ggsave(file="/Users/manasi/Documents/workspace/seedb/sigmod_figures/multi_agg.pdf")
}

multi_agg <- function(filename) {
    dirname="/Users/manasi/Documents/workspace/seedb/sigmod_figures/";
    outdirname="/Users/manasi/Documents/workspace/seedb/full-paper/Images/";
    tmp = read.table(paste(dirname, filename, ".txt", sep=""), sep="\t");
    colnames(tmp) = c("dataset", "size", "views", "x", "n_agg", "latency", "dbms")
	tmp$views = tmp$views * tmp$x;
	tmp$dbms = factor(tmp$dbms);

	ggplot(tmp, aes(n_agg, latency/1000)) + 
		geom_bar(aes(fill=dbms, ordered=TRUE), position="dodge", stat="identity") + 
		ylab("latency (s)") + xlab("Num Aggregate Attributes") + scale_y_log10() + 
		theme(text = element_text(size=24));
		ggsave(file=paste(outdirname, filename, ".pdf", sep=""));

	#ggplot(tmp, aes(n_agg, latency/1000, color=dbms)) + 
	#	geom_line() + 
	#	geom_point() + 
	#	ylab("latency (s)") +
	#	theme(text = element_text(size=24));
	#ggsave(file=paste(, 
	#	filename, ".pdf", sep=""));
}

multi_agg("multi_agg");
