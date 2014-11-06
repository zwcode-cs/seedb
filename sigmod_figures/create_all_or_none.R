
all_or_none <- function(filename) {
	dirname="/Users/manasi/Documents/workspace/seedb/sigmod_figures/";
	outdirname="/Users/manasi/Documents/workspace/seedb/full-paper/Images/";
	tmp = read.table(paste(dirname, filename, ".txt", sep=""), sep="\t");
	colnames(tmp) = c("size", "views", "x", "latency", "opt", "sel");
	tmp$views = tmp$views * tmp$x;
	tmp$opt = factor(tmp$opt);
	tmp$views = factor(tmp$views);
	tmp$size=factor(tmp$size, levels(tmp$size)[c(1, 3, 2)]);

	tmp1 = tmp[tmp$sel=='views',];
	ggplot(tmp1, aes(views, latency/1000)) + 
		geom_bar(aes(fill=opt, ordered=TRUE), position="dodge", stat="identity") + 
		ylab("latency (s)") + 
		theme(text = element_text(size=24));
	ggsave(file=paste(outdirname, filename, "_by_views.pdf", sep=""));
	tmp1 = tmp[tmp$sel=='size',];
	ggplot(tmp1, aes(size, latency/1000)) + 
		geom_bar(aes(fill=opt, ordered=TRUE), position="dodge", stat="identity") + 
		ylab("latency (s)") + 
		theme(text = element_text(size=24));
	ggsave(file=paste(outdirname, filename, "_by_size.pdf", sep=""));
}

