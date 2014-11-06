parallel <- function(filename) {
        dirname="";
        outdirname="../full-paper/Images/"
        tmp = read.table(paste(dirname, filename, ".txt", sep=""), sep="\t");
        colnames(tmp) = c("dataset", "size", "n_conn", "latency", "dbms");
        tmp$dbms = factor(tmp$dbms);
        #ggplot(tmp, aes(n_conn, latency/1000, color=dbms)) + geom_line() + geom_point() + ylab("latency (s)") + 
		#xlab("Num Parallel Queries") + theme(text = element_text(size=24));
        #ggsave(file=paste(dirname, filename, ".pdf", sep=""));
        tmp$n_conn <- as.character(tmp$n_conn)
        tmp$n_conn <- factor(tmp$n_conn, levels=unique(tmp$n_conn))

        ggplot(tmp, aes(n_conn, latency/1000)) + 
		geom_bar(aes(fill=dbms), position="dodge", stat="identity") + 
		ylab("latency (s)") +  theme_bw() + xlab("Num Parallel Queries") +
		theme(text = element_text(size=24))  + scale_fill_brewer(palette="Paired");
		ggsave(file=paste(outdirname, filename, ".pdf", sep=""));
}

parallel("parallel_noop");
