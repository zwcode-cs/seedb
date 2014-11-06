parallel <- function(filename) {
        dirname="/Users/manasi/Documents/workspace/seedb/sigmod_figures/";
        tmp = read.table(paste(dirname, filename, sep=""), sep="\t");
        colnames(tmp) = c("dataset", "size", "n_conn", "latency", "opt" "dbms");
        tmp$dbms = factor(tmp$dbms);
        ggplot(tmp, aes(n_conn, latency/1000, color=dbms)) + geom_line() + geom_point();
        ggsave(file=paste(dirname, filename, "_latency.pdf", sep=""));
}

parallel("all_vs_no_parallel.txt");
