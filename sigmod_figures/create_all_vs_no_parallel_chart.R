parallel <- function(filename) {
        tmp = read.table(filename, sep="\t");
        colnames(tmp) = c("dataset", "size", "n_conn", "latency", "opt", "dbms");
        tmp$dbms = factor(tmp$dbms);
        ggplot(tmp, aes(n_conn, latency/1000, color=dbms)) +  theme_bw() + geom_line() + geom_point()  + scale_fill_brewer(palette="Paired");
        ggsave(file=paste(dirname, filename, "_latency.pdf", sep=""),width=7,height=5);
}

parallel("all_vs_no_parallel.txt");
