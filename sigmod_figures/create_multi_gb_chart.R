old <- function() {
	tmp = read.table("multi_gb.txt", sep="\t")
	colnames(tmp) = c("dataset", "size", "dims", "measures", "n_GB", "latency", "dbms")
	tmp$dbms = factor(tmp$dbms)
	tmp$n_GB = factor(tmp$n_GB)
	ggplot(tmp, aes(n_GB, latency/1000)) +  theme_bw() + geom_bar(aes(fill=dbms, ordered=TRUE), position="dodge", stat="identity") + ylab("latency (s)") + scale_fill_brewer();
	ggsave(file="in_memory_latency.pdf")
}
gg_color_hue <- function(n) {
  hues = seq(15, 375, length=n+1)
  hcl(h=hues, l=65, c=100)[1:n]
}

multi_gb <- function(filename) {
        dirname="";
        tmp = read.table(paste(dirname, filename, ".txt", sep=""), sep="\t");
        colnames(tmp) = c("dataset", "size", "views", "x", "n_gb", "latency", "dbms")
        tmp$views = tmp$views * tmp$x;
	tmp$groups = 10^tmp$n_gb;
        tmp$dbms = factor(tmp$dbms);
	cols = gg_color_hue(2)
        #ggplot(tmp, aes(n_gb, latency/1000, color=dbms)) + geom_line() + geom_point() + theme(text = element_text(size=24)) + ylab("latency(s)") + geom_hline(aes(yintercept=195), color=cols[2], linetype="dashed") + geom_hline(aes(yintercept=123), color=cols[1], linetype="dashed");
        ggplot(tmp, aes(groups, latency/1000, color=dbms))  + theme_bw() + geom_line() + geom_point() + theme(text = element_text(size=24)) + ylab("latency(s)") + scale_x_log10(name="Number of groups") + scale_fill_brewer();
	ggsave(file=paste(dirname, filename, ".pdf", sep=""));
}

multi_gb("multi_gb");
multi_gb("multi_gb_same");
