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
    outdir = "../full-paper/Images/";
        tmp = read.table(paste(dirname, filename, ".txt", sep=""), sep="\t");
        colnames(tmp) = c("dataset", "size", "views", "x", "n_gb", "latency", "dbms", "n_dist")
        tmp$views = tmp$views * tmp$x;
	    tmp$groups = 10^tmp$n_gb;
        tmp$dbms = factor(tmp$dbms);
        tmp$n_dist = factor(tmp$n_dist);
        tmp$groups = factor(paste(tmp$dbms, tmp$n_dist, sep=","));
	    cols = gg_color_hue(2)
        ggplot(tmp[tmp$n_gb<=10,], aes(n_gb, latency/1000, color=dbms, group=groups))  + theme_bw() + 
        geom_line(aes(linetype=n_dist), size=1.5) + geom_point(size=2) + theme(text = element_text(size=24)) + 
        geom_text(data=NULL, aes(2,1.5,label="100"),color=cols[1]) +
        geom_text(data=NULL, aes(1,7,label="100"),color=cols[1]) +
        geom_text(data=NULL, aes(4,13,label="10000"),color=cols[2]) +
        geom_text(data=NULL, aes(1.2,27,label="10000"),color=cols[2]) +
        ylab("latency (s)") + xlab("Number of group by attrs") + scale_fill_brewer(palette="Paired");
	   ggsave(file=paste(outdir, filename, ".pdf", sep=""),width=7,height=5);
}

multi_gb("multi_gb_same");
