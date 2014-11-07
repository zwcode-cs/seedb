gg_color_hue <- function(n) {
  hues = seq(15, 375, length=n+1)
  hcl(h=hues, l=65, c=100)[1:n]
}

multi_gb <- function(filename) {
    dirname="";
    outdir = "../full-paper/Images/";
        tmp = read.table(paste(dirname, filename, ".txt", sep=""), sep="\t");
        colnames(tmp) = c("dataset", "size", "views", "x", "n_gb", "latency", "dbms", "algo", "sz")
        tmp$views = tmp$views * tmp$x;
	tmp$groups = 10^tmp$n_gb;
        tmp$dbms = factor(tmp$dbms);
        tmp$g = factor(paste(tmp$dbms, tmp$bp, tmp$sz))
	cols = gg_color_hue(2)
                                        #ggplot(tmp, aes(n_gb, latency/1000, color=dbms)) + geom_line() + geom_point() + theme(text = element_text(size=24)) + ylab("latency(s)") + geom_hline(aes(yintercept=195), color=cols[2], linetype="dashed") + geom_hline(aes(yintercept=123), color=cols[1], linetype="dashed");
        ggplot(tmp, aes(groups, latency/1000, color=dbms, group=g))  + theme_bw() + 
            geom_line(aes(linetype=algo), size=1.5) + geom_point(size=2) + theme(text = element_text(size=24)) + 
            ylab("latency (s)") + scale_x_log10(name="Number of groups") + 
            scale_fill_brewer(palette="Paired") + 

            geom_text(data=NULL, aes(10^2,145,label="100000"), color=cols[1]) + 
            geom_text(data=NULL, aes(10^18,215,label="100000"),color=cols[2]) +
            geom_text(data=NULL, aes(10^2,55,label="1000"), color=cols[1]) + 
            geom_text(data=NULL, aes(10^18,165,label="1000"),color=cols[2]) +
            #geom_text(data=NULL, aes(10^22,140,label="Col. bin packing"), color=cols[1]) + 
            #geom_text(data=NULL, aes(10^22,210,label="Row. bin packing"),color=cols[2]);
	       ggsave(file=paste(outdir, filename, ".pdf", sep=""),width=7,height=5);
}

multi_gb("multi_gb");
#multi_gb("multi_gb_same", FALSE);
