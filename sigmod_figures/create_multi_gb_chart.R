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
        ggplot(tmp[tmp$algo!='BP',], aes(n_gb, latency/1000, color=dbms))  + theme_bw() + 
            geom_line(size=1.5) + geom_point(size=2) + theme(text = element_text(size=24)) + 
            ylab("latency (s)") + xlab("Number of attributes") + 
            scale_fill_brewer(palette="Paired") + 
            geom_hline(yintercept=142,  color=cols[2], size=1.5, linetype='dashed')+
            geom_hline(yintercept=38,  color=cols[1], size=1.5, linetype='dashed')+

            #geom_text(data=NULL, aes(10^2,145,label="100000"), color=cols[1]) + 
            #geom_text(data=NULL, aes(10^18,215,label="100000"),color=cols[2]) +
            #geom_text(data=NULL, aes(10^2,55,label="1000"), color=cols[1]) + 
            #geom_text(data=NULL, aes(10^18,165,label="1000"),color=cols[2]) +
            geom_text(data=NULL, aes(15,58,label="COL BP 100"), color=cols[1]) + 
            geom_text(data=NULL, aes(15,162,label="ROW BP 10000"),color=cols[2]);
	       ggsave(file=paste(outdir, filename, ".pdf", sep=""),width=7,height=5);
}

multi_gb("multi_gb");
#multi_gb("multi_gb_same", FALSE);
