intercepts_diabetes = c(0.2809869607475946, 0.27600226725661714, 0.2716321065324011, 0.26374156522182585, 0.25763684725531105, 0.254661303145576, 0.2539014323511392, 0.2512375326239501, 0.25048929968885963, 0.2460851705687164, 0.17185486893728089, 0.1187985632979906, 0.09301708164241629);
intercepts_bank = c(0.12245041562681777, 0.10974741956296757, 0.0977562344159716, 0.09693084653726602, 0.09679026763394241, 0.09597317202954887, 0.09432531002986753, 0.0916609886547255, 0.08707702988846854, 0.06255858566594588, 0.04970413361888907, 0.04476536879152442, 0.030845207947047745);
intercepts_idx = c(1,2,3,4,5,6,7,8,9,10,15,20,25);
intercepts=cbind(intercepts_idx, intercepts_bank, intercepts_diabetes);

gg_color_hue <- function(n) {
  hues = seq(15, 375, length=n+1)
  hcl(h=hues, l=65, c=100)[1:n]
}

plotUtilityDistribution  <- function(filename, i) {
	cols = gg_color_hue(25);
	tmp = read.csv(filename, header=F);
	colnames(tmp) = c("view", "utility");
        .e <- environment();
	p = ggplot(tmp,environment=.e,  aes(x=utility, y=1)) + theme_bw() + geom_point(aes(size=20)) + 
		scale_y_discrete(breaks=NULL) + ylab("") + theme(text = element_text(size=24)) + 
		guides(size=FALSE) +  scale_fill_brewer(palette="Paired")
	for (n in 1:13) {
		p = p + geom_vline(xintercept= intercepts[n,i], color=cols[intercepts[n,1]])
            }
        p = p + geom_text(data=NULL,aes(intercepts[1,i],angle=90,vjust=1.5, hjust=.5, 1,label="highest utility view"),color=cols[intercepts[1,1]])
        p = p + geom_text(data=NULL,aes(intercepts[10,i],angle=90,vjust=-.5, hjust=.5, 1,label="10th highest utility view"),color=cols[intercepts[10,1]])
        p = p + geom_text(data=NULL,aes(intercepts[11,i],angle=90,vjust=-.5, hjust=.5, 1,label="15th highest utility view"),color=cols[intercepts[11,1]])
        p = p + geom_text(data=NULL,aes(intercepts[12,i],angle=90,vjust=-.5, hjust=.5, 1,label="20th highest utility view"),color=cols[intercepts[12,1]])
        p = p + geom_text(data=NULL,aes(intercepts[13,i],angle=90,vjust=-.5, hjust=.5, 1,label="25th highest utility view"),color=cols[intercepts[13,1]])

	p
    }

plotUtilityDistribution("bank_all_views.txt",2)
ggsave(file="../full-paper/Images/bank_utility_distribution.pdf",width=12,height=5)
plotUtilityDistribution("diabetic_data_all_views.txt",3)
ggsave(file="../full-paper/Images/diabetes_utility_distribution.pdf",width=12,height=5)

