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
	p = ggplot(tmp, aes(x=utility, y=1)) + theme_bw() + geom_point(aes(size=20)) + 
		scale_y_discrete(breaks=NULL) + ylab("") + theme(text = element_text(size=24)) + 
		guides(size=FALSE) +  scale_fill_brewer();
	for (n in 1:13) {
		p = p + geom_vline(xintercept=intercepts[n,i], color=cols[intercepts[n,1]]);
	}
	p
}

