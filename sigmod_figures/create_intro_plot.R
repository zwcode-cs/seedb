library(reshape2)
library(ggplot2)
month = factor(c("AT&T", "Verizon", "T-Mob", "Sprint"), order=TRUE)
avg_profit_StaplerX = c(180.55, 145.50, 122.00, 90.13)
demo1 = data.frame(month, avg_profit_StaplerX)
ggplot(data=demo1) + theme_bw() + geom_bar(aes(fill=month,x=month, y=avg_profit_StaplerX), stat="identity") + 
	ylab("Avg. load time (ms") + xlab("Carrier") + theme(text=element_text(size=24))
ggsave(file="../full-paper/Images/dist1.pdf",width=7,height=5)
dev.off()


avg_profit_StaplerX = c(26545.80, 29390.76, 34577.11, 43789.53)
demo1 = data.frame(month, avg_profit_StaplerX)
ggplot(data=demo1) + theme_bw() + geom_bar(aes(fill=month,x=month, y=avg_profit_StaplerX), stat="identity") + 
	ylab("Avg. load time (ms") + xlab("Carrier") + theme(text=element_text(size=24))
ggsave(file="../full-paper/Images/dist2.pdf",width=7,height=5)
dev.off()

avg_profit_StaplerX = c(43789.53, 34577.11, 29390.76, 26545.80)
demo1 = data.frame(month, avg_profit_StaplerX)
ggplot(data=demo1) + theme_bw() + geom_bar(aes(fill=month,x=month, y=avg_profit_StaplerX), stat="identity") + 
	ylab("Avg. load time (ms") + xlab("Carrier") + theme(text=element_text(size=24))
ggsave(file="../full-paper/Images/dist3.pdf",width=7,height=5)
dev.off()

