in_memory <- function(filename) {
	dirname="";
	outdirname="../full-paper/Images/"
	tmp = read.table(paste(dirname, filename, ".txt", sep=""), sep="\t");
	colnames(tmp) = c("dataset", "k", "latency", "accuracy", "accuracy_se", 
		"utility_dist", "utility_dist_se", "algo");
	limits_accuracy<-aes(ymax=accuracy + accuracy_se, ymin=accuracy - accuracy_se);
	limits_utility_dist<-aes(ymax=utility_dist+utility_dist_se, ymin=utility_dist- utility_dist_se);
	tmp$algo = factor(tmp$algo);

	ggplot(tmp[tmp$algo!='RANDOM',], aes(k, latency/1000, color=algo)) +  theme_bw() + geom_line(size=1.5) + geom_point() + 
		ylab("latency(s)") + theme(text = element_text(size=24))  + scale_fill_brewer(palette="Paired");
	ggsave(file=paste(outdirname, filename, "_latency.pdf", sep=""),width=7,height=5);

	#ggplot(tmp, aes(k, utility_dist, color=algo)) + geom_line() + geom_point() + 
	#	ylab("utility distance") + theme(text = element_text(size=24)) + 
	#	geom_errorbar(limits_utility_dist, width=0.25); #ylim(0, 1) + 

	ggplot(tmp, aes(k, utility_dist, color=algo, ymax= utility_dist + 
		utility_dist_se, ymin= utility_dist - utility_dist_se))+
		geom_line(size=1.5) + geom_point() +  theme_bw() + 
		ylab("utility distance") + theme(text = element_text(size=24)) +
		scale_fill_brewer(palette="Paired");
	ggsave(file=paste(outdirname, filename, "_utility_dist.pdf", sep=""),width=7,height=5);

	#ggplot(tmp, aes(k, accuracy, color=algo, max=accuracy + accuracy_se, ymin=accuracy - accuracy_se)) + ylim(0, 1) + geom_line() + geom_point() + 
	#	ylab("accuracy") + theme(text = element_text(size=24)) +
	#	geom_errorbar(width=0.25);

	ggplot(tmp[tmp$algo!='RANDOM',], aes(k, accuracy, color=algo, ymax=accuracy + accuracy_se, ymin=accuracy - accuracy_se)) + 
		ylim(0, 1.2) + geom_line(size=1.5) +  theme_bw() + geom_point() + 
 		ylab("accuracy") + theme(text = element_text(size=24)) +
 		scale_fill_brewer(palette="Paired");
	ggsave(file=paste(outdirname, filename, "_accuracy.pdf", sep=""),width=7,height=5);
}

in_memory("in_memory_bank");
in_memory("in_memory_dia");
