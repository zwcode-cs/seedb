tmp = read.table("/Users/manasi/Documents/workspace/seedb/sigmod_figures/baselines.txt", sep="\t");
colnames(tmp) = c("dataset", "size", "views", "x", "latency", "merged", "all", "dbms");
tmp$views = tmp$views * tmp$x;
tmp$dbms = factor(tmp$dbms);
tmp$views = factor(tmp$views);
tmp$size=factor(tmp$size, levels(tmp$size)[c(1, 3, 2)]);
tmp1 = tmp[tmp$views==250,];
ggplot(tmp1, aes(size, latency/1000)) + 
geom_bar(aes(fill=dbms, ordered=TRUE), position="dodge", stat="identity") + 
ylab("latency (s)") + 
theme(text = element_text(size=24));
ggsave(file="/Users/manasi/Documents/workspace/seedb/full-paper/Images/baselines_by_size.pdf");
tmp2 = tmp[tmp$size=='1M',];
ggplot(tmp2, aes(views, latency/1000)) + 
geom_bar(aes(fill=dbms, ordered=TRUE), position="dodge", stat="identity") + 
ylab("latency (s)") +
theme(text = element_text(size=24));
ggsave(file="/Users/manasi/Documents/workspace/seedb/full-paper/Images/baselines_by_views.pdf");
