all_opt_real2 <- function(filename) {
        dirname="";
        outdirname="../full-paper/Images/"
        tmp = read.table(paste(dirname, filename, ".txt", sep=""), sep="\t");
        colnames(tmp) = c("dataset", "opt", "latency");
	tmp$dataset = factor(tmp$dataset);
	tmp$dataset = ordered(tmp$dataset, levels = c("BANK", "DIAB", "AIR", "AIR10"));
	tmp$opt = factor(tmp$opt)
	tmp$opt = ordered(tmp$opt, levels = c("NO_OPT", "SHARING", "COMB", "COMB_EARLY"))
        offscale=subset(tmp,latency>100000)
        scale=subset(tmp,latency<=100000)
    ggplot(tmp, aes(opt, latency)) + 
		geom_bar(aes(fill=opt), position="dodge", stat="identity") + 
                    ylab("latency (ms)") +  theme_bw() + xlab("Datasets") + scale_y_log10(labels = trans_format("log10", math_format(10^.x))) +
                        geom_text(data = scale, aes(opt, label = latency), size=3, vjust=-.1) +
                        geom_text(data = offscale, aes(y=135000, label = latency), size=2) + 
                        facet_grid(. ~ dataset) +
                            theme(axis.text.x = element_blank(), axis.ticks = element_blank()) + theme(legend.position="top") +
                                theme(legend.title = element_text(size=16)) + theme(legend.text = element_text(size=16)) +
                                    theme(text = element_text(size=20))  + scale_fill_brewer(palette="Paired") + 
                                        geom_hline(yintercept=1000, linetype="dashed") + geom_text(aes(2,1000,label = "1s", vjust = -1)) + coord_cartesian(ylim = c(200, 150000)) ;
		ggsave(file=paste(outdirname, filename, ".pdf", sep=""),width=7,height=5);
}

all_opt_real <- function(filename) {
    dirname="";
    outdirname="../full-paper/Images/"
    tmp = read.table(paste(dirname, filename, ".txt", sep=""), sep="\t");
    colnames(tmp) = c("dataset", "opt", "latency");
	tmp$dataset = factor(tmp$dataset);
	tmp$dataset = ordered(tmp$dataset, levels = c("BANK", "DIAB", "AIR", "AIR10"));
	tmp$opt = factor(tmp$opt)
	tmp$opt = ordered(tmp$opt, levels = c("NO_OPT", "SHARING", "COMB", "COMB_EARLY"))
    
    #p1 = helper(tmp, "BANK", outdirname, filename);
    #p2 = helper(tmp, "DIAB", outdirname, filename);
    #p3 = helper(tmp, "AIR", outdirname, filename);
    #p4 = helper(tmp, "AIR10", outdirname, filename);
    #multiplot(p1, p2, p3, p4, cols=4);
    helper(tmp, "BANK", outdirname, filename);
    helper(tmp, "DIAB", outdirname, filename);
    helper(tmp, "AIR", outdirname, filename);
    helper(tmp, "AIR10", outdirname, filename);
}

helper <- function(tmp, dataset, outdirname, filename) {
	tmp = tmp[tmp$dataset==dataset,];
	ylabel = "";
	if (dataset == "BANK") {
		ylabel = "latency (s)";
	}

	p = ggplot(tmp, aes(opt, latency/1000)) + 
		geom_bar(aes(fill=opt), position="dodge", stat="identity") +
		ylab(ylabel) +  theme_bw() + xlab("") + #scale_y_log10() +
		theme(axis.text.x = element_blank(), axis.ticks = element_blank()) +
		theme(text = element_text(size=12))  + scale_fill_brewer(palette="Paired");
	if (dataset != "AIR10") {
		p = p + guides(fill=FALSE);
	}
	ggsave(file=paste(outdirname, filename, "_", dataset, ".pdf", sep=""),width=7,height=7);
	#p;
}

# Multiple plot function
#
# ggplot objects can be passed in ..., or to plotlist (as a list of ggplot objects)
# - cols:   Number of columns in layout
# - layout: A matrix specifying the layout. If present, 'cols' is ignored.
#
# If the layout is something like matrix(c(1,2,3,3), nrow=2, byrow=TRUE),
# then plot 1 will go in the upper left, 2 will go in the upper right, and
# 3 will go all the way across the bottom.
#
multiplot <- function(..., plotlist=NULL, file, cols=1, layout=NULL) {
  require(grid)

  # Make a list from the ... arguments and plotlist
  plots <- c(list(...), plotlist)

  numPlots = length(plots)

  # If layout is NULL, then use 'cols' to determine layout
  if (is.null(layout)) {
    # Make the panel
    # ncol: Number of columns of plots
    # nrow: Number of rows needed, calculated from # of cols
    layout <- matrix(seq(1, cols * ceiling(numPlots/cols)),
                    ncol = cols, nrow = ceiling(numPlots/cols))
  }

 if (numPlots==1) {
    print(plots[[1]])

  } else {
    # Set up the page
    grid.newpage()
    pushViewport(viewport(layout = grid.layout(nrow(layout), ncol(layout))))

    # Make each plot, in the correct location
    for (i in 1:numPlots) {
      # Get the i,j matrix positions of the regions that contain this subplot
      matchidx <- as.data.frame(which(layout == i, arr.ind = TRUE))

      print(plots[[i]], vp = viewport(layout.pos.row = matchidx$row,
                                      layout.pos.col = matchidx$col))
    }
  }
}

library('scales');
all_opt_real2("all_opt_real_data_row");
all_opt_real2("all_opt_real_data_col");
