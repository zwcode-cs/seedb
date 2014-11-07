print "drop table if exists s_1_5; select measure1 into s_1_5 from s_1;"
for i in range(20):
	print "alter table s_1_5 add column dim" + str(i+1) + "_100 varchar(10);"
	print "update s_1_5 set dim" + str(i+1) + "_100=ceil(random()*100);"
print "alter table s_1_5 add column selector integer;"
print "update s_1_5 set selector=ceil(random()*20);"