#!/usr/local/bin/perl

$temp = "tempresults";
if(-e $temp) {
   system("/bin/rm $temp");
}

print "Running tests...\n";

#system("./runSimple < queries/accumulate.qry > $temp");

print "Average query\n";
system("./runSimple < queries/average.qry > $temp");

print "Construct query\n";
system("./runSimple < queries/construct.qry >> $temp");

print "Construct Middle query\n";
system("./runSimple < queries/consmiddle.qry >> $temp");

print "Count query\n";
system("./runSimple < queries/count.qry >> $temp");

print "Dup-union query\n";
system("./runSimple < queries/dup-union.qry >> $temp");

print "Expression query\n";
system("./runSimple < queries/expression.qry >> $temp");

print "Hash Join query\n";
system("./runSimple < queries/hashjoin.qry >> $temp");

print "Nested Loops query\n"; 
system("./runSimple < queries/nljoin.qry >> $temp");

print "Scan query\n";
system("./runSimple < queries/scan.qry" >> $temp);

print "Select query\n";
system("./runSimple < queries/select.qry >> $temp");

print "Sort query\n";
system("./runSimple < queries/sort.qry >> $temp");

print "Sum query\n";
system("./runSimple < queries/sum.qry >> $temp");

print "Test have been run - differences below.\n";

system("diff $temp results");

print "End of differences.\n"