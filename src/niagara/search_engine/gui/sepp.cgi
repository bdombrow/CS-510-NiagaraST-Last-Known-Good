#!/s/std/bin/perl 

# @author Jaewoo Kang
#

use CGI qw(param, header); # -no_debug)
use IO::Socket;


$mailprog = '/usr/lib/sendmail';
$recipient = "jaewoo\@cs.wisc.edu czhang\@cs.wisc.edu qiongluo\@cs.wisc.edu";

$qstring = param("query");

%colpos = ();

STDOUT->autoflush(1);

print header();
&print_top;         
&print_head; 
&print_prompt();  
&do_work();  
&print_bottom;  


#############################################

sub print_top {
      print <<END;

<html>
<head>
<title>XML Search Engine</title>
</head>
<body bgcolor="#FFFFFF">
    

END
;
}


sub print_head {
    print <<END;

    <H1> Query Results </H1>
END
;
}


sub print_prompt {
}

sub print_bottom {
    print <<END;
    </body></html>

END
    ;
}


sub do_work {
    my $header;
    my @table;
    my $remote;

    if ($qstring eq undef) {
	print "<br><center><B><font color=red>Please specify your query in the query form.</font></B></center>";
	return;
    }
    
    my @decorated;

    $remote=IO::Socket::INET->new(Proto=>"tcp",
				  PeerAddr=>"provolone.cs.wisc.edu",
				  PeerPort=>"1710",
				  );
    unless ($remote) { 
#	open(MAIL,"|$mailprog $recipient") || die "$mailprog not available.\n";
#	print MAIL "Return-Path: jaewoo\@cs.wisc.edu\n";
#	print MAIL "From: jaewoo\@cs.wisc.edu\n";
#	print MAIL "Subject: SE++ server down!!!\n\n";
#	print MAIL "this is an automated message.\n".`date`."\n";
#	print MAIL "Please reboot the server.\n";
#	close(MAIL);
	
	die "ERROR: can't connect to SE++ Server!!!";
    }
    
    $remote->autoflush(1);
    my $qstring_s = $qstring;
#    $qstring_s =~ s/\s+/ /g;
#    $qstring_s =~ s/^\s//;
    $qs  = "<request>\n<type>0</type>\n<query>$qstring_s</query>\n</request>";
    print $remote "$qs\n";
    
#    $header =    <$remote>;

    
    $_ = <$remote>;
    if ($_ =~ /^Error/) {
	print "<h2> $_ </h2>";
    } else {
	print "<OL>\n";
	
	while (!(($_ = <$remote>) =~ /^<\/result>/)) { 
	    default_row($_);
	}
    
	print "</OL>\n";
    }

    close($remote);
}

sub default_header {
    my $header=pop(@_);
    my @header = split(/-\ca-/, $header);
    my @decorated;
    my (@url_cols, $i);
    $i=0;
    chomp(@header);
    foreach (@header) {
	$colpos{$_} = $i;
	if (/^url$/ && $i>0) {
	    push(@url_cols, $i);
	} 
	$i++;
    }
    return @url_cols;
}

sub default_row {
    my $row=pop(@_);

    $row =~ s/<[^>]*>//g;

    print <<END;
<LI> <p><a href="$row"><b><font face="Geneva, Arial, Helvetica" size="2">$row</font></b></a><br><font size="-1" face="Geneva, Arial, Helvetica"><br></LI>
END
    ;
#    print <<END;
#<p><a href="$row[$colpos{"url"}]"><b><font face="Geneva, Arial, Helvetica" size="2">$row[$colpos{"url"}]</font></b></a><font size="-1" face="Geneva, Arial, Helvetica"><br>
#$row[$colpos{"text"}] 
#<br></font></p>

#END
}




