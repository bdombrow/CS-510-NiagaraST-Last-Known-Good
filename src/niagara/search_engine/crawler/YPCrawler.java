
/**********************************************************************
  $Id: YPCrawler.java,v 1.2 2002/12/10 01:11:00 vpapad Exp $


  NIAGARA -- Net Data Management System                                 
                                                                        
  Copyright (c)    Computer Sciences Department, University of          
                       Wisconsin -- Madison                             
  All Rights Reserved.                                                  
                                                                        
  Permission to use, copy, modify and distribute this software and      
  its documentation is hereby granted, provided that both the           
  copyright notice and this permission notice appear in all copies      
  of the software, derivative works or modified versions, and any       
  portions thereof, and that both notices appear in supporting          
  documentation.                                                        
                                                                        
  THE AUTHORS AND THE COMPUTER SCIENCES DEPARTMENT OF THE UNIVERSITY    
  OF WISCONSIN - MADISON ALLOW FREE USE OF THIS SOFTWARE IN ITS "        
  AS IS" CONDITION, AND THEY DISCLAIM ANY LIABILITY OF ANY KIND         
  FOR ANY DAMAGES WHATSOEVER RESULTING FROM THE USE OF THIS SOFTWARE.   
                                                                        
  This software was developed with support by DARPA through             
   Rome Research Laboratory Contract No. F30602-97-2-0247.  
**********************************************************************/

package niagara.search_engine.crawler;

import java.util.*;
import java.net.*;
import java.io.*;

import com.oroinc.text.regex.*;

public class YPCrawler extends Crawler {
    Perl5Compiler compiler = new Perl5Compiler();
    String link_pattern = "href\\s*=\\s*(?:\")?([^\\s\">]*)[\"\\s>]";

    Perl5Pattern pattern;
    
    static long cntPage=0;

    public YPCrawler() {
	super();
	goCrawlers();
    }
    
    public YPCrawler(String url, int threads, int sdomain) {
	super(url, threads, sdomain);
	goCrawlers();
    }
    
    protected void goCrawlers() {
	if (searchDomain == Crawler.SERVER) {
	    int ind1 = startUrl.indexOf('/',10);
	    if (ind1<0) linkPrefix = startUrl;
	    else linkPrefix = startUrl.substring(0,ind1);
	} else if (searchDomain == Crawler.SUBTREE) {
	    int ind1 = startUrl.lastIndexOf('/');
	    if (ind1<10) linkPrefix = startUrl;
	    else linkPrefix = startUrl.substring(0,ind1);
	}
		
	try {
	    pattern = (Perl5Pattern)compiler.compile(link_pattern,Perl5Compiler.CASE_INSENSITIVE_MASK );
	} catch(Exception e) {
	    e.printStackTrace();
	    System.exit(-1);
	}
	addThreads(initThreads);
    }

    protected void processPage(String url) {
	if (url==null||url.length()<10) return;
	cntPage++;
	Runtime rt = Runtime.getRuntime();
	long tm = rt.totalMemory()/(1024*1024);
	long fm = rt.freeMemory()/(1024*1024);
	int id = ((Integer)htThreadId.get(Thread.currentThread())).intValue();
	
	System.out.println("Parsing ..."+url+"\n"+
			   "\tMemory Used="+tm+"M\tFree Memory="+fm+"M"+
			   "\tthread id="+(id+1)+"/"+numThreads+
			   "\tpages/sec="+cntPage/elapsedTime()+"\n"+
			   "\tXML Found="+StorageManager.cntXml()+
			   "\tPages Visited="+cntPage+
			   "\tURLs in YPQueue="+StorageManager.cntQ()+
			   "\tURLs in HTable="+StorageManager.cntHT());
	InputStream is=null;

	try {
	    URL u = new URL(url);
	    is = u.openStream();
	    Perl5Matcher matcher = new Perl5Matcher();
	    Perl5StreamInput in = new Perl5StreamInput(is);
	    MatchResult result;	    
	    StringBuffer sb = new StringBuffer();
	    //	    System.out.println(in+" "+pattern);
	    while(matcher.contains(in, pattern)) {
		result = matcher.getMatch();  
		if (result.groups()<2) continue;
		URL child = new URL(u,result.group(1));
		//System.out.println(u+"\n"+result.group(1)+"\n"+child);
		
		String ustr = child.toString();
		//		System.out.println(startUrl+"\t"+url+"\t"+ustr);
		int idx1 = ustr.lastIndexOf('/');
		int idx2 = ustr.lastIndexOf('#');
		if (idx1<idx2) ustr = ustr.substring(0,idx2);
		if (linkPrefix == null || ustr.toLowerCase().startsWith(linkPrefix.toLowerCase()))
		    sb.append(ustr).append(" ");
	    }
	    StorageManager.addUrls(sb.toString());
	    is.close();
	} catch (Exception e) {
	    //	    e.printStackTrace();
	    try {
		if (is!=null)
		    is.close();
	    } catch (Exception ex) {}
	}
    }
    
  public static void parseArguments(String args[], Hashtable table) throws Exception {
    for(int i=0;i<args.length;i++) {
      String option = args[i];
            
      if (!option.startsWith("-"))
	throw new Exception("Argument not preceded by option "+option);
            
      if (table.get(option) == null)
	throw new Exception("Option "+option+" not recognized.");
            
      if (i == args.length-1 || args[i+1].startsWith("-"))
	throw new Exception("Option not followed by argument "+option);
            
      for(;i<args.length-1 && !args[i+1].startsWith("-"); i++)
	((Vector)table.get(option)).addElement(args[i+1]);
    }
  }

  public static void main(String args[]) {
    String usage = "YPCrawler -s url [-m domain] [-t thread]\n";
    usage += "\t-s url:       url which webcrawler start at\n";
    usage += "\t-m domain:    search domain, one of following (server|subtree|web)\n";
    //        usage += "\t-u file-name: file which stores list of all url searched\n";
    usage += "\t-t thread:    # of web crawlers running concurrently\n";
	
    if (args.length < 2) {
      System.err.println(usage);
      System.exit(0);
    }
	
    Vector s_vector = new Vector();
    Vector m_vector = new Vector();
    Vector t_vector = new Vector();
    Hashtable table = new Hashtable();
    String s=null;
    int m,t;

    table.put("-s", s_vector);
    table.put("-m", m_vector);
    table.put("-t", t_vector);

    m = Crawler.WEB;
    s = "http://www.cs.wisc.edu";
    t = 5;
	
    try {
      parseArguments(args, table);

      Vector v;
      if ((v=(Vector)table.get("-s")).isEmpty()) {
	System.err.println(usage);
	System.exit(0);
      } else s = (String)v.firstElement();
      if ((v=(Vector)table.get("-m")).isEmpty())
	m = Crawler.WEB;
      else {
	  String dom = (String)v.firstElement();
	  if (dom!=null) {
	      if (dom.equals("server"))
		  m = Crawler.SERVER;
	      else if (dom.equals("subtree"))
		  m = Crawler.SUBTREE;
	  }
      }
      if (!(v=(Vector)table.get("-t")).isEmpty())
	  t = Integer.parseInt((String)v.firstElement());
      
      //YPServer ypServer = new YPServer(0); //take default port
      
      YPCrawler webCrawler = new YPCrawler(s,t,m);
    } catch (Exception e) {
      System.err.println("ERROR: "+e);
      System.err.println(usage);
      System.exit(0);
    }
  }

}
