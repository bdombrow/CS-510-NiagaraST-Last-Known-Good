
/**********************************************************************
  $Id: SECrawler.java,v 1.1 2000/05/30 21:03:27 tufte Exp $


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

 /** 
  * This is a preliminary version of the search engine crawler that has
  * severed relations from the YP family.
  * The crawler only crawls HTML documents finding out links that are
  * XML at present.
  *
  * @version 1.0
  *
  */

 public class SECrawler extends Crawler {

 Perl5Compiler compiler = new Perl5Compiler();
 String link_pattern = "href\\s*=\\s*(?:\")?([^\\s\">]*)[\"\\s>]";
 Perl5Pattern pattern;
 static long cntPage=0;

 public SECrawler() {
 super();
 goCrawlers();

 }

 public SECrawler(int threads,int sdomain) {
     super();
     goCrawlers();
  }

 public SECrawler(String url,int threads,int sdomain) {
 super(url,threads,sdomain);
 goCrawlers();

 }

     
 /**
  * Set appropriate link pattern to direct the crawl.
  *
  */

 public void goCrawlers()
 {
  if(searchDomain == Crawler.SERVER) {
     int ind1 = startUrl.indexOf('/',10);
     if (ind1 < 0) linkPrefix = startUrl;
        else linkPrefix = startUrl.substring(0,ind1);
    }else if (searchDomain == Crawler.SUBTREE) {
       int ind1 = startUrl.lastIndexOf('/');
       if (ind1 < 10) linkPrefix = startUrl;
       else linkPrefix = startUrl.substring(0,ind1);
    }

   // Compile the perl pattern to match HREFs 

    try {
      pattern = (Perl5Pattern)compiler.compile(link_pattern,Perl5Compiler.CASE_INSENSITIVE_MASK);
      } catch(Exception e) {
	e.printStackTrace();
	System.exit(-1);
      }

      addThreads(initThreads);

 }


 /**
  *  Analyze the current Url, find all the links and pass it on the
  *  Storage Manager.
  */
 protected void processPage(String url) {
     System.out.println(" Processing the page " + url);
     
     if (url==null||url.length()<10)
	 {
	     System.out.println(" No urls left for me");
	     return;
	 }
     cntPage++;
     
     
     if ((TTL > 0) && (elapsedTime() >= TTL)) {
	 System.out.println(" My time is up \n");
	 return;
     }


     Runtime rt = Runtime.getRuntime();
     long tm = rt.totalMemory()/(1024*1024);
     long fm = rt.freeMemory()/(1024*1024);
     
     int id = ((Integer) htThreadId.get(Thread.currentThread())).intValue();
  
     System.out.println( " Parsing ..."+url+"\n"+
			 "\t Memory Used ="+tm+"M\tFree Memory="+fm+"M"+ 
			 "\tthread id="+(id+1)+"/"+numThreads+
			 "\tpages/sec="+cntPage/elapsedTime()+"\n"+
			 "\tXML Found="+StorageManager.cntXml()+
		       "\tPages Visited="+cntPage+
			 "\tURLs in URLQueue="+StorageManager.cntQ()+
			 "\tURLS in HTTable="+StorageManager.cntHT());
     
     InputStream in = null;
     
     try {
	 URL u = new URL(url);
	 in = u.openStream();
	 
	 // Locate Links from this page.
	 Perl5Matcher matcher = new Perl5Matcher();
	 Perl5StreamInput pin = new Perl5StreamInput(in);
	 MatchResult result;
	 
	 StringBuffer sb = new StringBuffer();
	 
	 
      while(matcher.contains(pin,pattern)) {
	  
	  result = matcher.getMatch();
	if(result.groups() < 2) continue;
	
	URL child = new URL(u,result.group(1));
	
	String ustr = child.toString();
	int idx1 = ustr.lastIndexOf('/');
	int idx2 = ustr.lastIndexOf('#');
	
	if(idx1 < idx2) ustr = ustr.substring(0,idx2);
	if(linkPrefix == null || 
	   ustr.toLowerCase().startsWith(linkPrefix.toLowerCase()))
	    sb.append(ustr).append(" ");
	
      }
      
      //Add the URLS to the storage Manager..
      StorageManager.addUrls(sb.toString());
      in.close();
     } catch (Exception e) {
	 //System.out.println(" Problems in processing page "+e);
	 //e.printStackTrace();
	 try {
	     if( in!=null)
		 in.close();
	 } catch( Exception ex) {}
	 
     }
     
 }
     

 public static void parseArguments(String args[], Hashtable table)
     throws Exception
     {

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



     
 public static void main(String args[]) throws Exception {
             
     /* Need to provide a command line interface for the User */

    
     /* Help information on options */

     String usage = " The SECrawler can be customized usign the foll. options \n \n";
     usage += "\t -s <url>: Which url to start crawling ? \n";
     usage += "\t -m <domain>: search domain, one of the following (server|subtree|web) \n";
     usage += "\t -t <threads>: # of web crawlers running concurrently \n";
     usage += "\t -ip <serverName>: Name of Search Engine server \n";
     usage += "\t -o <fileName): Redirect all Documents found to a file \n";
     usage += "\t -ttl <seconds> : Stop the crawler after this time \n";
     usage += "\n None of the arguments need to be specified, system defaults exist. \n";
     usage += " Read the README file for more details on individual options \n"; 

     if ( (args.length == 1) && (args[0].equals("help"))) {
	 System.err.println(usage);
	 System.exit(0);
     }

     /* 
	Need to add options for search engine server host
	and filename of init Urls, file name to dump final Urls.
	Need more flexibility.
     */
     
     Vector s_vector = new Vector();
     Vector m_vector = new Vector();
     Vector t_vector = new Vector();
     Vector ip_vector = new Vector();
     Vector o_vector = new Vector();
     Vector ttl_vector = new Vector();

     Hashtable table = new Hashtable();
     String s = null;
     String server = null;
     String fname = null;
     int m,t,ttl;

     table.put("-s",s_vector);
     table.put("-m",m_vector);
     table.put("-t",t_vector);
     table.put("-ip",ip_vector);
     table.put("-o",o_vector);
     table.put("-ttl",ttl_vector);

     m = Crawler.WEB;
     //s = "http://www.cs.wisc.edu";
     s = null;
     t = 5;
     ttl = -1;

     try {

	 parseArguments(args,table);

	 Vector v;

	 if(!(v = (Vector)table.get("-s")).isEmpty()) 
	     s = (String)v.firstElement();
	 
	 if(!(v = (Vector) table.get("-o")).isEmpty())
	     fname = (String) v.firstElement();

	 if (!( v = (Vector) table.get("-ip")).isEmpty()) 
	     server = (String) v.firstElement();

	 if(!(v = (Vector) table.get("-ttl")).isEmpty())
	     ttl = Integer.parseInt((String) v.firstElement());

	 if((v=(Vector)table.get("-m")).isEmpty())
	     m = Crawler.WEB;
	 else {

	     String dom = (String) v.firstElement();
	     if (dom != null) {
		 if( dom.equals("server"))
		     m = Crawler.SERVER;
		 else if(dom.equals("subtree"))
		     m = Crawler.SUBTREE;
	     }
	 }

	 if(!(v = (Vector) table.get("-t")).isEmpty())
	     t = Integer.parseInt((String)v.firstElement());
 
	 /* Configure the Crawler according to input specifications */

	 if( server != null) 
	     StorageManager.setSEServer(server);
	 if ( fname != null)
	     StorageManager.setUrlFile(fname);
	 if( ttl != -1)
	     Crawler.setTTL(ttl);

	 SECrawler myCrawler; 

	 if (s != null)
	  myCrawler = new SECrawler(s,t,m);
	 else
	  myCrawler = new SECrawler(t,m);   

     } catch (Exception e) {
	 System.err.println(" ERROR :" +e);
	 System.err.println(usage);
	 System.exit(0);
     }
     
 }
 }


