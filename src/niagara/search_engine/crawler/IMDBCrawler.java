
/**********************************************************************
  $Id: IMDBCrawler.java,v 1.1 2000/05/30 21:03:27 tufte Exp $


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
import edu.upenn.w4f.util.*;

public class IMDBCrawler extends SECrawler {

 // Add to Storage Manager only those URLs
 // that conform to the wrappers.

 public IMDBCrawler(){
  super();
 }

 public IMDBCrawler(String url,int threads,int sdomain) {
	super(url,threads,sdomain);
 }
     
 protected void processPage(String url)
 {

     if( url == null || url.length() < 10)
	 {
	     System.out.println(" No urls left to process \n");
	     return;
	 }

     cntPage++;
     
     if( (TTL > 0) && (elapsedTime() >= TTL)) {
	 System.out.println(" My time is up \n");
	 return;
     }

     Runtime rt = Runtime.getRuntime();
     RetrievalAgent ra = new RetrievalAgent();

     int id = ((Integer) htThreadId.get(Thread.currentThread())).intValue();

     System.out.println(" Parsing ... " + url + "\n");
     System.out.println("\t Pages Visited="+cntPage+
			"\t URLs in URLQueue="+StorageManager.cntQ()+
			"\t URLS in HTTAble ="+StorageManager.cntHT());

     //InputStream in = null;
     DataInputStream in= null;

     try {
	 URL u = new URL(url);
	 //in = u.openStream();
         in = new DataInputStream(ra.GET(url));

	 Perl5Matcher matcher = new Perl5Matcher();
	 Perl5StreamInput pin = new Perl5StreamInput(in);
	 MatchResult result;

	 StringBuffer sb = new StringBuffer();

	 while(matcher.contains(pin,pattern)) {

	     result = matcher.getMatch();
	     if(result.groups() < 2) continue;
	     
	     URL child = new URL(u,result.group(1));
	     
	     String ustr = child.toString();
	   
	     System.out.println(" ustr is " + ustr);

	     int tidx = ustr.indexOf("Name?");
	     int tidx2 = ustr.indexOf("Title?");
	     int idx1 = ustr.lastIndexOf('/');
	     int idx2 = ustr.lastIndexOf('#');

	     if(idx1 < idx2) ustr = ustr.substring(0,idx2);

	     if((tidx > 0)|| (tidx2 > 0) ||(linkPrefix == null) || (ustr.toLowerCase().startsWith(linkPrefix.toLowerCase())))
		 sb.append(ustr).append(" ");
	     //StorageManager.addUrls(ustr);
	 }

	 StorageManager.addUrls(sb.toString());
	 in.close();
     } catch (Exception e) {
	System.out.println("Problem in processPage " + e);  
	 try {
	     if( in != null)
		 in.close();
	 }catch( Exception ex) {}

     }

 }


 public static void main(String args[]) throws Exception {

     System.out.println(" In main() \n");
     StorageManager.setUrlFile("IMDB-Actors");
     StorageManager.initFileStream();
     Crawler.setTTL(2000);
     //IMDBCrawler spider = new IMDBCrawler("http://us.imdb.com/top_250_films",
					 // 5,SERVER);
     IMDBCrawler spider = new IMDBCrawler();
 }

}
