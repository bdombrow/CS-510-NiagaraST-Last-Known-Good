
/**********************************************************************
  $Id: StorageManager.java,v 1.1 2000/05/30 21:03:27 tufte Exp $


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
import java.text.*;

import niagara.search_engine.indexmgr.*;
import niagara.search_engine.server.*;

/**
 * StorageManager is a static class which maintaining a queue for urls
 * to be searched and a set of urls having been visited. 
 *
 * Since the set for visited urls stores both urls for xml and html, 
 * it is likely to grow extremely big. Hence it needs large memory.
 * A disk storage manager hash index can be used to store the set as 
 * an alternate solution.
 *
 * Each visited url entry time stamped with the last modified time.  
 * The time stamp will be used to decide whether revisit a url or not when 
 * a url is encountered and the url has been visited before.
 *
 * @version 1.0
 */


public class StorageManager {
    
    /* set to a week's time */
    public static long EXPIRESIN = 604800;

    public static String HTMLFILEVISITED = "_HTMLFILEVISITED";
    public static String HTMLFILETOVISIT = "_HTMLFILETOVISIT";
    public static String XMLFILEVISITED = "_XMLFILEVISITED";
    public static String XMLFILETOVISIT = "_XMLFILETOVISIT";

    /* document formats */
    public static String mime_xml = "text/xml";
    public static String mime_html = "text/html";

    /* Multiple queues for storing the list of Urls to be visited */
    protected static URLQueue xmlListToVisit[];  
    protected static URLQueue htmlListToVisit[];

    /* HashTables for storing the timestamp of visited urls */
    protected static HashMap xmlHtVisited;
    protected static HashMap htmlHtVisited;

    
    // Name of the SEServer that the Storage Manager should contact.
    public static String SEServer= null;

    // Persistent connection to the Search Engine server.
    protected static SEClient myClient; 

    public static String UrlFile = null;
    public static PrintStream pout = null;
    protected static int current = 0;

    static {
      try {
	initialize();
      }catch(Exception e) {
        System.out.println("Problems in initializing SM " + e);
	System.exit(1);
       }
    }

    /**
     * initialize internal hashtable and list.
     * NOTE: it should be rewritten when the memory data structure 
     * replaced with RDB hash index.
     */
    public static void initialize() {

	System.out.println(" initThreads is " + Crawler.initThreads);
	xmlListToVisit = new URLQueue[Crawler.initThreads];
	for(int i=0;i < Crawler.initThreads;i++)
	xmlListToVisit[i] = new URLQueue();
	xmlHtVisited = new HashMap(1000011);

	htmlListToVisit = new URLQueue[Crawler.initThreads];
	for(int i=0;i< Crawler.initThreads;i++)
	htmlListToVisit[i] = new URLQueue();
	htmlHtVisited = new HashMap(1000011);

    }	    


    /**
     * Set the Search Engine Server name so that the XML documents
     * found can be sent to the SE to index.
     */

    public static void setSEServer(String serverName) throws IOException {
     try{
     SEServer = serverName;
     myClient = new SEClient(serverName);
     }catch(Exception e) {
      System.out.println(" Problems in setImServer "+e);
     }	
    }


    /**
     *  The XML documents found may be redirected to a file and indexed
     *  as a batch later on and not immediately as and when they are
     *  found, for this case use this method to set the OutputFile name.
     */

    public static void setUrlFile(String fname) {
	UrlFile = fname;
	initFileStream();
    }

    /*
     * Create a persistent file handle to write out Urls.
     *
     */

    public static void initFileStream() {

     	try {
        FileOutputStream fout = new FileOutputStream(UrlFile,true);
	pout = new PrintStream(fout);
        }catch(Exception e) {
          System.out.println("problems in opening file \n"+e);
	}

    }


    /**
     * Add a set of urls from the current page crawled to the Urls to Visit 
     * Queue.
     */

    public static void addUrls(String urls) {
        
	//System.out.println(" Storage Manager: add urls ");
	
	if (urls == null){
	    System.out.println(" No urls to process! ");
	return;
	}
	
 	StringTokenizer st = new StringTokenizer(urls," ",false);
	
	try {
	    for (;st.hasMoreTokens();) {
		try {
		    String urlstr = st.nextToken();
		    //System.out.println("gethere"+urlstr);
		    URL url = new URL(urlstr);
		    
		    if (url.getProtocol().equalsIgnoreCase("http")) {
			String type = guessContentTypeFromName(url.getFile());
			if (type == null) 
			    continue;
			if (type.equals(mime_html)) {
			    //System.out.println(" HTML ");
			    String nurl = normalizeHttpUrl(urlstr);
			    long tstamp = lookupHtmlVisited(nurl);
			    long cts = getLastModified(nurl);
			    if (expired(tstamp, cts)) {
				if (cts <= 0) 
				    cts = System.currentTimeMillis();
				listHtml(nurl, cts);
			    }
			} else if (type.equals(mime_xml)) {
			  System.out.println(" XML ");
			    String nurl = normalizeHttpUrl(urlstr);
			    long tstamp = lookupXmlVisited(nurl);
			    long cts = getLastModified(nurl);
			    if (expired(tstamp, cts)) {
				if (cts <= 0) 
				    cts = System.currentTimeMillis();
				listXml(nurl, cts);
			    }
			}
		    }
		} catch (Exception e) { 
		    //e.printStackTrace(); 
		}
	    }
	} catch (Exception ex) {
	    //	    ex.printStackTrace();
	}
    }


    /**
     * Check if the page needs to be crawled again, ie whether the
     * timestamp has expired.
     */

    protected static boolean expired(long tstamp, long cts) {
	// System.out.println(">>>>>"+tstamp+"   "+cts);
	try {
	    if (tstamp < 0) return true;
	    if (cts == 0) {
		long curr = System.currentTimeMillis();
		if (curr>tstamp+EXPIRESIN)
		    return true;
		else return false;
	    }
	    if (cts > tstamp) return true;
	    return false;
	} catch (Exception e) {
	    //	    e.printStackTrace();
	    return true;
	}
    }

    /**
     * Returns the last modified time. now currently just returns
     * 0, which means that the validity of each page will expire only
     * after a week's time.
     */

    protected static long getLastModified(String url) {
	/*
	  URLConnection con=null;
	try {
	    URL u = new URL(url);
	    con = u.openConnection();
	    //	    con.connect();
	    long cts = con.getLastModified();
	    //	    	    System.out.println(">>>>>"+cts);
	    ((HttpURLConnection)con).disconnect();
	    return cts;
	} catch (Exception e) {
	    e.printStackTrace();
	    if (con!=null) 
		((HttpURLConnection)con).disconnect();
	    return (long)0;
	}
	*/
	return 0;
    }

    /**
     * NOTE: it should be rewritten when the memory data structure 
     * replaced with RDB hash index.
     */
    protected static long lookupHtmlVisited(String url) {
	Long ts = (Long)htmlHtVisited.get(url);
	if (ts == null) return (long)-1;
	return ts.longValue();
    }

    /**
     * NOTE: it should be rewritten when the memory data structure 
     * replaced with RDB hash index.
     */

    protected static void listHtml(String url, long cts) {
	//synchronized(htmlHtVisited) {
	    int num;
	    htmlHtVisited.put(url, new Long(cts));
	    //System.out.println("listHtml " + url);

	    //  	}
	    //synchronized(htmlListToVisit) {
	    Object id = (
	       Crawler.htThreadId.get(Thread.currentThread()));
	    //System.out.println(" Thread id is " + id);
	    if ((id == null) || (current < Crawler.initThreads)) {
		current =  (current+1)%Crawler.initThreads;
		num = current;
	    }else {
	       num = ((Integer) id).intValue();
	    }

	    htmlListToVisit[num].push(url);
		// }

	// If we have any wrapper functionality ?
	sendHtmlUrlToWrapper(url);	

    }

    public static long cntXml() {
	return xmlHtVisited.size();
    }

    public static long cntHT() {
	return xmlHtVisited.size()+htmlHtVisited.size();
    }

    public static long cntQ() {
	//return xmlListToVisit.size()+htmlListToVisit.size();
	int count = 0;

	for(int i=0;i< Crawler.initThreads;i++) {
	    count += xmlListToVisit[i].size();
	    count += htmlListToVisit[i].size();
	}

	return count;
    }

    /**
     * NOTE: it should be rewritten when the memory data structure 
     * replaced with RDB hash index.
     */
    protected static long lookupXmlVisited(String url) {
	Long ts = (Long)xmlHtVisited.get(url);
	if (ts == null) return (long)-1;
	return ts.longValue();
    }

    /**
     * NOTE: it should be rewritten when the memory data structure 
     * replaced with RDB hash index.
     */
    protected static void listXml(String url, long cts) {
	//synchronized(xmlHtVisited) {
        xmlHtVisited.put(url, new Long(cts));
	//  	}

	int id = ((Integer) 
		  Crawler.htThreadId.get(Thread.currentThread())).intValue();
	xmlListToVisit[id].push(url);
	sendXmlUrlToUpdater(url);
    }
    

    /**
     * Guess the Document type (XML/HTML) from the url.
     *
     */

    public static String guessContentTypeFromName(String fname) {
	if (fname == null) return null;
	String s = fname.toLowerCase();
	//	System.out.println(fname);
	int ind = s.lastIndexOf('/');
	if (ind < 0) return mime_html;
	s = s.substring(ind);
	ind = s.lastIndexOf('.');
	if (ind < 0) return mime_html;
	if (s.endsWith(".htm") || s.endsWith(".html"))
	    return mime_html;
	else if (s.endsWith(".xml")) 
	    return mime_xml;
	return null;
	//return mime_html;
    }


    /**
     * Make all urls conform to a standard form before hashing.
     */

    public static String normalizeHttpUrl(String s) {
	if (s == null || s.length() < 8) return null;
	try {
	    URL url = new URL(s);
	    URL newUrl = new URL(url.getProtocol().toLowerCase(), 
				 url.getHost().toLowerCase(), 
				 url.getPort(), url.getFile());
	    String urlStr = newUrl.toString();
	    //System.out.println(" Normalized Url is " + newUrl);
	    /*
	    if (urlStr.charAt(urlStr.length()-1) == '/') {
		urlStr = urlStr.substring(0, urlStr.length()-1);
	    }
	    */
	    return urlStr;
	} catch (Exception e) { 
	    //e.printStackTrace(); 
	    return null; 
	}
    }
		    
    /**
     * NOTE: it should be rewritten when the memory data structure 
     * replaced with RDB hash index.
     */
    protected static String nextHtmlUrl() {
	String url = null;
	int id = ((Integer) 
		  Crawler.htThreadId.get(Thread.currentThread())).intValue();
	//System.out.println(" In nextHtmlurl for thread " + id);
	synchronized(htmlListToVisit[id]) {
	    if (!htmlListToVisit[id].isEmpty()) {
		url = (String)htmlListToVisit[id].pop();
	    }
	}
	return url;
    }

    /**
     * NOTE: it should be rewritten when the memory data structure 
     * replaced with RDB hash index.
     */
    protected static String nextXmlUrl() {
	String url = null;
	int id = ((Integer) 
		  Crawler.htThreadId.get(Thread.currentThread())).intValue();
	synchronized(xmlListToVisit[id]) {
	    if (!xmlListToVisit[id].isEmpty()) {
		url = (String)xmlListToVisit[id].pop();
	    }
	}
	return url;
    }

    /**
     *  At present, we assume that XML documents are only those that 
     *  emanate from html links, but in the future they may originate
     *  from other XML documents thro XSL etc, in which case this routine
     *  should be modified to use nextXmlUrl() appropriately.
     */

    public static String nextUrl() {
	return nextHtmlUrl();
    }


    /**
     * If we are using the crawler to wrap some source specific 
     * HTML files, then override this function to call 
     * the specific wrapper etc.
     */


    public static void sendHtmlUrlToWrapper(String url) {
	return;
     }


    /**
     * Send the XML document found to the search engine server or
     * the ouput file as requested.
     *
     */

    public static void sendXmlUrlToUpdater(String url) {

	System.out.println("FOUND: "+url);
	System.out.println(" Sending the url " + url + " to index mgr"); 
	try {
	    // Try to send in batches.
	   
	    if (SEServer != null) {
		String response = myClient.index(url);	
		if (response.startsWith("Error")) {
		    System.out.println(response);
		}
	    } else {
		// just stuff to some file.
		if (pout != null)
		pout.println(url);
	    }
	} catch( Exception e) {
	  System.out.println("Problems in indexing " +e);
        }
    }


}
