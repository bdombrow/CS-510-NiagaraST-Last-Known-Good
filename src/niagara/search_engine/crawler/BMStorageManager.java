
/**********************************************************************
  $Id: BMStorageManager.java,v 1.1 2000/05/30 21:03:27 tufte Exp $


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
 *
 * A version of the Storage Manager that uses bit maps to store Url
 * information rather than using Hash Map.
 *
 * @version 1.0
 *
 */


public class BMStorageManager extends StorageManager {
    
    
    //protected static int hashSize = 32; 
    protected static BitSet htmlVisited;
    protected static BitSet xmlVisited;
    protected static BitSet htmlMask; 
    protected static long xmlCnt;
    protected static clearBM daemon; 

    public static void initialize() {

        System.out.println(" In initialize");
	
	xmlListToVisit  = new URLQueue[Crawler.initThreads];
	for(int i=0;i< Crawler.initThreads;i++)
	    xmlListToVisit[i] = new URLQueue();
	xmlVisited = new BitSet();
	//xmlVisited.clear(Integer.MAX_VALUE);

	htmlListToVisit = new URLQueue[Crawler.initThreads];
	for(int i=0;i < Crawler.initThreads;i++)
	    htmlListToVisit[i] = new URLQueue();
	htmlVisited = new BitSet();
	System.out.println(" Setting htmlVisited ");
        htmlVisited.clear(Integer.MAX_VALUE);

	htmlMask = new BitSet();
        htmlMask.clear(Integer.MAX_VALUE);

	daemon = new clearBM();
	Thread worker = new Thread(daemon);

	worker.start();
    }


    /**
     * Must replace with a better hash algorithm.
     */

    protected static int hashURL(String url) 
    {
        System.out.println(" url : " + url);
	System.out.println(" hashcode " + Math.abs(url.hashCode()));
	return Math.abs(url.hashCode());
    }

 
    public static void addUrls(String urls)
    {
	
      if (urls == null) {
	  System.out.println(" No urls to process! ");
	  return;
      }
      
      StringTokenizer st = new StringTokenizer(urls," ",false);

      try {
	  for (;st.hasMoreTokens();) {
		  String urlstr = st.nextToken();
		  URL url = new URL(urlstr);

		  if (url.getProtocol().equalsIgnoreCase("http")) {
		      String type = guessContentTypeFromName(url.getFile());
		      if (type == null)
			  continue;
		      if (type.equals(mime_html)) {
			  System.out.println("HTML");
			  String nurl = normalizeHttpUrl(urlstr);
			  int off = hashURL(urlstr);
			  try {
			  if (htmlVisited == null)
			    System.out.println(" What the heck ");
			  if (!htmlVisited.get(off))
			      listHtml(nurl,off);
                         }catch (Exception e) {
			   System.out.println(" Exception in addUrls " + e);
                          }
		      } else if (type.equals(mime_xml)) {
			  System.out.println("XML");
			  String nurl = normalizeHttpUrl(urlstr);
			  int off = hashURL(urlstr);
			  if(!xmlVisited.get(off))
			      listXml(nurl,off);
		      }
		  }
	  }
      }catch(Exception e) {
	  System.out.println(" Big Exception " +e);
	  e.printStackTrace();
      }

  }

    
    protected static void clearBitMaps() {
	// Can we somehow capture a notion of time ??
	// Play around with XORs etc.

       /*
       if (htmlMask == null) {
       htmlMask = htmlVisited;
       htmlVisited.xor(htmlMask);
       }
       */
       
       System.out.println(" Clearing bit maps ");
       htmlVisited.xor(htmlMask);
       htmlMask = htmlVisited;
    }


    protected static void listHtml(String url,int offset) {

	System.out.println("In listHtml ");
	int num;
	htmlVisited.set(offset);
	Object id = 
	    (Crawler.htThreadId.get(Thread.currentThread()));
	if ((id == null) || (current < Crawler.initThreads)) {
	    current = (current+1)%Crawler.initThreads;
	    num = current;
	}else {
	    num = ((Integer) id).intValue();
	}

	htmlListToVisit[num].push(url);
	StorageManager.sendHtmlUrlToWrapper(url);

    }
  

    protected static void listXml(String url,int offset) {

	xmlVisited.set(offset);
	int id = ((Integer)
	   Crawler.htThreadId.get(Thread.currentThread())).intValue();
	xmlListToVisit[id].push(url);
	xmlCnt++;
	StorageManager.sendXmlUrlToUpdater(url);

    }

    public static long cntXml() {
	return xmlCnt;
    }

    protected static void flushBitMap() {
	// periodically flush the Bit Map to the disk ??
    }

    protected static void reconstructBitMap() {
	// obvious isnt it ??
    }


}
