
/**********************************************************************
  $Id: Crawler.java,v 1.1 2000/05/30 21:03:27 tufte Exp $


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
 * This is the base class of the crawler that any specific
 * crawler implementation  must extend.
 *
 * @version 1.0
 */


public abstract class Crawler implements Runnable {
    public static long startTime = System.currentTimeMillis();

    /**
     * Constants for domain of crawling.
     * WEB  crawls all the links to the document
     * SERVER crawls all documents under a single server domain.
     * SUBTREE crawls all documents that are children of the startUrl.
     */
    public static final int SERVER=0;
    public static final int SUBTREE=1;
    public static final int WEB=2;

    // Max number of threads that can be supported.
    public static int MAXRUNNER = 500;
 
    // Time to Live/crawl in seconds
    // default -1 means that the process goes on endlessly

    public static long TTL=-1;

    private Thread mainThread;

    public static HashMap htThreadId = new HashMap();
    public static int initThreads = 5;

    protected Thread[] runner = new Thread[MAXRUNNER];
    protected boolean[] idle = new boolean[MAXRUNNER];
    protected boolean[] finished = new boolean[MAXRUNNER];
    
    protected String linkPrefix = null;

    int numThreads = 0;
    String startUrl = "http://www.cs.wisc.edu/";
    int searchDomain = WEB;

    public Crawler() 
    {
	super();
	init(initThreads,searchDomain);
    }

    public Crawler( int threads, int sdomain)
    {
	super();
	init(threads,sdomain);
    }
    
    public Crawler(String url, int threads, int sdomain) 
    {
	super();
	init(url, threads, sdomain);
    }


    /**
     * Initialize the Storage Manager with a list of startUrls.
     * The Urls must be in a file "UrlList".
     */

    protected void initSM() 
    {

    // Add the initial urls from the file to the SM.
    try {	
	FileInputStream fin  = new FileInputStream("UrlList");
	BufferedReader bin = new BufferedReader(new InputStreamReader(fin));
	String url;
	System.out.println(" Initial urls are");
	// Add support for comments in the UrlList.
	while((url=bin.readLine()) != null) {
	    System.out.println(url);
	    StorageManager.addUrls(url);
	}
    }catch(Exception e) {
	System.out.println("Problems in initSM");
	// Just add the start Url
	System.out.println(" Adding start Url");
	StorageManager.addUrls(startUrl);

    }

    }

    /**
     * Initialize the crawler when the startUrls are in the file UrlList.
     *
     */
    protected void init(int threads, int sdomain) 
    {
	initThreads = threads;
	searchDomain = sdomain;
        //System.out.println(" In Crawler:Init()"); 
	initSM();
	mainThread = Thread.currentThread();
	URLConnection.setDefaultAllowUserInteraction(false);
    }

    /**
     * Initialize the crawler with a specific start Url.
     *
     */

    protected void init(String url,int threads,int sdomain) 
    {
	startUrl = url;
	initThreads = threads;
	searchDomain = sdomain;
	System.out.println( " In Crawler:Init ");
	System.out.println("start url is " + url);
	StorageManager.addUrls(startUrl);
	mainThread = Thread.currentThread();
	URLConnection.setDefaultAllowUserInteraction(false);

    }


    /**
     * Add num threads to the list of running threads.
     *
     */
    public void addThreads(int num) 
    {

        System.out.println(" In addThreads \n");
	if (numThreads+num> MAXRUNNER || numThreads+num<=0) return;
 	
	if (num < 0) {
	    for(int i=numThreads+num; i<numThreads; i++) {
		finished[i] = true;
		htThreadId.remove(runner[i]);
	    }
	    numThreads -= num;
	} else {
	    int nt = numThreads;
	    numThreads += num;
	    for(int i=nt; i<nt+num; i++) {
		runner[i] = new Thread(this);
		System.out.println("Started a new thread \n");
		idle[i] = false;	    
		finished[i] = false;
		htThreadId.put(runner[i], new Integer(i));
		runner[i].setPriority(Thread.MIN_PRIORITY);
		runner[i].start();
	    }
	}
    }

    /**
     * Return the time elapsed since the start of the crawl.
     */
    
    public static long elapsedTime() 
    {
	long et = (System.currentTimeMillis()-startTime)/1000;
	if (et<1) return 1;
	else return et;
    }
	

    /**
     * Set the TTL to limit the duration of the calling (in seconds).
     * Can be used for more focussed crawling.
     */
    public static void setTTL(long duration) 
    {
       TTL = duration;
    }

    
    /**
     * Code that each Crawler thread executes.
     *
     */
    public void run() 
    {
	String urlStr;
	
	System.out.println(" In run() ");
	int id = ((Integer)htThreadId.get(Thread.currentThread())).intValue();
	
	while(true) {
	    if (finished[id]) return;
	    urlStr = StorageManager.nextUrl();
	    if (urlStr == null) {
		try {
		    idle[id] = true;
		    /*
		       boolean done=true;
		       synchronized(idle) {
		       for (int i=0;i<numThreads;i++) {
		       if (!idle[i]) {
		       done = false;
		       break;
			    }
			    }
			if (done) {
			System.out.println("Web Crawler Finished ..."+"\n"+
			"\tRunning Time="+elapsedTime()+"sec");
			System.exit(0);
			}
			}	
		    */
	            
		    //System.out.println(" No URLS,for Thread:"+id+
		    //" sleeping for some time \n");
		    Thread.sleep(3000);
		    idle[id] = false;
		    continue;
		} catch(Exception e) {
		    idle[id] = false;
		    continue;
		}
	    }
	    
	    processPage(urlStr);
	
            if ((TTL > 0) && (elapsedTime() >= TTL)) {
	    System.out.println(" The Crawler TTL has expired! \n");
	    System.out.println(" The Crawler is exiting \n");
	    // Have to wait till system is stable, current Urls
	    // in the queue are flushed.
	    System.exit(0);
	}

    }

    }
    
    /**
     * Crawler specific implementation to handle each url encountered
     */
    abstract protected void processPage(String url);
}
