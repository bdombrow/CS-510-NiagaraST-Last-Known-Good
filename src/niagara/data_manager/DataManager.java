
/**********************************************************************
  $Id: DataManager.java,v 1.11 2003/09/22 01:52:11 vpapad Exp $


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


package niagara.data_manager;
import niagara.ndom.*;
import org.w3c.dom.*;
import org.xml.sax.*;
import java.util.*;
import java.io.*;
import java.net.*;

import niagara.utils.*;
import niagara.xmlql_parser.syntax_tree.*;

/**
 *  Initial implementation of data manager no memory management
 *  essentially managing two directories
 *  @see YPClient
 *  @version 1
 */
public class DataManager {

    private static String dtd_persist = ".dm_dtdcache";
    private static String xml_persist = ".dm_xmlcache";
    public CacheManager cacheM;
    private String tmpDir;

    /* Table storing accumulate files */
    public static HashMap AccumFileDir = new HashMap();
    
    public DataManager(String path, 
		       int diskSpace, 
		       int policy, 
		       int numFetchThreads,
		       int numUrlThreads)
    {
        // DTD and YP stuff.
        tmpDir = path;
        File dtdPersistFile = new File(tmpDir+dtd_persist);
	if (dtdPersistFile.exists()) {
	    System.out.println("initialize from previous incarnation"+
                               "of data manager...");
	    System.out.println("the previous incarnation of "+
                               "dtd directory is persisted at: \n"
                               +tmpDir+dtd_persist);
        }

        // Feng's Code all in CacheManager.
        cacheM = new CacheManager(tmpDir);
    }
    
    /**
     *  Generate a SE request from a vector of predicates.
     *  Send this request to the SE using YPClient.  The 
     *  response will be an xml doc.  Parse it and return it to the 
     *  caller This call will block while the search engine request 
     *  is being serviced.
     *
     *  @param requestList a vector of requests
     *  @return a TXDocument for the parsed root of xml file containing DTDinfo
     *  @exception DMClosedException this function called while 
     *                             data manager is being closed 
     */

    public synchronized DTDInfo getDTDInfo (Vector requestList) {
	assert false : "Search Engine Not Supported";
	return null;
    }

    public synchronized Vector getDTDList() {
        assert false : "Search Engine Not Supported";
	return null;
    }

    /**
     *  Gracefully shutdown the Data Manager by terminating all threads etc. 
     */
    public synchronized boolean shutdown()
    {
        File dtdPersistFile = new File(tmpDir+dtd_persist);
	if (dtdPersistFile.exists()) {
            dtdPersistFile.delete();
        }
        
        // Next need to shutdown XML Cache. 
        cacheM.shutdown();
        return true;        
    }

    // Backward compatibility issue
    public void enableCache() {
        return;
    }
    public void disableCache() {
        return;
    }

    public boolean getDocuments(Vector xmlURLList, 
				regExp pathExpr,   
				SinkTupleStream stream) 
    throws ShutdownException {
        return cacheM.getDocuments(xmlURLList, pathExpr, stream);
    }

    public static Document parseXML(String str) {
        niagara.ndom.DOMParser p = DOMFactory.newParser(); 
        Document doc = DOMFactory.newDocument();

        try {
	    p.parse(new InputSource(new StringReader(str)));
	}   catch (SAXException e) {
	    System.err.println("Error parsing " + e.getMessage() +
			       " XML: " + str);
	    e.printStackTrace();
            return null;
	} catch(IOException ioe) {
	    System.err.println("Error parsing " + ioe.getMessage() +
			       " XML: " + str);
	    ioe.printStackTrace();
            return null;	    
	}
	return doc;
    }
    
}



