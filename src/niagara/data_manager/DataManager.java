
/**********************************************************************
  $Id: DataManager.java,v 1.7 2002/05/07 03:10:49 tufte Exp $


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

import niagara.search_engine.server.*;

import niagara.trigger_engine.*;
import niagara.utils.*;
import niagara.query_engine.*;
import niagara.data_manager.XMLDiff.*;
import niagara.xmlql_parser.op_tree.*;
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
    private DTDDirectory dtdDir;

    private String SEhost;
    private int SEport;
    private boolean usePermanent;
    private SEClient client=null;

    private String tmpDir;

    /* Table storing accumulate files */
    public static HashMap AccumFileDir = new HashMap();
    
    public DataManager(String path, 
		       int diskSpace, 
		       int policy, 
		       String server, 
		       int port, 
		       int numFetchThreads,
		       int numUrlThreads,
                       boolean usePermanentConnection) 
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
            
            dtdDir = (DTDDirectory)CUtil.persistent2memory(tmpDir+dtd_persist);
	    /* uugh - this is not nice error handling, but it is an
	    * improvement
	    */
	    if(dtdDir == null) {
               System.err.println("WARNING: Error importing dtdDir");
	    }
        }
        else {
            dtdDir = new DTDDirectory(path);
        }
        SEhost = server;
        SEport = port;

        this.usePermanent = usePermanentConnection;
        if(usePermanentConnection) {
            System.err.println("Using Permanent Connection to Search Engine...");
	    
	    try {
		client = new SEClient(SEhost);
	    } catch (IOException ioe) { // KT-ERROR
		System.err.println("Error: Can't connect to Search Engine..."+ ioe.getMessage());
	    }
	    
        } else {
            System.err.println("Using On-demand Connection to Search Engine...");
 	    client = null;
       }

        // Feng's Code all in CacheManager.
        cacheM = new CacheManager(tmpDir);
    }
    
    /**
     *  Get a DTD from the DTDdir and return the root of parsed DTD
     *  since DTD is assumed to be cached, getDTD is simply get the file
     *
     *  @param dtdURL the URL of the dtd
     *  @return a DTD object which is the root of parsed DTD
     *  @exception DMClosedException this function called while 
     *                             data manager is being closed 
     */
    public synchronized DocumentType getDTD(String dtdURL) 
    throws ParseException, IOException, MalformedURLException {
	DocumentType dtd = null;
        dtd = dtdDir.getDTD(dtdURL);
	return dtd;
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
	String query = (String)requestList.elementAt(0);
	Document responseDoc;
	try {
	    if (!usePermanent) {
		client =  new SEClient(SEhost);
	    }
	    String response = client.query(query);
	    System.out.println("SE response:\n"+response);
	    if (!usePermanent) {
		client.closeConnection();
	    }

	    responseDoc = parseXML(response);
	}
	catch (IOException ioe) { 
	    System.out.println("DM Error: Unable to get DTD Info: "+ioe.getMessage());
	    responseDoc = null; 
	}

	if(responseDoc != null)
	    return DMUtil.parseDTDInfo(responseDoc);
	return null;
    }

    public synchronized Vector getDTDList() {
	Document responseDoc = null;

	try {
	    if (!usePermanent) {
		client =  new SEClient(SEhost);
	    }
	    String response = client.listDTD();
	    if (!usePermanent) {
		client.closeConnection();
	    }

	    responseDoc = parseXML(response);
	}
	catch (IOException e) { 
	    System.out.println("DM Error: Unable to get DTD List: "+e.getMessage());
	    responseDoc = null; 
	}

	if(responseDoc != null)
	    return DMUtil.parseDTDList(responseDoc);
	return new Vector();
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
        CUtil.memory2persistent(dtdDir,tmpDir+dtd_persist);
        
        // Next need to shutdown XML Cache. 
        cacheM.shutdown();
        return true;        
    }

    public synchronized String toString() 
    {
        String tmpStr = 
            "+-------------------------------+\n"+
            "|        Data Manager Info      |\n"+
            "+-------------------------------+\n"+
            dtdDir.toString() + "\n\n" ;
        return tmpStr;
    }   
    
    // Backward compatibility issue
    public void enableCache() {
        return;
    }
    public void disableCache() {
        return;
    }

    // Interface exported by CacheManager.
    public Document createDoc(String s) {
        return cacheM.createDoc(s);
    }
    public void modifyDoc(Object key, Object val) {
        cacheM.modifyDoc(key, val);
    }
    public void flushDoc(String s) {
        cacheM.flushDoc(s);
    }
    public void deleteDoc(String s) {
        cacheM.deleteDoc(s);
    }
    
    public void storeTuples(Vector tv, String fn) {
	cacheM.storeTuples(fn, tv);
    }

    public void storeTuples(Vector tv) {
        // System.out.println("DM.storeTuple : DM get Vec from ShutD " + tv.size());
        Hashtable vhash = new Hashtable();
        for(int i=0; i<tv.size(); i++) {
            StreamTupleElement ste = (StreamTupleElement) tv.elementAt(i);
            Node n = (Element)ste.getAttribute(ste.size()-1);
            Node firstChild = n.getFirstChild();
            String fn = firstChild.getNodeValue();
            Vector v = (Vector)vhash.get(fn);
            if(v==null) {
                v = new Vector();
                vhash.put(fn, v);
            } 
            ste.removeLastNAttributes(5);
            v.addElement(ste);
        }
        for(Enumeration en = vhash.keys(); en.hasMoreElements() ; ) {
            String k = (String)en.nextElement();
            Vector val = (Vector)vhash.get(k);
            // System.err.println("Tring to store Tuple in " + k);
            cacheM.storeTuples(k, val);
            // System.err.println("end Store Tuple " + k);
        }
        // System.err.println("StoreT. return");
    }
    
    /////////////  Interface for Yuan /////////////////////////
    
    public void setEventDetector(EventDetector ed) {
        cacheM.setEventDetector(ed);
    }
    public boolean monitorTriggerFile(String file, long time, Date d) {
        boolean isTrigTmp = CacheUtil.isTrigTmp(file);
        // if(isTrigTmp) System.err.println("A Trigger TMP Monitored.");
        boolean ret = false;
        try {
            ret = cacheM.Monitor(file, isTrigTmp);
        } catch (IOException ioe) {
            System.err.println("DataM.monitor/CacheM.monitor FileIO error");
        }
        // System.err.println("Setting " + file + " span " + time);
        cacheM.setFileSpan(file, time, isTrigTmp);
        return ret;
    }
    public void unMonitorTriggerFile(String file) {
        cacheM.unMonitor(file, CacheUtil.isTrigTmp(file));
    }
    public void setTriggerFileSpan(String file, long span) {
        cacheM.setFileSpan(file, span, CacheUtil.isTrigTmp(file));
    }
    public boolean getLastModified(String file, Date from, Date to) {
	boolean ret = cacheM.isModified(file, from.getTime(), to.getTime()); 
        return(ret);
    }
    //////////////////////////////////////////////////////////////////
    public Vector getLastModified(Vector files, Date from, Date to) {
	Vector ret = new Vector();
	for(int i=0; i<files.size(); i++) {
	    if(cacheM.isModified((String)files.elementAt(i), from.getTime(), 
			to.getTime() )) 
		ret.addElement(files.elementAt(i));
	}
	return ret;
    }
    //////////////  End //////////////////////////////////////

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



