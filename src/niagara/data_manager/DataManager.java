/**********************************************************************
  $Id: DataManager.java,v 1.12 2003/12/24 02:12:09 vpapad Exp $


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
import java.util.*;
import java.io.*;

import niagara.connection_server.Catalog;
import niagara.utils.*;
import niagara.xmlql_parser.*;

/**
 * Initial implementation of data manager no memory management essentially
 * managing two directories
 * 
 * @see YPClient
 * @version 1
 */
public class DataManager {
    private static String dtd_persist = ".dm_dtdcache";
    public CacheManager cacheM;
    private String tmpDir;

    /* Table storing accumulate files */
    public static HashMap AccumFileDir = new HashMap();

    /** Directory containing parsed XML files in a binary format */
    private File localDataDir;

    /** Mapping from URN being stored to their corresponding filename */
    private HashMap urn2file;

    /** The system catalog */
    private Catalog catalog;

    public DataManager(
        Catalog catalog,
        String path,
        int diskSpace,
        int policy,
        int numFetchThreads,
        int numUrlThreads) {
        this.catalog = catalog;

        String localDataDirName =
            catalog.getConfigParam("local data directory");
        localDataDir = new File(localDataDirName);
        if (!localDataDir.exists())
            localDataDir.mkdir();

        urn2file = new HashMap();

        // DTD and YP stuff.
        tmpDir = path;
        File dtdPersistFile = new File(tmpDir + dtd_persist);
        if (dtdPersistFile.exists()) {
            System.out.println(
                "initialize from previous incarnation" + "of data manager...");
            System.out.println(
                "the previous incarnation of "
                    + "dtd directory is persisted at: \n"
                    + tmpDir
                    + dtd_persist);
        }

        // Feng's Code all in CacheManager.
        cacheM = new CacheManager(tmpDir);
    }

    /**
	 * An output stream that can be used to store data for the given resource
	 */
    public FileOutputStream getOutputStreamFor(String urn) throws ShutdownException {
        String tempName = catalog.getNewFileName(urn);
        urn2file.put(urn, tempName);
        File f = new File(localDataDir, tempName);
        try {
            f.createNewFile();
            return new FileOutputStream(f);
        } catch (IOException ioe) {
            throw new ShutdownException(
                "Could not store "
                    + f.getName()
                    + "due to: "
                    + ioe.getMessage());
        }
    }

    /**
     * An input stream that can be used to read data for the given resource
     */
    public FileInputStream getInputStreamFor(String urn) throws ShutdownException {
        String filename = catalog.getFile(urn);
        File f = new File(localDataDir, filename);
        try {
            return new FileInputStream(f);
        } catch (IOException ioe) {
            throw new ShutdownException(
                    "Could not read "
                    + f.getName()
                    + "due to: "
                    + ioe.getMessage());
        }
    }
    
    /** We have finished storing the data for the given resource. */
    public void urnDone(String urn) {
        String fileName = (String) urn2file.remove(urn);
        catalog.addLocal(urn, fileName);
    }

    /**
	 * Generate a SE request from a vector of predicates. Send this request to
	 * the SE using YPClient. The response will be an xml doc. Parse it and
	 * return it to the caller This call will block while the search engine
	 * request is being serviced.
	 * 
	 * @param requestList
	 *                  a vector of requests
	 * @return a TXDocument for the parsed root of xml file containing DTDinfo
	 * @exception DMClosedException
	 *                       this function called while data manager is being closed
	 */

    public synchronized DTDInfo getDTDInfo(Vector requestList) {
        assert false : "Search Engine Not Supported";
        return null;
    }

    /**
	 * Gracefully shutdown the Data Manager by terminating all threads etc.
	 */
    public synchronized boolean shutdown() {
        File dtdPersistFile = new File(tmpDir + dtd_persist);
        if (dtdPersistFile.exists()) {
            dtdPersistFile.delete();
        }

        // Next need to shutdown XML Cache.
        cacheM.shutdown();
        return true;
    }

    public boolean getDocuments(
        Vector xmlURLList,
        regExp pathExpr,
        SinkTupleStream stream)
        throws ShutdownException {
        return cacheM.getDocuments(xmlURLList, pathExpr, stream);
    }
}
