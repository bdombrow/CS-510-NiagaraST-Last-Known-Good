
/**********************************************************************
  $Id: ServerQueryInfo.java,v 1.3 2002/09/14 04:56:47 vpapad Exp $


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


package niagara.connection_server;

/**Stores the info about a query in the server
*/
import niagara.utils.*;
import niagara.query_engine.*;

public class ServerQueryInfo { 
    
    // constants for the variable queryType
    final static int SearchEngine = 0;
    final static int QueryEngine = 1;
    final static int TriggerEngine = 2;
    final static int GetDTD = 3;
    final static int AccumFile = 4;

    int queryType;

    // The tranmitter thread for this query
    ResultTransmitter transmitter;
    private int queryId;

    // Is this query synchronous? 
    // (Implies no result padding, and closing of connection at the end)
    private boolean synchronous;
    
    // trigger query related data
    String triggerName;
    SynchronizedQueue queryResultQueue;

    // se query related data
    String searchEngineQuery;
    
    // qe query related data
    QueryResult queryResult;

    String accumFileName;

    /**Constructor
       @param queryId The Server Query Id (different from QID given by QE/Client 
       @param queryType Tells the module to which this query belongs
    */
    public ServerQueryInfo(int queryId,int queryType) {
	this.queryId = queryId;
	this.queryType = queryType;
	
	// Initialize the queue of query result objects
	// That exist between the server and Trigger Engine
	if (isTriggerQuery())
	    queryResultQueue = new SynchronizedQueue(100);

	searchEngineQuery = "";
    }

    public ServerQueryInfo(int queryId, int queryType, boolean synchronous) {
        this(queryId, queryType);
        this.synchronous = synchronous;
    }
    
    boolean isTriggerQuery() {
	return queryType == TriggerEngine;
    }

    boolean isSEQuery() {
	return queryType == SearchEngine;
    }

    boolean isQEQuery() {
	return queryType == QueryEngine;
    }
   
    boolean isAccumFileQuery() {
        return queryType == AccumFile;
    }

    boolean isDTDQuery() {
        return queryType == GetDTD;
    }

    boolean isSynchronous() {
        return synchronous;
    }
    
    public int getQueryId() {
	return queryId;
    }
}



