
/**********************************************************************
  $Id: ServerQueryInfo.java,v 1.6 2003/12/24 02:16:38 vpapad Exp $


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

import niagara.query_engine.*;

/**Stores the info about a query in the server */
public class ServerQueryInfo {

    // constants for the variable queryType
    public final static int QueryEngine = 1;
    public final static int AccumFile = 4;

    private int queryType;

    // The tranmitter thread for this query
    private ResultTransmitter transmitter;
    private int queryId;

    // Is this query synchronous? 
    // (Implies no result padding, and closing of connection at the end)
    private boolean synchronous;

    // qe query related data
    private QueryResult queryResult;
    private String accumFileName;

    /**Constructor
       @param queryId The Server Query Id (different from QID given by QE/Client 
       @param queryType Tells the module to which this query belongs
    */
    public ServerQueryInfo(int queryId, int queryType) {
        this.queryId = queryId;
        this.queryType = queryType;
    }

    public ServerQueryInfo(int queryId, int queryType, boolean synchronous) {
        this(queryId, queryType);
        this.synchronous = synchronous;
    }

    boolean isQEQuery() {
        return queryType == QueryEngine;
    }

    boolean isAccumFileQuery() {
        return queryType == AccumFile;
    }

    boolean isSynchronous() {
        return synchronous;
    }

    public int getQueryId() {
        return queryId;
    }
    
    public void setQueryResult(QueryResult qr) {
    	queryResult = qr;	
    }
    
    public void setTransmitter(ResultTransmitter rt) {
        transmitter = rt;	
    }
    
    public void setAccumFileName(String afName) {
    	accumFileName = afName;
    }
    
    public ResultTransmitter getTransmitter() {
    	return transmitter;
    }
    
    public QueryResult getQueryResult() {
    		return queryResult;
    }
    
    public String getAccumFileName() {
    	return accumFileName;
    }
}
