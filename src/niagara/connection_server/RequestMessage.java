
/**********************************************************************
  $Id: RequestMessage.java,v 1.6 2003/01/13 05:05:43 tufte Exp $


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

/** Contains the request message in an object form and implements some utility methods
*/

import niagara.utils.*;

public class RequestMessage {
    
    //Assigning numbers to request types
    // for "switch"ing purposes
    static final int EXECUTE_QE_QUERY = 0;
    static final int EXECUTE_SE_QUERY = 1;
    static final int EXECUTE_TRIGGER_QUERY = 2;
    static final int KILL_QUERY =3;
    static final int SUSPEND_QUERY = 4;
    static final int RESUME_QUERY = 5;
    static final int GET_NEXT = 6;
    static final int GET_DTD_LIST = 7;
    static final int GET_PARTIAL = 8;
    static final int GET_DTD = 9;
    // Execute a prepared query plan, described in XML
    static final int EXECUTE_QP_QUERY = 10; 
    static final int RUN_GC = 11; // run the garbage collector
    static final int SHUTDOWN = 12;
    // Same as QP_QUERY, but start sending back results immediately,
    // assuming this is the only outstanding query for this client
    // (EXECUTE_QP_QUERY and GET_NEXT in one step)
    static final int SYNCHRONOUS_QP_QUERY = 13;
    // Just show the optimized plan
    static final int EXPLAIN_QP_QUERY = 14;
    // A mutant query
    static final int MQP_QUERY = 15;
    static final int DUMPDATA = 16;

    String requestType;
    int serverID;
    int localID;

    String requestData;

    public RequestMessage() {
	serverID = -1;
	localID = -1;
    }

    public RequestMessage(int intType) throws InvalidRequestTypeException{
	setRequestType(intType);
	serverID = -1;
	localID = -1;
    }

    public void setRequestType(int intType) throws InvalidRequestTypeException{
	switch (intType) {
	case EXECUTE_QE_QUERY: this.requestType = "execute_qe_query";break;
	case EXECUTE_SE_QUERY: this.requestType = "execute_se_query";break;
	case EXECUTE_TRIGGER_QUERY:this.requestType = "execute_trigger_query";break;
	case KILL_QUERY:this.requestType = "kill_query";break;
	case SUSPEND_QUERY:this.requestType = "suspend_query";break;
	case RESUME_QUERY:this.requestType = "resume_query";break;
	case GET_NEXT:this.requestType = "get_next";break;
	case GET_DTD_LIST:this.requestType = "get_dtd_list";break;
	case GET_PARTIAL:this.requestType = "get_partial";break;
	case GET_DTD:this.requestType = "get_dtd";break;
	case EXECUTE_QP_QUERY: this.requestType = "execute_qp_query";break;
        case SYNCHRONOUS_QP_QUERY: this.requestType = "synchronous_qp_query";break;
        case EXPLAIN_QP_QUERY: this.requestType = "explain_qp_query";break;
	default: throw new InvalidRequestTypeException();
	}
    }

    // get the request type of this Message as an integer
    public int getIntRequestType() {
	if (requestType.equals("execute_qe_query"))
	    return EXECUTE_QE_QUERY;
	if (requestType.equals("execute_se_query"))
	    return EXECUTE_SE_QUERY;
	if (requestType.equals("execute_trigger_query"))
	    return EXECUTE_TRIGGER_QUERY;
	if (requestType.equals("kill_query"))
	    return KILL_QUERY;
	if (requestType.equals("suspend_query"))
	    return SUSPEND_QUERY;
	if (requestType.equals("resume_query"))
	    return RESUME_QUERY;
	if (requestType.equals("get_next"))
	    return GET_NEXT;
	if (requestType.equals("get_dtd_list"))
	    return GET_DTD_LIST;
	if (requestType.equals("get_partial"))
	    return GET_PARTIAL;
	if (requestType.equals("get_dtd"))
	    return GET_DTD;
	if (requestType.equals("execute_qp_query"))
	    return EXECUTE_QP_QUERY;
	if (requestType.equals("gc"))
	    return RUN_GC;
        if (requestType.equals("shutdown"))
	    return SHUTDOWN;
	if (requestType.equals("dumpdata"))
	    return DUMPDATA;
        if (requestType.equals("synchronous_qp_query"))
            return SYNCHRONOUS_QP_QUERY;
        if (requestType.equals("explain_qp_query"))
            return EXPLAIN_QP_QUERY;
        if (requestType.equals("mqp_query"))
            return MQP_QUERY;
	throw new PEException("Invalid request type: " + requestType);
    }

    class InvalidRequestTypeException extends Exception {
    }

	
}
