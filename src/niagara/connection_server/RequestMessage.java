
/**********************************************************************
  $Id: RequestMessage.java,v 1.1 2000/05/30 21:03:26 tufte Exp $


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
	default: throw new InvalidRequestTypeException();
	}
    }

    // get the request type of this Message as an integer
    public int getIntRequestType() {
	int intRequestType=-1;

	if (requestType.equals("execute_qe_query"))
	    intRequestType = EXECUTE_QE_QUERY;
	if (requestType.equals("execute_se_query"))
	    intRequestType = EXECUTE_SE_QUERY;
	if (requestType.equals("execute_trigger_query"))
	    intRequestType = EXECUTE_TRIGGER_QUERY;
	if (requestType.equals("kill_query"))
	    intRequestType = KILL_QUERY;
	if (requestType.equals("suspend_query"))
	    intRequestType = SUSPEND_QUERY;
	if (requestType.equals("resume_query"))
	    intRequestType = RESUME_QUERY;
	if (requestType.equals("get_next"))
	    intRequestType = GET_NEXT;
	if (requestType.equals("get_dtd_list"))
	    intRequestType = GET_DTD_LIST;
	if (requestType.equals("get_partial"))
	    intRequestType = GET_PARTIAL;
	if (requestType.equals("get_dtd"))
	    intRequestType = GET_DTD;
	return intRequestType;
    }

    class InvalidRequestTypeException extends Exception {
    }

	
}
