
/**********************************************************************
  $Id: ResponseMessage.java,v 1.1 2000/05/30 21:03:26 tufte Exp $


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


/**
 * This is the class that hold the information of the response message
 */

class ResponseMessage
{
    // Public Constants
    public static final int QUERY_RESULT = 0;
    public static final int END_RESULT = 1;
    public static final int DTD_LIST = 2;
    public static final int INVALID_SERVER_ID = 3;
    public static final int PARSE_ERROR = 4;
    public static final int ERROR = 5;
    public static final int SERVER_QUERY_ID = 6;
    public static final int SE_QUERY_RESULT = 7;
    public static final int DTD = 8;

    /**
     * local query ID
     */
    public int localID;
    /**
     * server query ID. The ID of the query on the server
     */
    public int serverID;
    /**
     * Message type
     */
    public int type;
    /**
     * Object corresponding to responsedata
     */
    public String responseData;
    
    /**
     * Constructor
     */
    public ResponseMessage(RequestMessage request, int type)
    {
	this.localID = request.localID;
	this.serverID = request.serverID;
	this.type = type;
	responseData = "";
    }

    /**Convert this response to an XML string
       @return The XML representation of this response
    */
    public String toXML() {
	String result = "<responseMessage localID =\"" + localID +
	    "\" serverID = \"" + serverID + 
	    "\" responseType = \"" + getResponseType() + "\">\n" +
	    "<responseData>"+
	    responseData +
	    "</responseData>" +
	    "</responseMessage>\n";
	return result;
    }

    /** Get the responseType of the message in this object
     */
    public String getResponseType() {
	switch (type) {
	case QUERY_RESULT: return "query_result";
	case END_RESULT: return "end_result";
	case DTD_LIST : return "dtd_list";
	case INVALID_SERVER_ID : return "invalid_server_id";
	case PARSE_ERROR : return "parse_error";
	case ERROR : return "No URLS found";
	case SERVER_QUERY_ID: return "server_query_id";
	case SE_QUERY_RESULT: return "se_query_result";
	case DTD: return "dtd";
	}
	System.out.println("Invalid type"+type);
	return "";
    }
}








