
/**********************************************************************
  $Id: ResponseMessage.java,v 1.11 2003/08/01 17:28:15 tufte Exp $


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

import java.io.IOException;
import java.io.Writer;

import org.w3c.dom.Node;

import niagara.ndom.saxdom.NodeImpl;
import niagara.query_engine.QueryResult;
import niagara.utils.XMLUtils;


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
    public static final int EXECUTION_ERROR = 9;

    /**
     * local query ID
     */
    public int localID;
    /**
     * server query ID. The ID of the query on the server
     */
    private int serverID;
    /**
     * Message type
     */
    public int type;
    /**
     * Object corresponding to responsedata
     */
    private StringBuffer responseData;
    
    public void clearData() {
        responseData.setLength(0);
    }
    
    public int dataSize() {
        return responseData.length();
    }
    
    public void appendData(String moreData) {
        responseData.append(moreData);
    }
    
    public int getServerID() {
    	return serverID;
    }
    
    public void appendResultData(QueryResult.ResultObject ro, 
                                 boolean prettyprint) {
        // XXX vpapad: really, really ugly
        Node node = (Node) ro.result;
        if (node instanceof NodeImpl) {
            ((NodeImpl) node).flatten(responseData, prettyprint);
        } else {
            XMLUtils.flatten(ro.result, responseData, false, prettyprint);
         }

    }
    
    public void setData(String data) {
        responseData.setLength(0);
        responseData.append(data);
    }
    
    /**
     * Constructor
     */
    public ResponseMessage(RequestMessage request, int type) {
		this.localID = request.localID;
		this.serverID = request.serverID;
		this.type = type;
		responseData = new StringBuffer();
    }

    /**Convert this response to an XML string
       @return The XML representation of this response
    */
    public void toXML(Writer writer, boolean padding) throws IOException {
        if (padding) {
            writer.write("<responseMessage localID =\"");
            writer.write(String.valueOf(localID));
	    	writer.write("\" serverID = \"");
            writer.write(String.valueOf(serverID));
            writer.write("\" responseType = \"");
            writer.write(getResponseType());
            writer.write("\">\n<responseData>\n");
        }
	boolean termnl = true; // is there a terminating new line??
	if(responseData.length() > 0 && 
	   responseData.charAt(responseData.length()-1) != '\n')
	    termnl = false;
        writer.write(responseData.toString());
        if (padding) {
	    // have to do this - client assumes that responseData is on
	    // its own line KT
	    if(!termnl) {
		writer.write("\n");
	    }
    	    writer.write("</responseData></responseMessage>\n");
        }
        if (type == END_RESULT) {
            writer.write("</response>\n");
	}
    }

    /** Get the responseType of the message in this object
     */
    public String getResponseType() {
	switch (type) {
	case QUERY_RESULT: return "query_result";
	case END_RESULT: return "end_result";
	case DTD_LIST : return "dtd_list";
	case PARSE_ERROR : return "parse_error";
	case ERROR : return "server_error";
	case SERVER_QUERY_ID: return "server_query_id";
	case SE_QUERY_RESULT: return "se_query_result";
	case DTD: return "dtd";
	case EXECUTION_ERROR: return "execution_error";
	default: assert false: "Invalid type " + type;
	    return "";
	}
    }
}








