
/**********************************************************************
  $Id: SEClient.java,v 1.2 2001/08/08 21:28:32 tufte Exp $


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


package niagara.search_engine.server;

import java.io.*;
import java.net.*;
import java.util.*;
import niagara.search_engine.util.*;

import org.w3c.dom.*;

/** 
 * Client for unfied server.
 *
 * Continuous session Search Engine query interface.
 * Port number is binded in the compile time and it is available
 * in the Const.java class.
 *
 * Uses generic XML template protocol.
 *
 *
 */

public class SEClient implements Const {
    public static final String PROMPT = "> ";
    public static final String ACTION_COMMAND = "go";
    public static final String CLEAR_COMMAND = "clear";
    public static final String QUIT_COMMAND = "exit";
    public static final String HELP_COMMAND = "help";
    public static final String RELOAD_COMMAND = "reload";

    public static final String LT = "@lessthan";

    private String serverName;	

    private Socket mySocket=null;
    private BufferedReader receiver=null;
    private PrintWriter sender=null;

    public static void main(String[] args) {
	String usage="Usage: Client server-address";
	if (args.length < 1) {
	    System.err.println(usage);
	    System.exit(1);
	}
	try {
	    SEClient client = new SEClient(args[0]);
	    client.startClient();
	} catch (UnknownHostException e) {
	    System.err.println("ERROR: unknown host: "+e);
	} catch (IOException e) {
	    System.err.println("ERROR: failed I/O: "+e);
	}
    }
  
    public SEClient(String serverName) throws IOException, UnknownHostException {
	this.serverName=serverName;
	contactServer();
    }

    protected void contactServer() throws UnknownHostException,IOException {
	mySocket = new Socket(serverName, SERVER_PORT);
	receiver=new BufferedReader(new InputStreamReader(mySocket.getInputStream()));
	sender=new PrintWriter(new OutputStreamWriter(mySocket.getOutputStream()));
    }

    public String query(String request) {
	sendQuery(request);
	return getResponse();
    }

    public String listDTD() {
	sendListDTD();

        String s =  getResponse();

	return s;
    }

    public String index(String url) {
	sendTypeRequest(SEQueryHandler.INDEX, url);
	return getResponse();
    }

    /**
     *  returns a list of results.
     *  convenient method for get the list of urls.
     *  it gets the result from server and parses the response xml for you.
     */
    public Vector queryList(String request) {
	String response = query(request);

	//TXDocument responseDoc = SEQueryHandler.parseXML(response);

	return parseList(response);
    }

    public void startClient() throws IOException {
	System.out.println();
	System.out.println("Connected to XML Search Engine at "+serverName);
	System.out.println();
	BufferedReader in = new BufferedReader(new InputStreamReader(System.in));

	System.out.print(PROMPT);
	String line,query="";
    
	while((line=in.readLine())!=null) {
	    line=line.trim();
	    if (line.equals(ACTION_COMMAND)) {
		if (query.startsWith(QUERY)) {
		    sendQuery(query.substring(6));
		    printResult();
		} else {
		    System.out.println("Error: illegal query protocol");
		}
	  
		query="";
	    } else if (line.equals("LISTDTD")) {
		sendListDTD();
		printDTDList();
		query="";
	    } else if (line.startsWith("INDEX")) {
		String res = index(line.substring(5));
		if (res.startsWith("Error")) {
		    System.out.println(res);
		} else {
		    System.out.println(getMessage(res));
		}
		query="";
	    } else if (line.startsWith("FLUSH")) {
		String res = flush();
		if (res.startsWith("Error")) {
		    System.out.println(res);
		} else {
		    System.out.println(getMessage(res));
		}
		query="";
	    } else if (line.startsWith("SHUTDOWN")) {
		String res = shutdown();
		if (res.startsWith("Error")) {
		    System.out.println(res);
		} else {
		    System.out.println(getMessage(res));
		}
		query="";
	    } else if (line.equals(CLEAR_COMMAND)) {
		query="";
	    } else if (line.equals(HELP_COMMAND)) {
		printHelpMessage();
	    } else if (line.equals(RELOAD_COMMAND)) {
		/*
		  sender.println(RELOAD);
		  sender.flush();
		*/
	    } else if (line.equals(QUIT_COMMAND)) {
		/*
		  sender.println(QUIT);
		  sender.flush();
		*/
		closeConnection();
		return;
	    } else {
		query+=line+" ";
	    }
	    System.out.print(PROMPT);
	}
    }

    protected String flush() {
	sendTypeRequest(SEQueryHandler.FLUSH);
	return getResponse();
    }

    protected String shutdown() {
	sendTypeRequest(SEQueryHandler.SHUTDOWN);
	return getResponse();
    }

    protected void sendQuery(String query) {

	String queryEncoded = encode(query);

	sender.println(SEQueryHandler.OREQUEST);
	sender.println(SEQueryHandler.OTYPE+"0"+
		       SEQueryHandler.CTYPE);
	sender.println(SEQueryHandler.OQUERY);
	sender.println(queryEncoded);
	sender.println(SEQueryHandler.CQUERY);
	sender.println(SEQueryHandler.CREQUEST);

	sender.flush();
    }

    public static String encode(String str) {
	
	String remainder = str;
	String s="";
	int idx = 0;
	
	while ((idx = remainder.indexOf("<"))>=0) {
	    s += remainder.substring(0,idx) + LT;
	    remainder = remainder.substring(idx+1);
	}

	s += remainder;

	return s;
    }
	
    public static String decode(String str) {
	
	String remainder = str;
	String s="";
	int idx = 0;
	
	while ((idx = remainder.indexOf(LT))>=0) {
	    s += remainder.substring(0,idx) + "<";
	    remainder = remainder.substring(idx+LT.length());
	}
	
	s += remainder;

	return s;
    }
	

    protected void printHelpMessage() {
	System.out.println("** A request to server starts with one of the following query protocols and ends with '"+ACTION_COMMAND+"' command. Command-line commands are processed sorely in the client. All protocols and command-line commands start at the beginning of a new line.");
	System.out.println();
	System.out.println("EXAMPLE:");
	System.out.println("> QUERY");
	System.out.println("> title contains (\"java\" AND \"programming\")");
	System.out.println("> go");
	System.out.println();
	System.out.println("QUERY PROTOCOLS to SERVER:");
	System.out.println("\t"+QUERY+"\t body of the query");
	System.out.println("\t"+INDEX+"\t url to index");
	System.out.println("\t"+RELOAD);
	System.out.println("\t"+FLUSH);
	System.out.println("\t"+SHUTDOWN);
	System.out.println();
	System.out.println("COMMAND-LINE COMMANDS for CLIENT:");
	System.out.println("\t"+ACTION_COMMAND+"\t execute queries");
	System.out.println("\t"+CLEAR_COMMAND+"\t clear inputs and reset query handle");
	System.out.println("\t"+HELP_COMMAND+"\t print this message");
	System.out.println("\t"+QUIT_COMMAND+"\t exit client session");
    }

    public String getResponse() {
	BufferedReader br = null;
	String request = "";
	String line=null;

	try {
	    while((line=receiver.readLine()) != null) {
		request += line+"\n";
		
		//protocol must be followed in case-sensitive manner
		if (line.indexOf(SEQueryHandler.CRESULT) >= 0 || line.startsWith("Error")) 
		    break;
	    }
	    
	    if (request.trim().equals("")) 
		return null;
	    return request;
	} catch (Exception e) {
	    System.out.println("ERROR: failed I/O");
	    return null;
	}
    }
    
    protected void printResult() {
	String result = getResponse();
	if (result == null) 
	    return;

	if (result.startsWith("Error")) {
	    System.out.println(result);
	    return;
	}

	Document doc = SEQueryHandler.parseXML(result);
		
	NodeList nodes = doc.getElementsByTagName(SEQueryHandler.ITEM);
	int n = nodes.getLength();
	
	if (n<=0) return;
	
	for (int i=0;i<n;i++) {
	    Node  node = nodes.item(i);
	    String url = node.getFirstChild().getNodeValue();
	    System.out.println(url);
	}
    }

    protected void sendListDTD() {
	sendTypeRequest(1);
    }


    private void sendTypeRequest(int type) {
	sender.println(SEQueryHandler.OREQUEST);
	sender.println(SEQueryHandler.OTYPE+type+SEQueryHandler.CTYPE);
	sender.println(SEQueryHandler.CREQUEST);

	sender.flush();
    }

    private void sendTypeRequest(int type, String query) {
	String queryEncoded = encode(query);
	
	sender.println(SEQueryHandler.OREQUEST);
	sender.println(SEQueryHandler.OTYPE+type+SEQueryHandler.CTYPE);
	sender.println(SEQueryHandler.OQUERY);
	sender.println(queryEncoded);
	sender.println(SEQueryHandler.CQUERY);
	sender.println(SEQueryHandler.CREQUEST);

	sender.flush();
    }


    protected void printDTDList() {
	String result = getResponse();
	if (result == null) 
	    return;
	Document doc = SEQueryHandler.parseXML(result);
		
	NodeList nodes = doc.getElementsByTagName(SEQueryHandler.ITEM);
	int n = nodes.getLength();
	
	if (n<=0) return;
	
	for (int i=0;i<n;i++) {
	    Node  node = nodes.item(i);
	    String dtd = node.getFirstChild().getNodeValue();
	    System.out.println(dtd);
	}
    }

    public void closeConnection() throws IOException {
	if (sender!=null) sender.close();
	if (receiver!=null) receiver.close();
	if (mySocket!=null) mySocket.close();
    }

    public static Vector parseList(String response) {
	Vector v = new Vector();

	if(response == null)
	    return v;
	
	Document responseDoc = SEQueryHandler.parseXML(response);
	
	return parseList(responseDoc);
    }

    public static Vector parseList(Document listDoc) {
	Vector v = new Vector();

	if(listDoc == null)
	    return v;

	NodeList list = listDoc.getElementsByTagName(SEQueryHandler.ITEM);
	int size = list.getLength();
	    
	for(int j = 0; j < size; j++){
	    String nodevalue = list.item(j).getFirstChild().getNodeValue();

	    v.addElement(nodevalue);
	}
	    
	return v;
    }

    public static String getMessage(String response) {
	if(response == null)
	    return null;
	
	Document responseDoc = SEQueryHandler.parseXML(response);
	
	return getMessage(responseDoc);
    }

    public static String getMessage(Document listDoc) {
	String message=null;

	if(listDoc == null)
	    return null;

	NodeList list = listDoc.getElementsByTagName(SEQueryHandler.ITEM);
	int size = list.getLength();
	
	if (size > 0) {
	    message = list.item(0).getFirstChild().getNodeValue();
	} 

	return message;
    }
}
 

    
    
