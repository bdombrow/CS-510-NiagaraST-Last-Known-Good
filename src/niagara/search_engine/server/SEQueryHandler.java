
/**********************************************************************
  $Id: SEQueryHandler.java,v 1.2 2001/08/08 21:28:32 tufte Exp $


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

import org.w3c.dom.*;
import org.xml.sax.InputSource;
import java.io.*;
import java.net.*;
import java.util.*;
import java_cup.runtime.*;

import niagara.ndom.*;
import niagara.search_engine.seql.*;
import niagara.search_engine.util.*;
import niagara.search_engine.operators.*;
import niagara.search_engine.indexmgr.*;
import niagara.xmlql_parser.op_tree.*;
import niagara.xmlql_parser.syntax_tree.*;

/** 
 * Query Handler for the SEServer.
 * It works with Generic XML protocol.
 *
 * ---- Request Template ----
 *<?xml encoding="US-ASCII"?>
 *<!ELEMENT request (type, query?)>
 *<!ELEMENT type (#PCDATA)>
 *<!ELEMENT query (#PCDATA)>
 *
 * ---- Response Template ----
 *<?xml encoding="US-ASCII"?>
 *<!ELEMENT result (item*)>
 *<!ELEMENT item (#PCDATA)>
 *
 *
 */

public class SEQueryHandler implements Runnable {
    //single  seql query request
    public static final int DOC_LIST = 0;
    public static final int DTD_LIST = 1;
    public static final int RELOAD = 2;
    public static final int INDEX = 3;
    public static final int FLUSH = 4;
    public static final int SHUTDOWN = 5;

    // for testing generated plans
    public static final int GEN_PLAN = 6;


    public static final String REQUEST = "request";
    public static final String TYPE = "type";
    public static final String QUERY = "query";
    public static final String RESULT = "result";
    public static final String URL = "url";
    public static final String DTD = "dtd";
    public static final String ITEM = "item";
    public static final String OREQUEST = openTag(REQUEST);
    public static final String OTYPE = openTag(TYPE);
    public static final String OQUERY = openTag(QUERY);
    public static final String ORESULT = openTag(RESULT);
    public static final String OURL = openTag(URL);
    public static final String ODTD = openTag(DTD);
    public static final String OITEM = openTag(ITEM);
    public static final String CREQUEST = closeTag(REQUEST);
    public static final String CTYPE = closeTag(TYPE);
    public static final String CQUERY = closeTag(QUERY);
    public static final String CRESULT = closeTag(RESULT);
    public static final String CURL = closeTag(URL);
    public static final String CDTD = closeTag(DTD);
    public static final String CITEM = closeTag(ITEM);

    
    private Socket mySocket=null;
    private BufferedReader receiver=null;
    private PrintWriter sender=null;

    public SEQueryHandler(Socket socket) {
	mySocket=socket;
    }
  
    public void run() {
	String request;
    
	System.out.println("SERVER: Client connected from... "+mySocket.getInetAddress());
	try {
	    sender = new PrintWriter(new OutputStreamWriter(mySocket.getOutputStream()));
	    receiver = new BufferedReader(new InputStreamReader(mySocket.getInputStream()));

	    String query;
	    while((request=getRequest()) != null) {

		Document doc = parseXML(request);

		int type = getRequestType(doc);
		
		switch(type) {
		case DOC_LIST:
		    query = getQueryString(doc);
		    processQuery(query);
		    break;
		case DTD_LIST:
		    processDTDListRequest();
		    break;
		case INDEX:
		    // process indexing request
		    String url = getQueryString(doc);
		    addURL(url);
		    break;
		case FLUSH:
		    // shutdown server gracefully
		    flushIndex();
		    break;
		case SHUTDOWN:
		    // shutdown server gracefully
		    //		    flushIndex();
		    printMessage("Server shutdown!");
		    System.exit(0);
		    break;
		case GEN_PLAN:
		    query = getQueryString(doc);
		    generatePlan(query);
		    break;
		default:
		    printErrorMessage("Error: wrong request type");
		    break;
		}
	    }
	    sender.flush();
	    System.out.println("SERVER: Client connection closed... "+mySocket.getInetAddress()+"\n");
	} catch (IOException e) {
	    System.err.println("Error: failed I/O :"+e);
	} finally {
	    try {
		if (sender!=null) sender.close();
		if (receiver!=null) receiver.close();
		if (mySocket!=null) mySocket.close();
	    } catch (Exception e) {}
	}
    }

    protected void printMessage(String message) {
	sender.println(ORESULT);
	sender.print(OITEM);
	sender.print(message);
	sender.println(CITEM);
	sender.println(CRESULT);
	sender.flush();
    }

    protected void printErrorMessage(String error) {
	sender.println(error);
	sender.flush();
    }

    protected void processQuery(String query) {
	try {
	    //      System.out.println(""+new Date()+"\n"+query);
	    parser myParser = new parser(new Yylex(new StringReader(query)));
    
	    QueryPlan plan=null;
	    //SEQL query parse
	    try {
		plan = myParser.getPlan(); // do the parse
	    } catch (Throwable e){
		printErrorMessage("Error: syntax error in query");
		return;
	    }

	    //    plan.dump();
	    try {
		plan.writeDot(plan.makeDot(), new FileWriter("QEP.dot"));
	    } catch (Exception e) { e.printStackTrace(); } 
	    

	    if (SEServer.useOptimizer) {
		//		System.out.println("Optimizing Execution Plan...");
		QueryPlan optimizedPlan = Optimizer.optimize(plan);
		plan = optimizedPlan;

		try {
		    plan.writeDot(plan.makeDot(), new FileWriter("QEP_OPT.dot"));
		} catch (Exception e) { e.printStackTrace(); } 
	    }

	    plan.eval();

	    //	    	    System.out.println("Query Evaluated...");

	    AbstractOperator op = plan.getOperator();
	    
	    
	    Table table; Tuple tp; Header header;
	    
	    Vector v = op.getResult();
	    if (v == null) {
		table = new Table();
	    } else {
		table = getTable(v);
	    }

	    //header = table.getHeader();
	    
	    sender.println(ORESULT);
	    for (int i=0; i < table.size(); i++) {
		tp = table.getTuple(i);
		sender.print(OITEM);
		sender.print(tp);
		sender.println(CITEM);
	    }
	    sender.println(CRESULT);
	    sender.flush();

	    return;

	} catch (Throwable e) {
	    printErrorMessage("Error: can't evaluate query");
	}
    }

    protected void generatePlan(String query) {
	try {
	    //      System.out.println(""+new Date()+"\n"+query);
	    parser myParser = new parser(new Yylex(new StringReader(query)));
    
	    QueryPlan plan=null;
	    //SEQL query parse
	    try {
		plan = myParser.getPlan(); // do the parse
	    } catch (Throwable e){
		printErrorMessage("Error: syntax error in query");
		return;
	    }

	    //    plan.dump();
	    try {
		plan.writeDot(plan.makeDot(), new FileWriter("QEP.dot"));
	    } catch (Exception e) { e.printStackTrace(); } 
	    

	    if (SEServer.useOptimizer) {
		//		System.out.println("Optimizing Execution Plan...");
		QueryPlan optimizedPlan = Optimizer.optimize(plan);
		plan = optimizedPlan;

		try {
		    plan.writeDot(plan.makeDot(), new FileWriter("QEP_OPT.dot"));
		} catch (Exception e) { e.printStackTrace(); } 
	    }

	    printMessage("Plan Generated...");

	    return;

	} catch (Throwable e) {
	    printErrorMessage("Error: can't evaluate query");
	}
    }

    public void processDTDListRequest() {
	Vector dtdList = IndexMgr.idxmgr.getDTDs();
		
	sender.println(ORESULT);
	for(int i=0;i<dtdList.size();i++) {
	    sender.print(OITEM);
	    sender.print((String)dtdList.elementAt(i));
	    sender.println(CITEM);
	}
	sender.println(CRESULT);
	sender.flush();
    }


    private void reload() {
    }

    private void addURL(String url) {
	try {
	    String response;
	    response = IndexMgr.idxmgr.index(new URL(url));
	    if (response == null)
		printMessage("URL "+url+" indexed...");
	    else 
		printErrorMessage("Error: indexing failed on URL "+url+"...");
	} catch (Exception e) {
	    printErrorMessage("Error: indexing failed on URL "+url+"...");
	    //	    System.err.println(e);
	    //	    e.printStackTrace();
	}
    }

    private void flushIndex() {
	try {
	    IndexMgr.idxmgr.flush();
	    printMessage("Index flushed!");
	    //	    sender.println("done");
	} catch (Exception e) {
	    printErrorMessage("Error: index fulsh failed...");
	    //	    e.printStackTrace();
	}
    }
    /**
     * A Tuple is a vector.  The first element is a string of url, and
     * the second element is a vector of qualified strings.
     */

        
    public Tuple getTuple(IVLEntry ivlEntry) 
    {
	Tuple tuple = new Tuple();
	long docno = ivlEntry.getDocNo();
	
	String docName = (IndexMgr.idxmgr).getDocName(docno);

	//doc name as the first element. 
	tuple.addElement(docName);

	return tuple;
	
    }  //end of getTuple()
 
    public Table getTable(Vector ivl) 
    {
	Table tb = new Table();

	tb.addColName(URL);
	//tb.addColName(TEXT);
	
	for (int i = 0; i< ivl.size(); i++) {
	    IVLEntry ivlEntry = (IVLEntry)ivl.elementAt(i);
	    Tuple tuple = getTuple(ivlEntry);
	    tb.addTuple(tuple);
	}
	return tb;
    }

    public String getRequest() {
	BufferedReader br = null;
	String request = "";
	String line=null;

	try {
	    while((line=receiver.readLine()) != null) {
		request += line+"\n";
		
		System.out.println(line);

		//protocol must be followed in case-sensitive manner
		if (line.indexOf(CREQUEST) >= 0) 
		    break;
	    }
	    
	    if (request.trim().equals("")) {
		return null;
	    }
	    return request;
	} catch (Exception e) {

	    return null;
	}
    }
    
    public static int getRequestType(Document doc) {
	NodeList nodes = doc.getElementsByTagName(TYPE);
	int n = nodes.getLength();
	
	if (n<=0) return -1;
	
	Node  node = nodes.item(0);
	String type = node.getFirstChild().getNodeValue();
	int typen = Integer.parseInt(type);

	return typen;
    }

    public static String getQueryString(Document doc) {
	NodeList nodes = doc.getElementsByTagName(QUERY);
	int n = nodes.getLength();
	
	if (n<=0) return null;
	
	Node  node = nodes.item(0);
	String query = node.getFirstChild().getNodeValue();

	String result = SEClient.decode(query);

	System.out.println("SEQL Query: "+result);

	return result;
    }

    public static Document parseXML(Reader reader) {
        niagara.ndom.DOMParser p = DOMFactory.newParser(); 
	Document doc = DOMFactory.newDocument();

	// KT: remove when new code works p.setElementFactory(doc);
	    
	try {
	    p.parse(new InputSource(reader));
	    // KT: code from before ndom - remove when new stuff works:p.readStream(reader);
	}   catch (Exception e) {
	    e.printStackTrace();
	    return null;
	}
	return doc;
    }

    public static Document parseXML(String str) {
	niagara.ndom.DOMParser p = DOMFactory.newParser();

	/* KT - code from before ndom - remove when new stuff works
	 * p.setWarningNoDoctypeDecl(false);
	 *  p.setWarningNoXMLDecl(false);
	 *  p.setKeepComment(false);
	 */
	Document doc = DOMFactory.newDocument();
	// KT - remove p.setElementFactory(doc);
	    
	    try {
		p.parse(new InputSource(new StringReader(str)));
		// KT - remove: p.readStream(new StringReader(str));
	    }   catch (Exception e) {
		e.printStackTrace();
		return null;
	    }
	    return doc;
    }

    public static String openTag(String tag) {
	return "<"+tag+">";
    }

    public static String closeTag(String tag) {
	return "</"+tag+">";
    }

    
}

