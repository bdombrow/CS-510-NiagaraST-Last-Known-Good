
/**********************************************************************
  $Id: QueryHandler.java,v 1.1 2000/05/30 21:03:28 tufte Exp $


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
import java_cup.runtime.*;
import niagara.search_engine.seql.*;
import niagara.search_engine.util.*;
import niagara.search_engine.operators.*;
import niagara.search_engine.indexmgr.*;

/** 
 * Query Handler for the SEServer.
 * It works with simple command-line protocol.
 *
 *
 */
public class QueryHandler implements Runnable, Const {
    private Socket mySocket=null;
    private BufferedReader receiver=null;
    private PrintWriter sender=null;

    public QueryHandler(Socket socket) {
	mySocket=socket;
    }
  
    public void run() {
	String line;
    
	try {
	    sender = new PrintWriter(new OutputStreamWriter(mySocket.getOutputStream()));
	    receiver = new BufferedReader(new InputStreamReader(mySocket.getInputStream()));

	    while((line=receiver.readLine()) != null) {
		if (line.startsWith(QUERY)) {
		    String query = line.substring(QUERY.length());
		    processQuery(query);
		} else if (line.startsWith(SQUERY)) {
		    String query = line.substring(SQUERY.length());
		    processSingleQuery(query);
		    break;
		} else if (line.startsWith(RELOAD)) {
		    reload();
		} else if (line.startsWith(SRELOAD)) {
		    reload();
		    break;
		} else if (line.startsWith(INDEX)) {
		    String url = line.substring(INDEX.length());
		    addURL(url);
		} else if (line.startsWith(SINDEX)) {
		    String url = line.substring(SINDEX.length());
		    addURL(url);
		    break;
		} else if (line.startsWith(ADDURL)) {
		    String url = line.substring(ADDURL.length());
		    addURL(url);
		    break;
		} else if (line.startsWith(FLUSH)) {
		    flushIndex();
		    break;
		} else if (line.startsWith(SFLUSH)) {
		    flushIndex();
		    break;
		} else if (line.startsWith(SHUTDOWN)) {
		    flushIndex();
		    printMessage("Server shutdown!");
		    System.exit(0);
		} else if (line.startsWith(SSHUTDOWN)) {
		    flushIndex();
		    printSingleMessage("Server shutdown!");
		    System.exit(0);
		} else if (line.startsWith(QUIT)) {
		    break;
		} else {
		    printErrorMessage("unknown command: "+line);
		}
	    }
	    sender.flush();
	} catch (IOException e) {
	    System.err.println("ERROR: failed I/O :"+e);
	} finally {
	    try {
		if (sender!=null) sender.close();
		if (receiver!=null) receiver.close();
		if (mySocket!=null) mySocket.close();
	    } catch (Exception e) {}
	}
    }
    protected void printMessage(String message) {
	sender.println(RESULT);
	sender.println(message);
	sender.println(END_RESULT);
	sender.flush();
    }

    protected void printSingleMessage(String message) {
	sender.println(message);
	sender.flush();
    }

    protected void printErrorMessage(String error) {
	printMessage(error);
    }
    protected void printSingleErrorMessage(String error) {
	sender.println(error);
	sender.flush();
    }

    protected void processSingleQuery(String query) {
	processQuery(query, true);
    }
    protected void processQuery(String query) {
	processQuery(query, false);
    }
    protected void processQuery(String query, boolean single) {
	try {
	    //      System.out.println(""+new Date()+"\n"+query);
	    parser myParser = new parser(new Yylex(new StringReader(query)));
    
	    QueryPlan plan=null;
	    //SEQL query parse
	    try {
		plan = myParser.getPlan(); // do the parse
	    } catch (Throwable e){
		if(!single)
		    printErrorMessage("syntax error in query: \n"+query+": "+e);
		else printSingleErrorMessage("syntax error in query: \n"+query+": "+e);
		return;
	    }

	      //    plan.dump();
	    try {
		plan.writeDot(plan.makeDot(), new FileWriter("QEP.dot"));
	    } catch (Exception e) { e.printStackTrace(); } 
	    
	    plan.eval();
	    AbstractOperator op = plan.getOperator();
	    
	    
	    Table table; Tuple tp; Header header;

	    Vector v = op.getResult();
	    table = getTable(v);
	    header = table.getHeader();

	    if(!single)
		sender.println(RESULT);
	    sender.println(header);
	    sender.flush();
	    for (int i=0; i < table.size(); i++) {
		tp = table.getTuple(i);
		sender.println(tp);
		sender.flush();
	    }

	    if(!single)
		sender.println(END_RESULT);
    	    sender.flush();
	    return;
		    
	} catch (Throwable e) {
	    printErrorMessage("can't evaluate query: "+e);
	}
    }

    private void reload() {
    }
    private void addURL(String url) {
	try {
	    IndexMgr.idxmgr.index(new URL(url));
	    //	    sender.println("done");
	} catch (Exception e) {
	    e.printStackTrace();
	}
    }
    private void flushIndex() {
	try {
	    IndexMgr.idxmgr.flush();
	    //	    sender.println("done");
	} catch (Exception e) {
	    e.printStackTrace();
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
}

