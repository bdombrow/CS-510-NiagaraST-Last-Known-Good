
/**********************************************************************
  $Id: Client.java,v 1.1 2000/05/30 21:03:28 tufte Exp $


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

/** 
 * Continuous session Search Engine query interface.
 * Port number is binded in the compile time and it is available
 * in the Const.java class.
 * 
 * Uses simple command-line protocol.
 *
 *
 */

public class Client implements Const {
  public static final String PROMPT = "> ";
  public static final String ACTION_COMMAND = "go";
  public static final String CLEAR_COMMAND = "clear";
  public static final String QUIT_COMMAND = "exit";
  public static final String HELP_COMMAND = "help";
  public static final String RELOAD_COMMAND = "reload";

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
      Client client = new Client(args[0]);
      client.startClient();
    } catch (UnknownHostException e) {
      System.err.println("ERROR: unknown host: "+e);
    } catch (IOException e) {
      System.err.println("ERROR: failed I/O: "+e);
    }
  }
  
  public Client(String serverName) {
    this.serverName=serverName;
  }

  protected void contactServer() throws UnknownHostException,IOException {
    mySocket = new Socket(serverName, SERVER_PORT);
    receiver=new BufferedReader(new InputStreamReader(mySocket.getInputStream()));
    sender=new PrintWriter(new OutputStreamWriter(mySocket.getOutputStream()));
  }

  public void startClient() throws IOException {
    contactServer();
    System.out.println();
    System.out.println("Connected to XML Search Engine at "+serverName);
    System.out.println();
    BufferedReader in = new BufferedReader(new InputStreamReader(System.in));

    System.out.print(PROMPT);
    String line,query="";
    
    while((line=in.readLine())!=null) {
      line=line.trim();
      if (line.equals(ACTION_COMMAND)) {
	sender.println(query);
	sender.flush();
	printResult();
	query="";
      } else if (line.equals(CLEAR_COMMAND)) {
	query="";
      } else if (line.equals(HELP_COMMAND)) {
	printHelpMessage();
      } else if (line.equals(RELOAD_COMMAND)) {
	sender.println(RELOAD);
	sender.flush();
      } else if (line.equals(QUIT_COMMAND)) {
	sender.println(QUIT);
	sender.flush();
	closeConnection();
	return;
      } else {
	query+=line+" ";
      }
      System.out.print(PROMPT);
    }
  }

  protected void printHelpMessage() {
    System.out.println("** A request to server starts with one of the following query protocols and ends with '"+ACTION_COMMAND+"' command. Command-line commands are processed sorely in the client. All protocols and command-line commands start at the beginning of a new line.");
    System.out.println();
    System.out.println("EXAMPLE:");
    System.out.println("> QUERY");
    System.out.println("> title contains (java AND programming)");
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

  protected void printResult() {
    try {
      String line=receiver.readLine();
      while((line=receiver.readLine())!=null) {
	if (line.equals(END_RESULT)) break;
	System.out.println(line);
      }
      if (line==null) System.out.println("ERROR: server connection broken");
      System.out.flush();
    } catch (IOException e) {
      System.out.println("ERROR: failed I/O");
    }
  }

  protected void closeConnection() throws IOException {
    if (sender!=null) sender.close();
    if (receiver!=null) receiver.close();
    if (mySocket!=null) mySocket.close();
  }
}
 

    
    
