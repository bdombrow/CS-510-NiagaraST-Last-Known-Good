/**********************************************************************
  $Id: NiagraServer.java,v 1.17 2002/10/24 00:11:24 vpapad Exp $


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
import java.util.*;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.net.InetAddress;
import niagara.search_engine.server.SEClient;
import niagara.trigger_engine.TriggerManager;
import niagara.query_engine.QueryEngine;
import niagara.ndom.DOMFactory;
import niagara.optimizer.Optimizer;
import niagara.utils.*;

import niagara.ndom.saxdom.BufferManager;

import java.net.InetAddress;

// For Jetty Servlet engine
import com.mortbay.HTTP.*;
import com.mortbay.Util.*;
import com.mortbay.HTTP.Handler.*;
import com.mortbay.HTTP.Handler.Servlet.*;

/**The main Niagra Server which receives all the client requests
   It has an instance of query engine and a trigger manager and a SEClient for 
   contacting the SE Server
*/
public class NiagraServer
{
    
    // All constants defined here
    private static String SEHOST;
    private static int SEPORT = niagara.search_engine.util.Const.SERVER_PORT;
    private static int NUM_QUERY_THREADS;
    private static int NUM_OP_THREADS;

    private static boolean connectToSE = true;

    // SAXDOM
    private static final int SAXDOM_DEFAULT_NUMBER_OF_PAGES = 1024;
    private static final int SAXDOM_DEFAULT_PAGE_SIZE = 1024;
    private static boolean useSAXDOM = true;
    private static int saxdom_pages = SAXDOM_DEFAULT_NUMBER_OF_PAGES;
    private static int saxdom_page_size = SAXDOM_DEFAULT_PAGE_SIZE;

    // Defaults
    private static int DEFAULT_QUERY_THREADS = 10;
    private static int DEFAULT_OPERATOR_THREADS = 50;

    // The port for client communication 
    private static int client_port = 9020;

    // The port for server-to-server communication
    protected static int server_port = 8020;
    
    // For executing QE queries
    QueryEngine qe;
    // For executing trigger manager queries
    TriggerManager triggerManager;
    // for managing all the client connections
    ConnectionManager connectionManager;
    // Client for contacting the search engine
    SEClient seClient;

    private static boolean dtd_hack = false;

    private static boolean startConsole = false;

    // Catalog
    private static String catalogFileName = "catalog.xml";
    private static Catalog catalog = null;

    public static boolean QUIET = false;

    public static boolean KT_PERFORMANCE = false;

    public NiagraServer() {
	try {
            // Create the query engine
            qe = new QueryEngine(this, NUM_QUERY_THREADS,
                                 NUM_OP_THREADS, 
                                 SEHOST, 
                                 SEPORT,
                                 true,     // Connection Manager
                                 connectToSE);    // Search Engine
	    
            // Create the trigger manager
            triggerManager = new TriggerManager(qe);
            new Thread(triggerManager,"TriggerManager").start();
	    
            // Create and start the connection manager
            connectionManager =
                new ConnectionManager (client_port, this, dtd_hack);

            if (startConsole) {
                Console console = new Console(this, System.in);
                console.start();
            }
            if (useSAXDOM)
		BufferManager.createBufferManager(saxdom_pages,
						  saxdom_page_size);

            catalog = new Catalog(catalogFileName);

            if (connectToSE)
                seClient = new SEClient(SEHOST);
	      if(!KT_PERFORMANCE) { 
		  // Start HTTP server for interserver communication
		  HttpServer hs = new HttpServer();
		  hs.addListener(new InetAddrPort(server_port));
		  HandlerContext hc = hs.addContext(null, "/servlet/*");
		  
		  ServletHandler sh = new ServletHandler();
		  ServletHolder sholder = sh.addServlet("/communication",
							"niagara.connection_server.CommunicationServlet");
		  
		  
		  hc.addHandler(sh);
		  hs.start();
		  
		  Context context = sh.getContext();
		  context.setAttribute("server", this);
	      } 
	} catch(IOException ioe) {
	    throw new UnrecoverableException("IO Error Unable to start server: " + ioe.getMessage());
	}
    }
    
    
    /** For reading the options from a configuration file and in case
		the file is not there or the command line argument is
		-init then takes the options from the user and stores
		them in the configuration file */
    public static void main(String[] args)
		{

			// Initialize the QueryEngine parameters
			//
			init(args);
	
			new NiagraServer();
		}
    
    
    public static void init(String[] args) {	
        File configInfoFile = new File(".niagra_config");
        
        // If -init flag is given, create the init file
        //
        if(args.length == 1 && args[0].equals("-init")){
	    
            byte[] input = new byte[512];
            String inputString = null;
            int inputLen = 0;	    
            boolean validInput = false;
            
            // get user input for parameters
            try {		
                
                FileOutputStream fout = new FileOutputStream(configInfoFile);
                
                // Get the yellow pages host
                //
                while(! validInput){
                    System.out.print("Enter Search Engine Server hostname: ");
                    inputLen = System.in.read (input);
                    
                    validInput = hostIsValid(new String(input, 0, inputLen-1));
                    
                    if(validInput){
                        
                        // Write to the file
                        //
                        fout.write(new String("SEHOST:").getBytes());
                        fout.write(input, 0, inputLen-1);
                        fout.write(new String("\n").getBytes());
                    }
                }
                
		
                // Get the number of query threads
                //
                validInput = false;
                while(! validInput){
                    System.out.print("Enter the number of query threads (default 10): ");
                    inputLen = System.in.read (input);
                    if(inputLen == 0){
                        System.out.println("read blank line");
                        continue;
                    }
                    
                    if (input[0] == '\n') {
                        System.out.println("Setting number of query threads to 10");
                        validInput = true;
                        input = "10".getBytes();
                        inputLen = 3;
                    }
                    else 
                        validInput = isInteger(new String(input, 0, inputLen-1));
                    
                    if(validInput){
                        // Write to the file
                        //
                        fout.write(new String("NUM_QUERY_THREADS:").getBytes());
                        fout.write(input, 0, inputLen-1);
                        fout.write(new String("\n").getBytes());
                    }
                }
                
                // Get the number of operator threads
                //
                validInput = false;
                while(! validInput){
                    System.out.print("Enter the number of operator threads (default 50): ");
                    inputLen = System.in.read (input);
                    if (input[0] == '\n') {
                        System.out.println("Setting number of operator threads to 50");
                        validInput = true;
                        input = "50".getBytes();
                        inputLen = 3;
                    }
                    else
                        validInput = isInteger(new String(input, 0, inputLen-1));
                    
                    if(validInput){
                        // Write to the file
                        //
                        fout.write(new String("NUM_OP_THREADS:").getBytes());
                        fout.write(input, 0, inputLen-1);
                        fout.write(new String("\n").getBytes());
                    }
                }
                
                fout.flush();
                fout.close();
            }
            catch (IOException e) {
                e.printStackTrace();
                System.exit(-1);
            }
        }
        
        // Print usage/help info if -help flag is given
        //
        else if(args.length == 1 && args[0].equals("-help")){
            usage();
            return;
        }
        // Bypass the config file and specify host directly
        else if(args.length == 2 && args[0].equals("-se")){
				// bypass config. Config cannot be read in NT
            String host = args[1];
            SEHOST = host;
            NUM_QUERY_THREADS = DEFAULT_QUERY_THREADS; // hard wired defaults
            NUM_OP_THREADS = DEFAULT_OPERATOR_THREADS; // hard wired defaults
            return;
        }
        else {
            boolean valid_args = false;
            for (int i=0; i < args.length; i++) {
                if (args[i].equals("-without-se")) {
                    connectToSE = false;
                    NUM_QUERY_THREADS = DEFAULT_QUERY_THREADS; // hard wired defaults
                    NUM_OP_THREADS = DEFAULT_OPERATOR_THREADS; // hard wired defaults
                    valid_args = true;
                } else if (args[i].equals("-quiet")) {
		    QUIET = true;
		} else if (args[i].equals("-dtd-hack")) {
                    dtd_hack = true;
                    valid_args = true;
                } else if (args[i].equals("-console")) {
                    startConsole = true;
                    valid_args = true;
                } else if (args[i].equals("-client-port")) {
                    if ((i+1) >= args.length) {
                        cerr("Please supply a parameter to -client-port");
                        usage();
                    } else {
                        try {
                            client_port = Integer.parseInt(args[i+1]);
                        }
                        catch (NumberFormatException nfe) {
                            cerr("Invalid argument to -client-port");
                            usage();
                        }
                    }
                    i++; // Cover for argument
                    valid_args = true;
                } else if (args[i].equals("-server-port")) {
                    if ((i+1) >= args.length) {
                        cerr("Please supply a parameter to -server-port");
                        usage();
                    }
                    else {
                        try {
                            server_port = Integer.parseInt(args[i+1]);
                        }
                        catch (NumberFormatException nfe) {
                            cerr("Invalid argument to -server-port");
                            usage();
                        }
                    }
                    i++; // Cover for argument
                    valid_args = true;
                } else if (args[i].equals("-catalog")) {
                    if ((i+1) >= args.length) {
                        cerr("Please supply a parameter to -catalog");
                        usage();
                    }
                    else {
                        catalogFileName = args[i+1];
                    }
                    i++; // Cover for argument
                    valid_args = true;
                } else if (args[i].equals("-dom")) {
                    if ((i+1) >= args.length) {
                        cerr("Please supply a parameter to -dom");
                        usage();
                    }
                    else {
                        DOMFactory.setImpl(args[i+1]);
                    }
                    i++; // Cover for argument
                    valid_args = true;
                } else if (args[i].equals("-saxdom")) {
                    useSAXDOM = true;
                    valid_args = true;
                } else if (args[i].equals("-optimizer")) {
                    Optimizer.init();
                    valid_args = true;
                } else if (args[i].equals("-saxdom-pages")) {
		    saxdom_pages = parseIntArgument(args, i);
                    i++; // Cover for argument
                    valid_args = true;
                } else if (args[i].equals("-saxdom-page-size")) {
		    saxdom_page_size = parseIntArgument(args, i);
                    i++; // Cover for argument
                    valid_args = true;
                } else {
                    cerr("Unknown option: " + args[i]);
                    usage();
                }
            }
            if (valid_args) return;
        }
        
        // Open the config file if it exists and use it to init params
        //
        if(configInfoFile.exists()){
            
	    try{
                FileInputStream fin = new FileInputStream(configInfoFile);
                byte[] inbuff = new byte[512];
                String curParam = null;
                
                // Read the config file into the inbuff
                //
                int len = fin.read(inbuff, 0, 512);
                
                String inStr = new String(inbuff, 0, len);
                StringTokenizer st = 
                    new StringTokenizer(inStr, "\n");
                
                // Get the yellow pages host
                //
                curParam = st.nextToken();
                curParam = curParam.substring(curParam.indexOf(":")+1);
                SEHOST = curParam;
                
                // Get the yellow pages host
                //
                curParam = st.nextToken();
                curParam = curParam.substring(curParam.indexOf(":")+1);
                NUM_QUERY_THREADS = Integer.parseInt(curParam);
                
                
                // Get the yellow pages host
                //
                curParam = st.nextToken();
                curParam = curParam.substring(curParam.indexOf(":")+1);
                NUM_OP_THREADS = Integer.parseInt(curParam);
                
	    }catch(java.io.FileNotFoundException e){
		throw new UnrecoverableException("Unable to retrieve config file " + e.getMessage());
	    } catch (IOException ioe) {
		throw new UnrecoverableException("Unable to read config file " 
						 + ioe.getMessage());
	    }
            return;
        }
        
        cout("\nNo confing file, start server with '-init' flag");
        usage();
    }
    
    /**
     * see if int is valid
     */
    private static boolean isInteger(String intValue) {
	int tmpInt = Integer.parseInt(intValue);
	return true;
    }

    /**
     *  see if host is valid
     */
    private static boolean hostIsValid(String host)
    {
	try{
	    // Try to lookup the host
	    InetAddress ip = InetAddress.getByName(host);
	    return true;
	}catch(java.net.UnknownHostException e){
	    cerr("* Host '"+host+"' not found *");
	    return false;
	}
    }

    private static int parseIntArgument(String args[], int pos) {
	if ((pos+1) >= args.length) {
	    cerr("Please supply a parameter to " + args[pos]);
	    usage();
	} else {
	    try {
		return Integer.parseInt(args[pos+1]);
	    }
	    catch (NumberFormatException nfe) {
		cerr("Invalid argument to " + args[pos]);
		usage();
	    }
	}
	
	System.exit(-1);
        return -1; /* Unreachable */
    }

    /**
     * Print help and usage information
     */
    private static void usage() {
        cout("");
        cout("Usage: java niagara.connection_server.NiagraServer [flags]");
        cout("\t-init   create (re-create) the .niagra_config file");
        cout("\t-se <host name> use the the search engine on <host name>");

        cout("\t-without-se   Do not try to connect to a search engine");
        cout("\t-dtd-hack     Add HTML entity definitions to results");
        cout("\t-console      Rudimentary control of the server from stdin");
        cout("\t-client-port <number> Port number for client-server communication");
        cout("\t-server-port <number> Port number for inter-server communication");
        cout("\t-catalog <file> Specify catalog file (default is:" 
             + catalogFileName +")");
        cout("\t-dom  <implementation name> Default DOM implementation.");
        cout("\t-optimizer  Use the Columbia optimizer.");
        cout("\t-saxdom  Use SAXDOM for input documents.");
	cout("\t-saxdom-pages <number> Number of SAXDOM pages.");
	cout("\t-saxdom-page-size <number> Size of each SAXDOM page.");
        cout("\t-help   print this help screen");
        System.exit(-1);
    }

    private static void cout(String msg) {
        System.out.println(msg);
    }

    private static void cerr(String msg) {
        System.err.println(msg);
    }


    /**
     * <code>getLocation</code> 
     * 
     * @return the location <code>String</code> for this server
     */
    public String getLocation() {
        String ret = "";
        try {
            ret = InetAddress.getLocalHost().getHostAddress() + ":" + server_port;
        }
        catch (java.net.UnknownHostException e) {
            cerr("Server host name is unknown! -- aborting...");
            System.exit(-1);
        }

        return ret;
    }

    public static boolean usingSAXDOM() {
        return useSAXDOM;
    }

    public static Catalog getCatalog() {
        return catalog;
    }

    public QueryEngine getQueryEngine() {
        return qe;
    }

    // Try to shutdown
    public void shutdown() {
        connectionManager.shutdown();
    }

    /** Simple method for info messages - just outputs to stdout now
     * we can extend it later to do something fancier */
    public static void info(String msg) {
	cout(msg);
    }
}



