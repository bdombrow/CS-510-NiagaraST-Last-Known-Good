/**********************************************************************
  $Id: NiagraServer.java,v 1.30 2003/12/24 02:16:38 vpapad Exp $


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

import java.net.InetAddress;

import niagara.ndom.DOMFactory;
import niagara.ndom.saxdom.BufferManager;
import niagara.query_engine.QueryEngine;

/**The main Niagra Server which receives all the client requests
   It has an instance of query engine and a SEClient for 
   contacting the SE Server
*/
public class NiagraServer {

    // All constants defined here
    private static int NUM_QUERY_THREADS;
    private static int NUM_OP_THREADS;

    // SAXDOM
    private static final int SAXDOM_DEFAULT_NUMBER_OF_PAGES = 1024;
    private static final int SAXDOM_DEFAULT_PAGE_SIZE = 1024;
    private static boolean useSAXDOM = true;
    private static int saxdom_pages = SAXDOM_DEFAULT_NUMBER_OF_PAGES;
    private static int saxdom_page_size = SAXDOM_DEFAULT_PAGE_SIZE;

    // Defaults
    private static int DEFAULT_QUERY_THREADS = 3;
    private static int DEFAULT_OPERATOR_THREADS = 30;

    // The port for client communication 
    private static int client_port = 9020;

    // The port for server-to-server communication
    protected static int server_port = 8020;

    // For executing QE queries
    QueryEngine qe;
    // for managing all the client connections
    ConnectionManager connectionManager;
    // Client for contacting the search engine
    //SEClient seClient;

    private static boolean startConsole = false;

    // Catalog
    private static String catalogFileName = "catalog.xml";
    private static Catalog catalog = null;

    public static boolean KT_PERFORMANCE = true;
    public static boolean RUNNING_NIPROF = false;
    public static boolean TIME_OPERATORS = false;

    public NiagraServer() {
        try {
            // Read the catalog
            catalog = new Catalog(catalogFileName);

            // Create the query engine
            qe = new QueryEngine(this, NUM_QUERY_THREADS, NUM_OP_THREADS);

            // Create and start the connection manager
            connectionManager = new ConnectionManager(client_port, this);

            if (startConsole) {
                Console console = new Console(this, System.in);
                console.start();
            }
            if (useSAXDOM)
                BufferManager.createBufferManager(
                    saxdom_pages,
                    saxdom_page_size);
        } catch (ConfigurationError ce) {
            System.err.println(ce.getMessage());
            System.exit(-1);
        }
    }

    public static void main(String[] args) {
        // Initialize the QueryEngine parameters
        init(args);
        if (TIME_OPERATORS | RUNNING_NIPROF) {
            System.loadLibrary("profni");
        }
        new NiagraServer();
    }

    public static void init(String[] args) {
        // Print usage/help info if -help flag is given
        if (args.length == 1 && args[0].equals("-help")) {
            usage();
            return;
        }

        NUM_QUERY_THREADS = DEFAULT_QUERY_THREADS;
        NUM_OP_THREADS = DEFAULT_OPERATOR_THREADS;

        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-quiet")) {
                ResultTransmitter.QUIET = true;
            } else if (args[i].equals("-full-tuple")) {
                ResultTransmitter.OUTPUT_FULL_TUPLE = true;
            } else if (args[i].equals("-console")) {
                startConsole = true;
            } else if (args[i].equals("-client-port")) {
                if ((i + 1) >= args.length) {
                    cerr("Please supply a parameter to -client-port");
                    usage();
                } else {
                    try {
                        client_port = Integer.parseInt(args[i + 1]);
                    } catch (NumberFormatException nfe) {
                        cerr("Invalid argument to -client-port");
                        usage();
                    }
                }
                i++; // Cover for argument
            } else if (args[i].equals("-server-port")) {
                if ((i + 1) >= args.length) {
                    cerr("Please supply a parameter to -server-port");
                    usage();
                } else {
                    try {
                        server_port = Integer.parseInt(args[i + 1]);
                    } catch (NumberFormatException nfe) {
                        cerr("Invalid argument to -server-port");
                        usage();
                    }
                }
                i++; // Cover for argument
            } else if (args[i].equals("-catalog")) {
                if ((i + 1) >= args.length) {
                    cerr("Please supply a parameter to -catalog");
                    usage();
                } else {
                    catalogFileName = args[i + 1];
                }
                i++; // Cover for argument
            } else if (args[i].equals("-dom")) {
                if ((i + 1) >= args.length) {
                    cerr("Please supply a parameter to -dom");
                    usage();
                } else {
                    DOMFactory.setImpl(args[i + 1]);
                }
                i++; // Cover for argument
            } else if (args[i].equals("-no-saxdom")) {
                useSAXDOM = false;
            } else if (args[i].equals("-saxdom-pages")) {
                saxdom_pages = parseIntArgument(args, i);
                i++; // Cover for argument
            } else if (args[i].equals("-saxdom-page-size")) {
                saxdom_page_size = parseIntArgument(args, i);
                i++; // Cover for argument
            } else if (args[i].equals("-run-niprof")) {
                RUNNING_NIPROF = true;
            } else if (args[i].equals("-time-operators")) {
                TIME_OPERATORS = true;
            } else {
                cerr("Unknown option: " + args[i]);
                usage();
            }
        }
    }

    private static int parseIntArgument(String args[], int pos) {
        if ((pos + 1) >= args.length) {
            cerr("Please supply a parameter to " + args[pos]);
            usage();
        } else {
            try {
                return Integer.parseInt(args[pos + 1]);
            } catch (NumberFormatException nfe) {
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
        cout("\t-console      Rudimentary control of the server from stdin");
        cout("\t-client-port <number> Port number for client-server communication");
        cout("\t-server-port <number> Port number for inter-server communication");
        cout(
            "\t-catalog <file> alternate catalog file (default is:"
                + catalogFileName
                + ")");
        cout("\t-dom  <implementation name> Default DOM implementation.");
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
    public static String getLocation() {
        String ret = "";
        try {
            ret =
                InetAddress.getLocalHost().getHostAddress() + ":" + server_port;
        } catch (java.net.UnknownHostException e) {
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
        info("Server is shutting down.");
        connectionManager.shutdown();
        qe.shutdown();
        catalog.shutdown();
        System.exit(0);
    }

    /** Simple method for info messages - just outputs to stdout now
     * we can extend it later to do something fancier */
    public static void info(String msg) {
        cout(msg);
    }
}
