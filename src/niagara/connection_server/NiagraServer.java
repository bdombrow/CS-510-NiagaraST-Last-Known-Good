
/**********************************************************************
  $Id: NiagraServer.java,v 1.5 2000/08/11 22:29:27 vpapad Exp $


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

	// Defaults
	private static int DEFAULT_QUERY_THREADS = 10;
	private static int DEFAULT_OPERATOR_THREADS = 50;
    
    // For executing QE queries
    QueryEngine qe;
    // For executing trigger manager queries
    TriggerManager triggerManager;
    // for managing all the client connections
    ConnectionManager connectionManager;
    // Client for contacting the search engine
    SEClient seClient;

    // The port that all client communication is done on
    //
    private static int CONNECTION_PORT = 9020;
    

    private static boolean dtd_hack = false;

    public NiagraServer() {
		try {
			// Create the query engine
			//
			qe = new QueryEngine(NUM_QUERY_THREADS,
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
				new ConnectionManager (CONNECTION_PORT, this, dtd_hack);
	    
			if (connectToSE)
			    seClient = new SEClient(SEHOST);
        
		}
		catch (Exception ex) {
			System.err.println("Could not start server becuase of ");
			ex.printStackTrace();
		}
    }
    
    
    /** For reading the options from a configuration file and in case the file
		is not there or the command line argument is -init then takes the options
		from the user and stores them in the configuration file
    */
    public static void main(String[] args)
		{

			// Initialize the QueryEngine parameters
			//
			init(args);
	
			new NiagraServer();
		}
    
    
    public static void init(String[] args)
		{
	
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
				usage(1);
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
				}
				else if (args[i].equals("-dtd-hack")) {
				    dtd_hack = true;
				    valid_args = true;
				}
				else if (args[i].equals("-console")) {
				    Console console = new Console(System.in);
				    console.start();
				    valid_args = true;
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

				}catch(Exception e){
		
				}
				return;
			}
	
			System.out.println("\nNo confing file, start server with '-init' flag");
			usage(0);
			System.exit(-1);
		}
    
    /**
     * see if int is valid
     */
    private static boolean isInteger(String intValue)
		{

			try{

				// Convert String to int
				//
				int tmpInt = Integer.parseInt(intValue);
				return true;
			}catch(Exception e){
				System.err.println("* Invalid integer value *");
				return false;
			}
		}

    /**
     *  see if host is valid
     */
    private static boolean hostIsValid(String host)
		{
			try{

				// Try to lookup the host
				//
				InetAddress ip = InetAddress.getByName(host);
				return true;
			}catch(Exception e){
				System.err.println("* Host '"+host+"' not found *");
				return false;
			}
		}

    /**
     * Print help and usage information
     */
    private static void usage(int detail)
		{
			if(detail <= 1){
				System.out.println();
				System.out.println("Usage: java niagara.connection_server.NiagraServer [flags]");
				System.out.println("\t-init   create (re-create) the .niagra_config file");
				System.out.println("\t-se <host name> use the the search engine on <host name>");
				System.out.println("\t-without-se Do not try to connect to a search engine");
				System.out.println("\t-dtd-hack   Add HTML entity definitions to results");
				System.out.println("\t-console    Rudimentary control of the server from stdin");

				System.out.println("\t-help   print detailed help screen");
			}
			if(detail == 1){
				System.out.println("\n\tNiagara help message goes here");
			}
			System.out.println();
			System.exit(-1);
		}
}



