
/**********************************************************************
  $Id: ConnectionReader.java,v 1.1 2000/05/30 21:03:24 tufte Exp $


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


package niagara.client;
import com.microstar.xml.XmlParser;
import java.net.*;
import java.io.*;

/**
 * This class establishes a connection with the server.
 * Then calls the parse method of the responseHandler 
 * to read the session document.
 * Each session with the server generates a separate 
 * document which conforms to response.dtd. Inisde this document
 * all the client queries are serviced
 *
 */

class ConnectionReader implements Runnable
{
	// constants
	public static final String REQUEST = "request";

	// member variables
	/**
	 * the stream from which to read
	 */
	private Reader cReader;
    /**
	 * the stream to which you write
	 */
	private Writer cWriter;
	/**
	 * The socket of the connection
	 */
	private Socket socket;
	/**
	 * This object handles callbacks for the session document.
	 */
	private ResponseHandler responseHandler;
	/**
	 * This is the parser
	 */
	private XmlParser parser = new XmlParser();
	/**
	 * This is the queryRegistry of the connection reader
	 */
	private QueryRegistry queryRegistry;
	
	// Constructors
	/**
	 * Constructor: This creates a connection to the server side
	 * @param hostame Server host name
	 * @param port Server port
	 * @param 
	 */
	public ConnectionReader(String hostname, int port, UIDriverIF ui, DTDCache dtdCache)
		{
			// Set up the connection
			try{
				socket = new Socket(hostname, port);
				
				cReader = new InputStreamReader(socket.getInputStream());
				cWriter = new OutputStreamWriter(socket.getOutputStream());
			}
			catch(UnknownHostException e){
				System.err.println("Unknown host");
			}
			catch(IOException e){
				e.printStackTrace();
			}

			initializeConnectionReader(ui, dtdCache);
		}
	
	/**
	 * Constructor: This creates a Reader that reads from any reader
	 * even from a file and is used for debugging purposes. Also it 
	 * needs a writer to return to threads requesting a channel to 
	 * talk to the server
	 * @param reader Some reader
	 * @param writer Some writer
	 */
	public ConnectionReader(Reader reader, Writer writer, UIDriverIF ui, DTDCache dtdCache)
		{
			// initialize the input streams
			cReader = reader;
			cWriter = writer;
			initializeConnectionReader(ui, dtdCache);
		}

	// Public MemberFunctions
	/**
	 * @return a Writer object for writing information to the server
	 */
	public Writer getWriter()
		{
			return cWriter;
		}
	
    /**
	 * @return a PrintWriter object for writing formatted information to the server
	 */
	public PrintWriter getPrintWriter()
		{
			return new PrintWriter(cWriter, true);
		}	
	
	/**
	 * @return a refernce to the this objects QueryRegistry
	 */
	public QueryRegistry getQueryRegistry()
		{
			return queryRegistry;
		}

	/**
	 * The run method that invokes the parser
	 */
	public void run()
		{
			// Read the connection and throw the callbacks
			try{
				System.err.println("Parsing started");
				parser.parse(null, null, cReader);
				System.err.println("Parsing finished. Client Exiting.");
			}
			catch(EOFException e){
				System.err.println("Server side ended the session");
				try{
					socket.close();
				}
				catch(IOException ee){
					ee.printStackTrace();
				}
				return;
			}
			catch(SocketException e){
				System.err.println("Parser Closed down the socket");
				return;
			}
			catch(Exception e){
				e.printStackTrace();
				// Parser exceptions will be caught and handled here
			}
			System.exit(0);
		}
	
    /**
	 * End the request session upon Client shutdown
	 */
	public void endRequestSession() throws IOException
		{
			// end the session by writing an end element
			cWriter.write("</" + REQUEST +">");
			cWriter.flush();
		}

	// PRIVATE FUNCTIONS
	/**
	 * Start the request session. This writes the following to the 
	 * out put stream:
	 *<code>
	 * <?xml version="1.0"?>
	 * <!DOCTYPE request ...
     * <request>
	 * ...
	 *</code>
	 */
	private void startRequestSession() throws IOException
		{
			// This function initializes the session document
			String s = "<"+ REQUEST +">\n";
			cWriter.write(s);
		}

	/**
	 * Common constructor initializations
	 */
	private void initializeConnectionReader(UIDriverIF ui, DTDCache dtdCache)
		{
			// Construct a registry
			queryRegistry = new QueryRegistry();
			try{
				// send initial header to server
				startRequestSession();
			}
			catch(IOException e){
				e.printStackTrace();
			}
			// create a response handler and pass to it the registry
			responseHandler = new ResponseHandler(queryRegistry, ui, dtdCache);
			parser.setHandler(responseHandler);
		}

	// Entry Point (debug main)
	public static void main(String argv[]) throws Exception
		{
			
		}	
}











