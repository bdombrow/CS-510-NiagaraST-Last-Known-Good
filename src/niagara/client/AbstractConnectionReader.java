/**********************************************************************
  $Id: AbstractConnectionReader.java,v 1.4 2002/10/29 02:01:29 vpapad Exp $


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

public abstract class AbstractConnectionReader implements Runnable
{
	// constants
	public static final String REQUEST = "request";

	// member variables
	/**
	 * the stream from which to read
	 */
	protected Reader cReader;
    /**
	 * the stream to which you write
	 */
	protected Writer cWriter;
	/**
	 * The socket of the connection
	 */
	protected Socket socket;

	/**
	 * This is the queryRegistry of the connection reader
	 */
	protected QueryRegistry queryRegistry;

        private boolean initialized;
        	
	/**
	 * Constructor: This creates a connection to the server side
	 * @param hostame Server host name
	 * @param port Server port
	 * @param 
	 */
	public AbstractConnectionReader(String hostname, int port, UIDriverIF ui, DTDCache dtdCache)
		{
			// Set up the connection
			try{
				socket = new Socket(hostname, port);
				
				cReader = new InputStreamReader(socket.getInputStream());
				cWriter = new OutputStreamWriter(socket.getOutputStream());
			}
                        catch (ConnectException ce) {
                            ui.errorMessage(-1, "Could not connect to " + hostname + ":" + port);
                            return;
                        }
			catch(UnknownHostException e){
				ui.errorMessage(-1, "Unknown host");
                            return;
			}
			catch(IOException e){
				e.printStackTrace();
                                return;
			}

			initializeConnectionReader(ui, dtdCache);
                    initialized = true;
		}
	
	/**
	 * Constructor: This creates a Reader that reads from any reader
	 * even from a file and is used for debugging purposes. Also it 
	 * needs a writer to return to threads requesting a channel to 
	 * talk to the server
	 * @param reader Some reader
	 * @param writer Some writer
	 */
	public AbstractConnectionReader(Reader reader, Writer writer, UIDriverIF ui, DTDCache dtdCache)
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
     * The thread's run method 
     */
    abstract public void run();
	
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
        
        private UIDriverIF ui;
        public UIDriverIF getUI() {
            return ui;
        }

	/**
	 * Common constructor initializations
	 */
	private void initializeConnectionReader(UIDriverIF ui, DTDCache dtdCache)
		{
                        this.ui = ui;
                        
			// Construct a registry
			queryRegistry = new QueryRegistry();
			try{
				// send initial header to server
				startRequestSession();
			}
			catch(IOException e){
				e.printStackTrace();
			}
		}
        
        public boolean isInitialized() {
            return initialized;
        }
}











