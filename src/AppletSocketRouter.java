
/**********************************************************************
  $Id: AppletSocketRouter.java,v 1.1 2000/05/30 21:03:24 tufte Exp $


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


import java.net.*;
import java.io.*;
/**
 * this is used to route the applets socket connection
 *
 * Quick hack to route from the WWW Server to the actual Niagara Server
 */

public class AppletSocketRouter
{
	
	private static int PORT = 9020;

	private BufferedReader fromServer;
	private PrintWriter toServer;
	
	private BufferedReader fromClient;
	private PrintWriter toClient;

	private static final String traceFile = "/p/niagara/web/trace";
	PrintWriter tr;

	private class Pipe extends Thread
	{
		BufferedReader from;
		PrintWriter to;
		
		Pipe(BufferedReader from, PrintWriter to)
			{
				this.from = from;
				this.to = to;
			}
		
		public void run()
			{
				try{
					while(true){
						String s = from.readLine();
						s.length();// generate a null pointer exception
						to.println(s);
					}
				}
				catch(Exception e){
					System.err.println("Broken Pipe");
					return;
				}
			}
	
	}
	
	
	public AppletSocketRouter(Socket c, Socket s) throws IOException
		{

			tr = new PrintWriter(new FileWriter(traceFile), true);
			tr.println("Router Started " + this.getClass().getName());

			fromServer = new BufferedReader(new InputStreamReader(s.getInputStream()));
			toServer = new PrintWriter(s.getOutputStream(), true);
			
			fromClient = new BufferedReader(new InputStreamReader(c.getInputStream()));
			toClient = new PrintWriter(c.getOutputStream(), true);

			new Pipe(fromServer, toClient).start();
			new Pipe(fromClient, toServer).start();
			tr.println("Threads Created Succesfully. Exiting");
		}

	public static void main(String args[])
		{
			if(args.length != 1){
				System.err.println("Usage: java AppletSocketRouter <niagara-server-url>");
				System.exit(1);
			}
			

			try{
				ServerSocket ss = new ServerSocket(PORT);
				
				// Accept a connection

				while(true){
					System.err.println("Connecting to Server");
                    Socket s = new Socket(args[0], PORT);
					System.err.println("Waiting for a Client");
                    Socket c = ss.accept();

					new AppletSocketRouter(c, s);
				}
			}
			catch(IOException e){
				
			}
		}
}


				

			
