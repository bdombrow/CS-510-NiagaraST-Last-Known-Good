
/**********************************************************************
  $Id: EntitySupplement.java,v 1.1 2000/05/30 21:03:26 tufte Exp $


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

import java.io.*;
import java.util.*;


/**
 * Graceful hack to incorporate entities found in the results
 */

class EntitySupplement
{
	// Inserts missing entities in the result stream
	// Those entities are found in a specifice file
	public static String returnEntityDefs(String file)
		{
			Vector v = new Vector();
			v.add(file);
			return returnEntityDefs(v);
		}
	
	// Inserts entities from multiple files in the result stream
	public static String returnEntityDefs(Vector files)
		{
			StringBuffer res = new StringBuffer();
			
			res.append("<!DOCTYPE response [\n");

			for(int i = 0; i < files.size(); i++){
				String fileName = (String)(files.elementAt(i));
				
				try{
					BufferedReader r = new BufferedReader(new FileReader(fileName));

					String line = r.readLine();
					while(line != null){
						res.append(line + "\n");
						line = r.readLine();
					}
					
					
				}
				catch(IOException e){
					System.err.println("File " + fileName + " not found.");
				}				
			}
			

			res.append("]>\n");
				
			return res.toString();
		}

	public static void main(String args[])
		{
			System.out.println(returnEntityDefs("CarSet.cfg"));
		}
}

