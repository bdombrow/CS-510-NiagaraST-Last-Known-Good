
/**********************************************************************
  $Id: ConstructIndex.java,v 1.1 2000/05/30 21:03:28 tufte Exp $


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


//ConstructIndex.java
package niagara.search_engine.server;
/**
 * construct index for files whose urls are contained in a file
 */

import java.io.*;
import java.util.*;
import java.net.*;

import niagara.search_engine.indexmgr.*;

//construction of yellow pages from a file containing a list of  urls
public class ConstructIndex
{
    public ConstructIndex(Vector docUrls)
    {
	if (docUrls == null) {
	    System.out.print("Null list of urls");	    
	    return;	
	}
	
	try {	    
	    for (int i=0;i<docUrls.size();i++){
		IndexMgr.idxmgr.index
		    (new URL((String)(docUrls.elementAt(i))));
	    }
	 
	    //flush to disk 
	    IndexMgr.idxmgr.flush();	    
	}
	catch (IMException e) {
	    System.err.println("Error in indexing");
	}
	
        catch (MalformedURLException e) {
	    System.err.println("Bad Url");
	} 	
    }
           
    public static void main(String[] args)
    {
	String fileName;
	
	if (args.length <1) {
	    System.err.println("USAGE: ConstructIndex <url-list file>");
	    System.exit(0);
	}

	fileName = args[0];

	Vector newUrls = new Vector();
	
	try {
	    BufferedReader reader = new BufferedReader(new FileReader
						       (fileName));
	    String line;
	    
	    for (;;) {
		line = reader.readLine();
		
		if (line == null) {
		    //the end of the file is reached
		    reader.close();
		    break;
		} else {
		    line = line.trim();
		    if (line.equals("") || line.startsWith("#"))
			continue;
		}
		newUrls.addElement(line);
	    }
	} // end of try
	
	catch (Exception e) {
	    System.err.println("Error in reading file of new urls ");
	    e.printStackTrace();
	}
	
	ConstructIndex ci = new ConstructIndex(newUrls);
    }
    
}



