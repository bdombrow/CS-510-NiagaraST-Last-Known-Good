
/**********************************************************************
  $Id: regExpDataNode.java,v 1.3 2002/03/26 23:54:13 tufte Exp $


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


/**
 *
 * This class is used to represent the leaf node (that contains string, 
 * variables, etc.)
 *
 *
 */
package niagara.xmlql_parser.syntax_tree;

import java.io.*;

import org.w3c.dom.*;

public class regExpDataNode extends regExp {
	
	private data expData;  // leaf data (could be string, variable, etc.)

	/**
	 * Constructor
	 *
	 * @param data that represents the leaf of regular expression tree
	 */

	public regExpDataNode(data d) {
		expData = d;
	}

	/**
	 * @return the leaf data
	 */

	public data getData() {
		return expData;
	}

	/**
	 * print the data to the standard output
	 *
	 * @param number of tabs at the beginning of each line
	 */

	public void dump(int depth) {
		expData.dump(depth);
	}

	//trigger --Jianjun
	public String toString() {
	     return expData.toString();
	}

    public boolean isNever() {
	if(expData.getType() == dataType.VAR) {
	    if(((String)(expData.getValue())).equals("NEVER")){
		return true;
	    }
	}
	return false;
    }
}


