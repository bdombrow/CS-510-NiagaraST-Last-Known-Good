
/**********************************************************************
  $Id: arg.java,v 1.1 2003/12/24 01:19:54 vpapad Exp $


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
 * This class is used to represent the arguments of a function in XML-QL.
 * As we donot support functions in XML-QL, this class is not used at this
 * time.
 *
 */
package niagara.xmlql_parser;

public class arg {
	
	String var;		// variable name
	String type;		// variable type

	/**
	 * Constructor
	 * 
	 * @param variable name
	 * @param variable type
	 */

	public arg(String var, String type) {
		this.var = var;
		this.type = type;
	}
	
	public void dump() {
		System.out.println(var + " " + type);
	}

};


