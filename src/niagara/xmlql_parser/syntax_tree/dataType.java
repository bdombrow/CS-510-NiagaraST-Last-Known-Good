
/**********************************************************************
  $Id: dataType.java,v 1.1 2000/05/30 21:03:30 tufte Exp $


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
 * This class declares the diffrent type a data object can be
 *
 */
package niagara.xmlql_parser.syntax_tree;

public class dataType {
	public static final int IDEN = 1;    // for the time being its same
	public static final int STRING = 1;  // may have to be changed later

	public static final int VAR = 3;     // variables
	public static final int NUMBER = 4;  // not used now
	public static final int ATTR = 5;    // for schemaAttribute
	
	public static final int TAGVAR = 10; // Tag Variables
	public static final int ELEMENT_AS = 11; // if the variable is for 
						 // element_as
	public static final int CONTENT_AS = 12; // if the variable is for
						 // content_as
}

