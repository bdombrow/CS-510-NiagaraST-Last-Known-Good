
/**********************************************************************
  $Id: stp.java,v 1.1 2000/05/30 21:03:30 tufte Exp $


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
 * This class is used in the parser to pass information from one 
 * production rule to another.
 *
 */
package niagara.xmlql_parser.syntax_tree;

import java.util.*;
import java.io.*;

public class stp {

    regExp regularExp;    // regular expression
    Vector attrList;      // list of attribute-value pair

    /**
     * Constructor
     *
     * @param regular expression 
     * @param list of attribute-value pair
     */

    public stp (regExp regularExp, Vector attrList) {
	
	this.regularExp = regularExp;
	this.attrList = attrList;
    }

    /**
     * @return regular expression
     */

    public regExp getRegExp() {
	return regularExp;
    }

    /**
     * @return list of attribute-value (attr) pair
     */

    public Vector getAttrList() {
	return attrList;
    }

    /**
     * prints to the standard output
     */

    public void dump() {
	System.out.println("stp:");
	if(regularExp instanceof regExpOpNode)
		((regExpOpNode)regularExp).dump(0);
	else
		((regExpDataNode)regularExp).dump(0);
	for(int i= 0; i<attrList.size(); i++)
		((attr)attrList.elementAt(i)).dump(0);
    }
}
