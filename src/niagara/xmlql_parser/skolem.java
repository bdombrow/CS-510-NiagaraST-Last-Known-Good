
/**********************************************************************
  $Id: skolem.java,v 1.1 2003/12/24 01:19:53 vpapad Exp $


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
 * This class is used to represent the skolem function in the start tag.
 *
 */
package niagara.xmlql_parser;

import java.util.*;

public class skolem {
	private String name;     // name of the skolem function
	private Vector varList;  // list of variables or arguments

	/**
	 * Constructor
	 *
	 * @param name of the skolem function
	 * @param list of arguments
	 */

	public skolem(String n, Vector vl) {
		name = n;
		varList = vl;
	}

	/**
	 * @return get the name of the function
	 */

        public String getName() {
		return name;
	}

	/**
	 * @return the list of variables/arguments
	 */

	public Vector getVarList() {
		return varList;
	}

	/**
	 * replace the variables with their corresponding schemaAttributes.
	 *
	 * @param the variable table that maps variable to their schemaAttribute
	 */

	public void replaceVar(varTbl vt) {
		schemaAttribute sa;		
		for(int i=0;i<varList.size();i++) {
			sa = vt.lookUp((String)varList.elementAt(i));
			varList.setElementAt(sa,i);
		}
	}

	/**
	 * prints to the standard output
	 */

	public void dump() {
		System.out.println("skolem:");
		System.out.println(name);
		for(int i=0; i<varList.size(); i++) {
			Object obj = varList.elementAt(i);
			if(obj instanceof String)
				System.out.println("\t" + (String)varList.elementAt(i));
			else
				((schemaAttribute)obj).dump(0);
		}
	}

    public boolean equals(Object obj) {
	if(obj == null || !(obj instanceof skolem))
	    return false;
	skolem other = (skolem)obj;
	return name.equals(other.name) &&
	    varList.equals(other.varList);
    }
}
