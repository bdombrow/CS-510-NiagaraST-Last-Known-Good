
/**********************************************************************
  $Id: varToAttr.java,v 1.1 2000/05/30 21:03:30 tufte Exp $


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
 * Stores all the schemaAttributes associated with a variable. This class is
 * the entered into the vector of the varTbl.
 *
 */
package niagara.xmlql_parser.syntax_tree;

import java.util.*;
import com.ibm.xml.parser.*;
import org.w3c.dom.*;

public class varToAttr {
	private String var;     // name of the variable
	private Vector attrs;   // list of schemaAttribute associated with it

        /**
	 * Constructor
	 *
	 * @param name of the variable
	 * @param schemaAttribute associated with it
	 */

	public varToAttr(String v, schemaAttribute a) {
		var = new String(v);
		attrs = new Vector();
		attrs.addElement(a);
	}

        /**
	 * Constructor
	 *
	 * @param the varToAttr to make a copy of
	 */

	public varToAttr(varToAttr v2a) {
		var = new String(v2a.var);
		attrs = new Vector();
		for(int i=0;i<v2a.attrs.size();i++)
			attrs.addElement(new schemaAttribute((schemaAttribute)v2a.attrs.elementAt(i)));
	}

	/**
	 * @return the list of attributes associated with this variable
	 */

	public Vector getAttributeList() {
		return attrs;
	}

	/**
	 * @return the name of the variable
	 */

	public String getVar() {
		return var;
	}

	/**
	 * @param another schemaAttribute to be associated with this variable
	 */

	public void addAttribute(schemaAttribute a) {
		attrs.addElement(a);
	}

	/**
	 * @return the first schemaAttribute associated with this variable
	 */

	public schemaAttribute getAttribute() {
		return (schemaAttribute)attrs.elementAt(0);
	}

	/**
	 * print to the standard output
	 */

	public void dump() {
		System.out.println("varToAttr");
		System.out.println("var : " + var);
		System.out.println("Attribute List");
		for(int i=0;i<attrs.size();i++)
		       ((schemaAttribute)attrs.elementAt(i)).dump();
	}
}
