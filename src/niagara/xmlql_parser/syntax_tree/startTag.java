
/**********************************************************************
  $Id: startTag.java,v 1.2 2000/08/21 00:41:05 vpapad Exp $


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
 * This class is used to represent the tag name with attributes and skolem
 * function in the construct part of XML-QL.
 *
 */
package niagara.xmlql_parser.syntax_tree;

import java.util.*;

import org.w3c.dom.*;

public class startTag {
	private data sdata;      // tag name (identifier or variable)
	private skolem skolemId; // for skolem function
	private Vector attrList; // list of attribute-value pairs

	/**
	 * Constructor
	 *
	 * @param tag name
	 * @param skolem function
	 * @param list of attribute-value pairs
	 */

	public startTag(data d, skolem s, Vector v) {
		sdata = d;
		skolemId = s;
		attrList = v;
	}
       
	/**
	 * @return the tag name
	 */

	public data getSdata() {
		return sdata;
	}

	/**
	 * replaces the occurences of variables in the tag name, attributes 
	 * and skolem function with their corresponding schemaAttributes
	 *
	 * @param the variable table that maps variable to their schemaAttribute
	 */

	public void replaceVar(varTbl vt) {
		
		String var;
		int type;
		schemaAttribute sa;
		attr attribute;

		type = sdata.getType();
		if(type == dataType.VAR) {
			sa = vt.lookUp((String)sdata.getValue());
			sdata = new data(dataType.ATTR,sa);
		}

		for(int i=0;i<attrList.size();i++) {
			attribute = (attr)attrList.elementAt(i);
			attribute.replaceVar(vt);
		}

		if(skolemId != null)
			skolemId.replaceVar(vt);
	}

	/**
	 * @return skolem function
	 */

	public skolem getSkolemId() {
		return skolemId;
	}

	/**
	 * @return the list attribute-value pair
	 */

	public Vector getAttrList() {
		return attrList;
	}

	/**
	 * prints to the standard output
	 *
	 * @param number of tabs at the beginning of each line
	 */

	public void dump(int depth) {
		Util.genTab(depth);
		System.out.println("start_tag:");
		sdata.dump(depth);
		if(skolemId != null)
			skolemId.dump();
		for(int i = 0; i<attrList.size(); i++)
			((attr)attrList.elementAt(i)).dump(depth);
	}

}
