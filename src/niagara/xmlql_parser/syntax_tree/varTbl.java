
/**********************************************************************
  $Id: varTbl.java,v 1.4 2000/08/28 22:06:15 vpapad Exp $


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
* This class is used to store mapping between variable and attribute
*
*/
package niagara.xmlql_parser.syntax_tree;

import java.util.*;

import org.w3c.dom.*;

public class varTbl {
	private Vector varList;   // of varToAttr

	public varTbl() {
		varList = new Vector();
	}

	/**
	 * Constructor
	 *
	 * @param variable table to make a copy of
	 */

	public varTbl(varTbl vt) {
		varList = new Vector();
		for(int i=0;i<vt.varList.size();i++)
			varList.addElement(new varToAttr((varToAttr)vt.varList.elementAt(i)));
	}

	/**
	 * @return number of variables
	 */

	public int size() {
		return varList.size();
	}

	/**
	 * @param index into the table
	 * @return the Nth entry of the table
	 */

	public varToAttr getVarToAttr(int i) {
		return (varToAttr)varList.elementAt(i);
	}

	/**
	 * @param the variable to look for
	 * @param the schemaAttribute for the given variable
	 */

	public schemaAttribute lookUp(String var) {
		varToAttr entry;

		for(int i=0;i<varList.size();i++) {
			entry = (varToAttr)varList.elementAt(i);
			if(var.equals(entry.getVar()))
				return entry.getAttribute();
		}
		return null;
	}

	/**
	 * to check if this table contains a given set of variables
	 *
	 * @param the given set of variables
	 * @return true if it contains the given set of variables, false
	 *         otherwise
	 */

	public boolean contains(Vector variables) {
		String var;
		for(int i=0;i<variables.size();i++) {
			var = (String)variables.elementAt(i);
			if(lookUp(var) == null)
				return false;
		}
		return true;
	}

	/**
	 * add a new entry to the table
	 *
	 * @param the name of the variable
	 * @param associated schemaAttribute
	 */

	public void addVar(String var, schemaAttribute _attr) {
		varToAttr entry;

		for(int i=0;i<varList.size();i++) {
			entry = (varToAttr)varList.elementAt(i);
			if(var.equals(entry.getVar())) {
				entry.addAttribute(_attr);
				return;
			}
		}

		varList.addElement(new varToAttr(var,_attr));
	}

	/**
	 * to find the intersection with another varTbl
	 *
	 * @param another variable table to intersect with
	 * @return list of variables coomon in the two var tables
	 */

	public Vector intersect(varTbl rightVarTbl) {
		Vector rightVarList = rightVarTbl.varList;
		Vector leftVarList = varList;
		Vector commonVar = new Vector();
		varToAttr leftvar, rightvar;
		String var;

		for(int i=0;i<leftVarList.size();i++) {
			leftvar = (varToAttr)leftVarList.elementAt(i);
			for(int j=0;j<rightVarList.size();j++) {
				rightvar = (varToAttr)rightVarList.elementAt(j);
				var = leftvar.getVar();	
				if(var.equals(rightvar.getVar()))
					commonVar.addElement(var);
			}
		}
		return commonVar;
	}

    public HashMap getMapping() {
	HashMap hm = new HashMap(varList.size());
	for (int i=0; i < varList.size(); i++) {
	    varToAttr vta = (varToAttr) varList.get(i);
	    String varName = vta.getVar();
	    schemaAttribute sa = (schemaAttribute) vta.
		getAttributeList().get(0);
	    int position = sa.getAttrId();
	    hm.put(varName, new Integer(position));
	}
	return hm;
    }

	/**
	 * print to the standard output
	 */

	public void dump() {
		for (int i=0;i<varList.size();i++) 
			((varToAttr)varList.elementAt(i)).dump();
	}
}
