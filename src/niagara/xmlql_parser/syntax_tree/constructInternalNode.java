/**********************************************************************
  $Id: constructInternalNode.java,v 1.4 2002/12/10 00:53:29 vpapad Exp $


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
* This class is used to represent tags and subelements in construct part. The
* value of a leaf element is represented by another class called 
* constructLeafNode. 
*
*/
package niagara.xmlql_parser.syntax_tree;

import java.util.*;

import niagara.optimizer.colombia.Attrs;
import niagara.utils.CUtil;
import org.w3c.dom.*;

public class constructInternalNode extends constructBaseNode {

	// children or subelements of this node
	private Vector children;

	// tagnames with attributes and skolem function
	private startTag st;

	/**
	 * Constructor
	 *
	 * @param start tag that representa tag name, attributes, and skolem
	 * @param list of children
	 */

	public constructInternalNode(startTag s, Vector v) {
		st = s;
		children = v;
	}

	/**
	 * @return list of children
	 */
	public Vector getChildren() {
		return children;
	}

	/**
	 * @param set children to this list
	 */
	public void setChildren(Vector child) {
		children = child;
	}

	/**
	 * @return get the start tag
	 */
	public startTag getStartTag() {
		return st;
	}

	/**
	 * @return tag name
	 */
	public data getTagData() {
		return st.getSdata();
	}

	/**
	 * @return skolem function
	 */
	public skolem getSkolem() {
		return st.getSkolemId();
	}

	/**
	 * @return list of attribute-value pair
	 */
	public Vector getAttrList() {
		return st.getAttrList();
	}

	/**
	 * replaces the occurences of variables with their corresponding
	 * schema attribute
	 *
	 * @param the var table that stores the mapping between variables
	 *        and schema attribute
	 */

	public void replaceVar(varTbl vt) {
		st.replaceVar(vt);
		
		constructBaseNode child;
		for(int i=0;i<children.size();i++) {
			child = (constructBaseNode)children.elementAt(i);
			child.replaceVar(vt);
		}
	}

// UGLY---------------will have to go    

	public void truncate() {
		children = new Vector();
		children.addElement(new constructLeafNode(new data(dataType.ATTR,new schemaAttribute(0,varType.ELEMENT_VAR))));
	}
//-----------------------------------------------------------------------

	/**
	 * prints to the standard output
	 *
	 * @param number of tabs before each line
	 */

	public void dump(int depth) {
		CUtil.genTab(depth);
		System.out.println("CHILDREN");
		(getStartTag()).dump(depth);
		Object bn;
		for(int i = 0; i<children.size(); i++) {
			bn = children.elementAt(i);
			if(bn instanceof constructLeafNode)
				((constructLeafNode)bn).dump(depth+1);
			else if (bn instanceof constructInternalNode)
				((constructInternalNode)bn).dump(depth+1);
			else if(bn instanceof schemaAttribute)
				((schemaAttribute)bn).dump(depth+1);
			else if(bn instanceof Integer)
				System.out.println(((Integer)bn).intValue());
		}
	}

    public Attrs requiredInputAttributes(Attrs attrs) {
        Attrs reqAttrs = new Attrs();
        reqAttrs.merge(st.requiredInputAttributes(attrs));
        for (int i = 0; i < children.size(); i++)
            reqAttrs.merge(((constructBaseNode) children.get(i)).requiredInputAttributes(attrs));
        return reqAttrs;
    }
}

