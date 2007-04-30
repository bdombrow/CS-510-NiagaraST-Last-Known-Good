/**********************************************************************
  $Id: constructLeafNode.java,v 1.2 2007/04/30 19:24:58 vpapad Exp $


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
* This class is used for representing leaf node in the construct tree
*
*/
package niagara.xmlql_parser;

import niagara.logical.Variable;
import niagara.optimizer.colombia.Attrs;
import niagara.optimizer.colombia.Attribute;
import niagara.utils.CUtil;
import niagara.utils.PEException;

public class constructLeafNode extends constructBaseNode {
    private data leafData;

    /**
     * constructor
     *
     * @param leaf data
     */
    public constructLeafNode(data d) {
        super();
        leafData = d;
    }

    /**
     * get the data
     *
     * @return leaf data
     */
    public data getData() {
        return leafData;
    }

    /**
     * if this leaf data is a variable then repalce it with the 
     * schema attribute representing that variable
     *
     * @param variable table that maps variable to schema attribute
     */
    public void replaceVar(varTbl vt) {
        schemaAttribute attr;
        int type = leafData.getType();
        if (type == dataType.VAR) {
            String var = (String) leafData.getValue();
            attr = vt.lookUp(var);
            // XXX vpapad: super ugly - must get "$" out of variables
            if (attr == null && var.charAt(0) == '$')
                attr = vt.lookUp(var.substring(1));
            if (attr == null) {
            	System.out.println(vt.getVars());
                throw new PEException("Could not look up variable " + var);
            }
            leafData = new data(dataType.ATTR, attr);
        }
    }

    /**
     * print leaf data to standard output
     *
     * @param number of tabs in the beginning of each line
     */
    public void dump(int depth) {
        CUtil.genTab(depth);
        System.out.println("constructLeaf");
        leafData.dump(depth);
    }

    /**
     * @see niagara.xmlql_parser.syntax_tree.constructBaseNode#requiredInputAttrs(Attrs)
     */
    public Attrs requiredInputAttributes(Attrs attrs) {
        int type = leafData.getType();
        if (type == dataType.VAR) {
            String varName = (String) leafData.getValue();
            // XXX vpapad: super ugly - must get "$" out of variables
            if (varName.charAt(0) == '$')
                varName = varName.substring(1);
	    Attribute var = attrs.getAttr(varName);
	    // This should have been caught at compile time
	    if (var == null)
                throw new PEException("Unknown variable: " + varName);
            return new Attrs(var);
        }
        else
            return new Attrs();
    }
}
