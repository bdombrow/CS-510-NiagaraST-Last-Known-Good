
/**********************************************************************
  $Id: Nest.java,v 1.4 2003/07/09 04:59:41 tufte Exp $


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
 * This is the class for the nest operator, that is a type of group
 * operator.
 *
 * @version 1.0
 *
 */

package niagara.logical;

import java.io.StringReader;
import java.util.Vector;

import org.w3c.dom.*;

import niagara.utils.XMLUtils;
import niagara.connection_server.InvalidPlanException;
import niagara.optimizer.colombia.*;
import niagara.xmlql_parser.syntax_tree.*;

// KT ooooh is this UUUGLY!!!!!!!
public class Nest extends niagara.xmlql_parser.op_tree.groupOp {

    // This stores the template of the result
    private constructBaseNode resultTemplate;

    public Nest() {
	resultTemplate = null;
	groupingAttrs = null;
    }

    /**
     * @see niagara.optimizer.colombia.Op#copy()
     */
    public Op opCopy() {
	Nest newOp = new Nest();
	newOp.resultTemplate = this.resultTemplate;
	newOp.groupingAttrs = this.groupingAttrs;
	return newOp;
    }
    

    /**
     * @see java.lang.Object#equals(Object)
     */
    public boolean equals(Object obj) {
	if (obj == null || !(obj instanceof Nest))
            return false;
        if (obj.getClass() != Nest.class)
            return obj.equals(this);
        Nest other = (Nest) obj;
        // XXX vpapad: constructBaseNode.equals is still Object.equals
        return this.resultTemplate.equals(other.resultTemplate) &&
	    this.groupingAttrs.equals(groupingAttrs);
    }

    /**
     * @see java.lang.Object#hashCode()
     */
    public int hashCode() {
        return resultTemplate.hashCode() ^ groupingAttrs.hashCode();
    }


    /**
     * This function sets the construct part of the nest operator, which
     * specifies the result template
     *
     * @param resultTemplate The template of the result
     */
    public void setNest (constructBaseNode resultTemplate) {
	// Set the result template
	this.resultTemplate = resultTemplate;
    }


    /**
     * This function returns the result template associated with the operator
     *
     * @return Result template associated with the operator
     */
    public constructBaseNode getResTemp () {

	// Return the result template
	return resultTemplate;
    }

    public void dump() {
	System.out.println("Nest");
    }

    public void loadFromXML(Element e, LogicalProperty[] inputProperties)
        throws InvalidPlanException {

        NodeList children = e.getChildNodes();
        String content = "";
        for (int i = 0; i < children.getLength(); i++) {
            int nodeType = children.item(i).getNodeType();
            if (nodeType == Node.ELEMENT_NODE)
                content += XMLUtils.explosiveFlatten(children.item(i));
            else if (nodeType == Node.CDATA_SECTION_NODE)
                content += children.item(i).getNodeValue();
        }
        resultTemplate = null;

        try {
            Scanner scanner = new Scanner(new StringReader(content));
            ConstructParser cep = new ConstructParser(scanner);
            resultTemplate = (constructBaseNode) cep.parse().value;
            cep.done_parsing();
        } catch (Exception ex) {
            throw new InvalidPlanException("Error while parsing Nest construct template: "
					   + content);
        }
	loadGroupingAttrsFromXML(e, inputProperties[0], "neston");
	//groupingAttrs = 
	//((constructInternalNode) resultTemplate).getSkolem();
	verifyTopLevelAttrs();
    }

    // verify that only attributes used to nest are used in the
    // creation of attributes in the top-level node (this is 
    // similar to restriction that only grouping attributes
    // can appear in the select clause of a query with groupby)
    private void verifyTopLevelAttrs() //LogicalProperty inputLogProp) 
	throws InvalidPlanException {
	// get attribute from the resultTemplate 
	// appears we know that resultTemplate is a constructInternalNode
        Vector attrs = ((constructInternalNode)resultTemplate).
	                            getStartTag().getAttrList();
	int numAttrs = attrs.size();
	for(int i = 0; i<numAttrs; i++) {
	    attr attribute = (attr)attrs.get(i);
	    data attrData = attribute.getValue();
	    switch(attrData.getType()) {
	    case dataType.STRING:
		// think ok - nothing to check
		break;
		
	    case dataType.ATTR:
		// maybe I don't need this code...
		assert false : "KT Don't think I should get attrs here";
		break;
		/* old code frm when I thought there might be attrs,
		   save for a bit...
                schemaAttribute schema = (schemaAttribute) attrData.getValue();
                switch(schema.getType()) {
		case varType.ELEMENT_VAR:
		case varType.CONTENT_VAR:
		    int attributeId =
                        ((schemaAttribute) attrData.getValue()).getAttrId();
		    // look up attribute id in something??
		    //Attribute a = inputLogProp.GetAttr(attributeId);
		    //assert a != null: "Couldn't find attribute in log prop ";
 		    //String attrName = a.getName(); 
		    Vector v = groupingAttrs.getVarList(); 
		    boolean ok = false;
		    for(int j = 0; j<v.size(); j++) {
			schemaAttribute sa = (schemaAttribute)v.get(j);
			if(sa.getAttrId() == attributeId)
			    ok = true;
		    }
		    if(!ok)
			throw new InvalidPlanException("Detected invalid top level attribute in Nest"); 
		default:
		    assert false: "Unknown var type in attribute constructor";
		}
		break;*/

	    case dataType.VAR:
		String varname = (String)attrData.getValue();
		if (varname.charAt(0) == '$')
		    varname = varname.substring(1);
		Vector v = groupingAttrs.getVarList(); 
		boolean ok = false;
		for(int j = 0; j<v.size(); j++) {
		    // v.get(j) is of type Variable
		    String gpname  = ((Variable)v.get(j)).getName();
		    if(gpname.equals(varname))
			ok = true;
		}
		if(!ok) 
		    throw new InvalidPlanException("Detected invalid top level attribute in Nest");
		break;
		
	    default:
		assert false: "Unknown data type " + attrData.getType();
            }    
	}
    }
}
