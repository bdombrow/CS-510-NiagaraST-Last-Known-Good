
/**********************************************************************
  $Id: DTDXMLQLTreeNode.java,v 1.1 2000/05/30 21:03:25 tufte Exp $


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


package niagara.client.dtdTree;

import java.awt.Color;
import java.util.*;

/**
 * Bookkeeping for xmlql queries.
 *
 */

public class DTDXMLQLTreeNode extends DTDTreeNode {
    // Variable counter
    private static int vc = 0;
    
    // further data for annotating in the gui
    //
    private String predicate = "";
    // the variable name associated with this node
    private String variable;
    // the default name of the variable
    // It is used to revert to the prior state
    // after a cancel join
    private String defaultVariable;
    // the projection flag
    private boolean isProjected;
    // signals whether this node participates in a join
    private boolean isJoined;
    
    // data used in creating the query
    //
    private StringBuffer qBuf = new StringBuffer();
    // 
    private boolean isSelected = false;
    
    private Color joinColor;
    
    // used only in the DTD class
    DTDTreeNode create(String name, int type) {
		return new DTDXMLQLTreeNode(name, type);
    }
    
    /**
     * Ctor 1 -- Should not be used
     */
    public DTDXMLQLTreeNode() {
		super();
    }
    /**
     * Ctor 2
     */
    public DTDXMLQLTreeNode(String name, int type) {
		super(name, type);
		variable = "$v"+(vc++);
		defaultVariable = variable;
		qBuf.append(">");
		joinColor = null;
    }
    
    public void setJoinColor(Color color) {
		joinColor = color;
    }
    
    public Color getJoinColor() {
		return joinColor;
    }
    
    public String getPredicate() {
		return predicate;
    }
    
    public void setPredicate(String predicate) {
		this.predicate = predicate;
    }
    
    public boolean hasPredicate() {
		return (predicate.length() > 0);
    }
    
    public String getVariableName() {
		return this.variable;
    }
	
    /**
     * changes the variable name for the 
     * purposes of join 
     * @param varName the name of the variable
     */
    public void setVariableName(String varName) {
		variable = varName;
    }
    
    /**
     * The gui collects the variable names from the 
     * nodes to be joined and calls this.
     * @param varNames a vector of variables
     */
    public void joinWith(Vector varNames) {
		Iterator it = varNames.iterator();
	
		while(it.hasNext()){
			addJoinVariable((String)(it.next()));
		}
    }
    
    public void cancelJoin() {
		setVariableName(defaultVariable);
    }
    
    
    /**
     * this adds the variable of the node to 
     * be joined.
     * @param var the variable name
     */
    public void addJoinVariable(String var) {
		int idx = 0;
		// get the rank of the variable
		int rank = Integer.parseInt((new StringTokenizer(var,"$v")).nextToken());
	
		StringTokenizer st = new StringTokenizer(variable, "$v");
	
		String s = null;
		while(st.hasMoreTokens()){
			s = st.nextToken();
			int vn = Integer.parseInt(s);
			// check where rank goes
			if(rank < vn){
				idx = variable.indexOf(s) - 1;
				break;
			}					
		}
	
		if(idx == 0) {
			idx = variable.length();
		}
	
		StringBuffer sb = new StringBuffer(variable);
		sb.insert(idx, "v" + rank);
		variable = sb.toString();
	
    }
    
    public boolean isJoined() {
		return isJoined;
    }
    
    public void project() {
		isProjected = true;
    }
    
    public void setJoined() {
		isJoined = true;
    }
    
	public boolean isProjected()
		{
			return isProjected;
		}
	
	public void setProjection(boolean prj)
		{
			isProjected = prj;
		}

	public void setSelection(boolean sel)
		{
			isSelected = sel;
		}
        
	public void select()
		{
			isSelected = true;
		}
        
	public void deSelect()
		{
			isSelected = false;
		}


	public boolean isSelected()
		{
			return isSelected || (predicate.length() > 0);
		}

	public StringBuffer getQueryBuffer()
		{
			return qBuf;
		}

	public void setQueryBuffer(StringBuffer sb)
		{
			qBuf = sb;
		}

	public String toString()
		{
			String ret = super.toString();
			return ret + ":" + getType() + ":" 
				+ variable + ":" 
				+ (isProjected()?"[X]":"[ ]") + ":"
				+ (isJoined()?"[J]":"[ ]");
		}

}
