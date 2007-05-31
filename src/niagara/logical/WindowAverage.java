/**********************************************************************
  $Id: WindowAverage.java,v 1.2 2007/05/31 03:36:20 jinli Exp $


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
 * This is the class for the count operator, that is a type of group
 * operator.
 *
 * @version 1.0
 *
 */

package niagara.logical;
import org.w3c.dom.*;
import java.util.ArrayList;

import niagara.connection_server.Catalog;
import niagara.connection_server.InvalidPlanException;
import niagara.optimizer.colombia.LogicalProperty;

public class WindowAverage extends WindowAggregate {

	public void dump() {
	super.dump("windowAverageOp");
	}

	public void loadFromXML(Element e, LogicalProperty[] inputProperties, Catalog catalog) 
	throws InvalidPlanException {
		ArrayList aggrAttrNames = new ArrayList ();
		String attrName1, attrName2;
		
		attrName1 = e.getAttribute("avgattr");
		if (attrName1 != "") {
			aggrAttrNames.add("avgattr");
		} else {
			attrName1 = e.getAttribute("sumattr");
			attrName2 = e.getAttribute("countattr");
			if (attrName1 != "" && attrName2 != "" ) {
				aggrAttrNames.add("sumattr");
				aggrAttrNames.add("countattr");				
			} else {
				throw new InvalidPlanException("no aggregate attribute specified"); 
			}
		}
		super.loadFromXML(e, inputProperties, aggrAttrNames);
	}

	protected WindowAggregate getInstance() {
	return new WindowAverage();
	}
}
