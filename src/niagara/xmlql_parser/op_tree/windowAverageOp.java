/**********************************************************************
  $Id: windowAverageOp.java,v 1.1 2003/12/06 06:54:12 jinli Exp $


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

package niagara.xmlql_parser.op_tree;
import org.w3c.dom.*;

import niagara.connection_server.InvalidPlanException;
import niagara.optimizer.colombia.LogicalProperty;

public class windowAverageOp extends windowAggregateOp {

	public void dump() {
	super.dump("windowAverageOp");
	}

	public void loadFromXML(Element e, LogicalProperty[] inputProperties) 
	throws InvalidPlanException {
	super.loadFromXML(e, inputProperties, "avgattr");
	}

	protected windowAggregateOp getInstance() {
	return new windowAverageOp();
	}
}
