/**********************************************************************
  $Id: dupOp.java,v 1.6 2002/10/31 04:17:05 vpapad Exp $


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
 * The class <code>dupOp</code> is the class for operator Duplicate.
 * 
 * @version 1.0
 *
 * @see op 
 */
package niagara.xmlql_parser.op_tree;

import java.util.ArrayList;

import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;

public class dupOp extends unryOp {

    private int numDestinationStreams;

    public void addDestinationStreams(){
        numDestinationStreams++;
    };

    public void setDup(int numDestinationStreams) {
	this.numDestinationStreams = numDestinationStreams;
    }

    public void dump() {
	System.out.println("dupOp");
    }

    public int getNumberOfOutputs() {
        return numDestinationStreams;
    }
    
    public LogicalProperty findLogProp(ICatalog catalog, LogicalProperty[] input) {
        return input[0].copy();
    }

    public Op copy() {
        dupOp op = new dupOp();
        op.numDestinationStreams = numDestinationStreams;
        return op;
    }
    
    public int hashCode() {
        return numDestinationStreams;
    }
    
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof dupOp)) return false;
        if (obj.getClass() != dupOp.class) return obj.equals(this);
        dupOp other = (dupOp) obj;
        return numDestinationStreams == other.numDestinationStreams;
    }
}

