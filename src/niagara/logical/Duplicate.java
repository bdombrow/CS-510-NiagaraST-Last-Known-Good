/**********************************************************************
  $Id: Duplicate.java,v 1.1 2003/12/24 02:08:27 vpapad Exp $


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
package niagara.logical;

import org.w3c.dom.Element;

import niagara.connection_server.Catalog;
import niagara.connection_server.InvalidPlanException;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;

public class Duplicate extends UnaryOperator {

    private int numDestinationStreams;

    public void addDestinationStreams(){
        numDestinationStreams++;
    };

    public void dump() {
	System.out.println("dupOp");
    }

    public int getNumberOfOutputs() {
        return numDestinationStreams;
    }
    
    public LogicalProperty findLogProp(ICatalog catalog, LogicalProperty[] input) {
        return input[0].copy();
    }

    public Op opCopy() {
        Duplicate op = new Duplicate();
        op.numDestinationStreams = numDestinationStreams;
        return op;
    }
    
    public int hashCode() {
        return numDestinationStreams;
    }
    
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof Duplicate)) return false;
        if (obj.getClass() != Duplicate.class) return obj.equals(this);
        Duplicate other = (Duplicate) obj;
        return numDestinationStreams == other.numDestinationStreams;
    }

    public void loadFromXML(Element e, LogicalProperty[] inputProperties, Catalog catalog)
        throws InvalidPlanException {
        String branchAttr = e.getAttribute("branch");
        // XXX vpapad: catch format exception, check that we really have
        // that many output streams - why do we have to specify this here?
        numDestinationStreams = Integer.parseInt(branchAttr);
    }
}

