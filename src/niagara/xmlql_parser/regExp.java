
/**********************************************************************
  $Id: regExp.java,v 1.1 2003/12/24 01:19:54 vpapad Exp $


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
 * Regular Expression is represented as a binary tree. There are two type
 * of nodes: one represent the operators (like *, +, etc.) while other 
 * represents data (string or variable). This is the base class for both
 * of these types - regExpDataNode and regExpOpNode
 *
 */
package niagara.xmlql_parser;

import niagara.logical.path.Bar;
import niagara.logical.path.Constant;
import niagara.logical.path.Dot;
import niagara.logical.path.Epsilon;
import niagara.logical.path.Plus;
import niagara.logical.path.RE;
import niagara.logical.path.Star;
import niagara.logical.path.Wildcard;
import niagara.logical.path.ZeroOne;
import niagara.utils.PEException;

public abstract class regExp {

    public abstract void dump(int depth);
    public abstract String toString();
    public abstract boolean isNever();
                    
    public void genTab(int depth){
	for(int i=0; i<depth; i++)
	    System.out.print("\t");
    }

    public static RE regExp2RE(regExp r) {
        if (r == null)
            return new Epsilon();
    
        if (r instanceof regExpDataNode)
            return new Constant(
                ((String) ((regExpDataNode) r).getData().getValue()));
    
        regExpOpNode rop = (regExpOpNode) r;
        switch (rop.getOperator()) {
            case opType.BAR :
                return new Bar(
                    regExp2RE(rop.getLeftChild()),
                    regExp2RE(rop.getRightChild()));
            case opType.DOT :
                return new Dot(
                    regExp2RE(rop.getLeftChild()),
                    regExp2RE(rop.getRightChild()));
            case opType.DOLLAR :
                return new Wildcard();
            case opType.QMARK :
                return new ZeroOne(regExp2RE(rop.getLeftChild()));
            case opType.STAR :
                return new Star(regExp2RE(rop.getLeftChild()));
            case opType.PLUS :
                return new Plus(regExp2RE(rop.getLeftChild()));
            default :
                throw new PEException("Unknown operator in regExp2RE");
        }
    }
}