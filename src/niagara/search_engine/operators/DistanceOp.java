
/**********************************************************************
  $Id: DistanceOp.java,v 1.1 2000/05/30 21:03:28 tufte Exp $


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


package niagara.search_engine.operators;

import java.util.Vector;
import niagara.search_engine.indexmgr.*;
import java.net.*;

/**
 * Given two inverted lists, a numeric comparison operator and a 
 * numeric value, DistanceOp returns an inverted list in
 * which each position is a pairing of the positions from the two
 * inverted lists.
 *
 * This operator can only be applied on words, not on elements, since we
 * do not support ordering on elements, nor distance.
 *
 */

public class DistanceOp extends AbstractOperator {
    private Vector from; // the 'from' inverted list
    private Vector to;   // the 'to' inverted list
    private String orderOp; // the numeric comparison operator
    private double value; // the numeric value


    /**
     * Constructor
     */
    public DistanceOp(Vector operands) {
	super(operands);

	from = (Vector)operands.elementAt(0);
	to = (Vector)operands.elementAt(1);
	orderOp = (String)operands.elementAt(2);
	value = ((Double)operands.elementAt(3)).doubleValue();
    }

  //<NOTE: change 1, 2.1
    /**
     * Constructor
     */
    public DistanceOp(String op, double val) {
      orderOp = op;
      value = val;
    }

    public void addIVLs(Vector operands) {
	from = (Vector)operands.elementAt(0);
	to = (Vector)operands.elementAt(1);
    }

    public String toString() {
	return "DistanceOp("+orderOp+value+")";
    }
  //NOTE>

    /**
     * Operator evaluation
     */
    public void evaluate () throws IMPositionException 
    {
	isEvaluated = true;

	if (from==null || to==null) {
	    return;
        }

	int i=0, j=0;
	IVLEntry fromEnt, toEnt;
	long fromDocno, toDocno;

	while (i < from.size() && j < to.size()) {
	    fromEnt = (IVLEntry)from.elementAt(i);
	    toEnt = (IVLEntry)to.elementAt(j);
	    fromDocno = fromEnt.getDocNo();
	    toDocno = toEnt.getDocNo();

	    if (fromDocno < toDocno) {
		i++;
		continue;		
	    }
	    else if (toDocno < fromDocno) {
		j++;
		continue;
	    }
	    else { //(fromDocno == toDocno) 
		Vector qualifiedPosList = new Vector();
		Vector fromPosList = fromEnt.getPositionList();
		Vector toPosList = toEnt.getPositionList();

		if (fromPosList != null && toPosList != null) {
		    // scan over two position lists and compare
		    for (int pi=0; pi<fromPosList.size(); pi++) {
			Position fromPos = (Position)fromPosList.elementAt(pi);
			if (fromPos.getType() != Position.PT_SINGLE) {
			    throw new IMPositionException();
			}
			for (int pj=0; pj<toPosList.size(); pj++) {
			    Position toPos = (Position)toPosList.elementAt(pj);
			    if (toPos.getType() != Position.PT_SINGLE) {
				throw new IMPositionException();
			    }
			    int distance = Math.abs(
				fromPos.intValue()-toPos.intValue());
			    if (orderEvaluate(orderOp, distance, value)
				==true) {
				qualifiedPosList.addElement (
				    fromPos.concatenate(toPos));
			    }
			}
		    }

		    if (qualifiedPosList.size() != 0) {
			resultIVL.addElement (new IVLEntry
				(fromDocno, qualifiedPosList));
		    }
		    else {
			qualifiedPosList = null;
		    }
		    i++;
		    j++;		
		}
	    }
	} // end while
    }

    /**
     * Evaluate an order operator on two numeric values.
     *
     * @return true if the two numeric values satisfy the order op.
     */
    private boolean orderEvaluate(String orderOp, double val1, double val2) {
	if (orderOp.equals(">")) {
	    return (val1 > val2);
	}
	else if (orderOp.equals(">=")) {
	    return (val1 >= val2);
	}
	else if (orderOp.equals("=")) {
	    return (val1 == val2);
	}
	else if (orderOp.equals("<")) {
	    return (val1 < val2);
	}
	else if (orderOp.equals("<=")) {
	    return (val1 <= val2);
	}
	return false;
    }

    /**
     * Test driver
     */
    public static void main(String args[]) {
	try {
	    IndexMgr.idxmgr.index (
		new URL("http://www.cs.wisc.edu/~czhang/xml/bib_19.xml"));

	    // "vendor"
	    Vector params = new Vector();
	    params.addElement(IndexMgr.idxmgr);
	    params.addElement("vendor");
	    params.addElement(new Boolean(false));
	    IVLOp iop1 = new IVLOp(params);
	    iop1.evaluate();
	    Vector ivl1 = iop1.getResult();

	    // "title"
	    params.setElementAt("title",1);
	    IVLOp iop2 = new IVLOp(params);
	    iop2.evaluate();
	    Vector ivl2 = iop2.getResult();

	    // distance("title","vendor")<=5
	    params = null;
	    params = new Vector();
	    params.addElement (ivl1);
	    params.addElement (ivl2);
	    params.addElement (new String("<="));
	    params.addElement (new Double(5));
	    DistanceOp dop = new DistanceOp(params);
	    dop.evaluate();
	    Vector disIvl = dop.getResult();

	    // print results
	    for (int i = 0; i < disIvl.size(); i++) {
		IVLEntry ivlent = (IVLEntry)disIvl.elementAt(i);
		System.out.println ("Found in "
		    +IndexMgr.idxmgr.getDocName(ivlent.getDocNo()));

		Vector pl = ivlent.getPositionList();
		System.out.println(IndexMgr.idxmgr.retrieve(
			ivlent.getDocNo(), pl));
	    }
	}
	catch (IMException e) {
	    e.printStackTrace();
	}
	catch (MalformedURLException e) {
	    e.printStackTrace();
	}
    }
}
