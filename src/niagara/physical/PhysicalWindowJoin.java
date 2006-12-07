/**********************************************************************
  $Id: PhysicalWindowJoin.java,v 1.1 2006/12/07 00:06:31 jinli Exp $


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


package niagara.physical;

import java.util.Vector;
import java.util.LinkedList;

import niagara.logical.*;
import niagara.logical.predicates.*;
import niagara.optimizer.colombia.*;
import niagara.physical.predicates.PredicateImpl;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;
import niagara.utils.*;

import org.w3c.dom.*;

/**
 * This is the <code>PhysicalWindowJoinOperator</code> that extends
 * the basic PhysicalOperator with the implementation of the Hash
 * Join operator.
 *
 * @version 1.0
 *
 */

public class PhysicalWindowJoin extends PhysicalJoin {
    // No blocking input streams
    private static final boolean[] blockingSourceStreams = { true, true };

    // Optimization time structures
    /** Equijoin predicates */
    private EquiJoinPredicateList eqjoinPreds;
    /** Runtime implementation of joinPredicate */
    private PredicateImpl pred;

    private Hasher[] hashers;
    private String[] rgstPValues;
    private String[] rgstTValues;
    private ArrayList[] rgPunct = new ArrayList[2];
    
    private int count;
    
    private int INTERVAL = 0;

    // The array of hash tables of partial tuple elements that are read from the
    // source streams. The index of the array corresponds to the index of the
    // stream from which the tuples were read.
    DuplicateHashtable[] partialSourceTuples;

    // The array of hash tables of final tuple elements that are read from the
    // source streams. The index of the array corresponds to the index of the
    // stream from which the tuples were read.
    DuplicateHashtable[] finalSourceTuples;
    
    Document doc;
    
    private SimpleAtomicEvaluator[] timestamp;
       
    
    public PhysicalWindowJoin() {
        setBlockingSourceStreams(blockingSourceStreams);
    }
    
    public void initJoin(Join join) {
        eqjoinPreds = join.getEquiJoinPredicates();
        // In hash join, we hope that most tuples that hash the same
        // do indeed pass the equijoin predicates, so we put them last
        joinPredicate = And.conjunction(join.getNonEquiJoinPredicate(), 
                                        eqjoinPreds.toPredicate());
        pred = joinPredicate.getImplementation();
        setWindow((WindowJoin)join);
        initExtensionJoin(join);
    }
    
    /**
     * As we assume linear punctuation on timestamp attribute, we decide whether a input tuple 
     * should be stored by the timestamp value of the newest punctuation. In the previous 
     * implementation, punctuations are assumed to be on joining attributes.
     */
    protected void blockingProcessTuple (
		     Tuple tupleElement,
		     int streamId) 
    throws ShutdownException {

	// Get the hash code corresponding to the tuple element
	String hashKey = hashers[streamId].hashKey(tupleElement);
	
	// ignore null attributes...
	if(hashKey == null)
		return;
		
	String stPunctJoinKey = null;
	
	// Determine the id of the other stream
	int otherStreamId = 1 - streamId;
	
	// Now loop over all the partial elements of the other source 
	// and evaluate the predicate and construct a result tuple if 
	// the predicate is satisfied
	boolean producedResult;
	producedResult = constructJoinResult(tupleElement, streamId, hashKey,
				     partialSourceTuples[otherStreamId]);
	
	// Extension join note: Extension join indicates that we
	// expect an extension join with referential integrity 
	// meaning that tuples on the denoted extension join side
	// join only with one tuple from the other side, if I
	// join with a tuple, in partial, no need to check final
	// if I join with tuple from partial or final, no need
	// to insert in hash table
	
	if(!extensionJoin[streamId] || !producedResult) {
	// Now loop over all the final elements of the other source 
	// and evaluate the predicate and construct a result tuple if 
	// the predicate is satisfied
	producedResult = constructJoinResult(tupleElement, streamId, 
					 hashKey,
				   finalSourceTuples[otherStreamId]);
	}
	
	if(!extensionJoin[streamId] || !producedResult) {
	//System.out.println("KT inserting in hash table streamID " + streamId);
	// we don't add to hash table if this is extension join
	// and already joined (see above)
	
	// Add the tuple element to the appropriate hash table,
	// but only if we haven't yet seen a punctuation from the
	// other input that matches it
	boolean fMatch=false;
	
	int size = rgPunct[otherStreamId].size();
	int pos, ts;
	//Node tsNode;
	BaseAttr tsNode;
	if (size == 0)
		ts = -1;
	else {
		// Get the punctuated ts value;
		Punctuation punct = (Punctuation)rgPunct[otherStreamId].get( size - 1);
		pos = inputTupleSchemas[otherStreamId].getPosition("timestamp"+(otherStreamId+1));
		//tsNode = punct.getAttribute(pos);
		tsNode = (BaseAttr)punct.getAttribute(pos);
		
		// The threshold ts value that tuples with smaller ts on the other input can be purged. 
		// "+1" is a hack, to fix the punctuation from data source.
		//ts = Integer.parseInt(tsNode.getFirstChild().getNodeValue()) +1 - INTERVAL;		
		ts = Integer.parseInt(tsNode.toASCII()) +1 - INTERVAL;
	}
	
	pos = inputTupleSchemas[streamId].getPosition("timestamp"+(streamId+1));
	//tsNode = tupleElement.getAttribute(pos);
	tsNode = (BaseAttr)tupleElement.getAttribute(pos);
	//double tupleTs =  Double.parseDouble(tsNode.getFirstChild().getNodeValue());
	double tupleTs =  Double.parseDouble(tsNode.toASCII());
	
	// if the tuples' ts value is smaller than the threshold ts, it should not be stored
	if (tupleTs < ts)
		fMatch = true;
	
	if (fMatch == false) {
	if (tupleElement.isPartial()) {
	   partialSourceTuples[streamId].put(hashKey, tupleElement);
	} else {
	   finalSourceTuples[streamId].put(hashKey, tupleElement);
	}
	}
	} else {
	//System.out.println("KT: not inserting in hash table " + streamId);
	}
}


    /**
     * This function removes the effects of the partial results in a given
     * source stream. This function over-rides the corresponding function
     * in the base class.
     *
     * @param streamId The id of the source streams the partial result of
     *                 which are to be removed.
     *
     */

    protected void removeEffectsOfPartialResult (int streamId) {

	// Clear the list of tuples in the appropriate stream
	partialSourceTuples[streamId].clear();
    }


    /**
     * This function constructs a join result based on joining with tupleElement
     * from the stream with id streamId with the hash table of tuples of the other
     * stream represented by the hash table otherStreamTuples. The join results are
     * returned in result
     *
     * @param tupleElement The tuple to be joined with tuples in other stream
     * @param streamId The stream id of tupleElement
     * @param hashCode The join hash code
     * @param otherStreamTuples The tuples to be joined with tupleElement
     * @returns true if result produced (that is tupleElement joined
     *  with something in otherStreamTuples)
     */

    private boolean constructJoinResult(Tuple tupleElement,
					int streamId,
					String hashKey,
					DuplicateHashtable otherStreamTuples) 
	throws ShutdownException{

    try {
	boolean producedResult = false;

	// Get the list of tuple elements having the same hash code in
	// otherStreamTuples
	//
	Vector otherSourceTuples = otherStreamTuples.get(hashKey);

	// Loop over all the elements of the other source stream and
	// evaluate the predicate and construct a result tuple if the
	// predicate is satisfied
	if (otherSourceTuples == null) 
	    return false;

	for (int tup = 0; tup < otherSourceTuples.size(); ) {
	    // Get the appropriate tuple for the other source stream
	    Tuple otherTupleElement = 
		(Tuple) otherSourceTuples.get(tup);

	    // Make the right order for predicate evaluation
	    //
	    Tuple leftTuple;
	    Tuple rightTuple;

	    if (streamId == 0) {
		leftTuple = tupleElement;
		rightTuple = otherTupleElement;
	    }
	    else {
		leftTuple = otherTupleElement;
		rightTuple = tupleElement;
	    }

	    boolean removed = false;
	    // Check whether the predicate is satisfied
	    if (pred.evaluate(leftTuple, rightTuple)) {
	    	/*int leftpos = inputTupleSchemas[0].getPosition("timestamp1");
	    	int rightpos = inputTupleSchemas[1].getPosition("timestamp2");
	    	double leftVal = Double.parseDouble(leftTuple.getAttribute(leftpos).getFirstChild().getNodeValue());
	    	double rightVal = Double.parseDouble(rightTuple.getAttribute(rightpos).getFirstChild().getNodeValue());
	    	if (Math.abs(leftVal - rightVal) > INTERVAL) {
	    		tup++;
	    		continue;
	    	}*/
	    	
	    	produceTuple(leftTuple, rightTuple);
	    	count++;
            producedResult = true;
            if(extensionJoin[1-streamId]) {
                boolean success =
                	otherStreamTuples.remove(hashKey, otherTupleElement);
                if(!success)
                	throw new PEException("KT: removal from hash table failed");
                removed = true;
            } 
            if(extensionJoin[streamId])
               	return producedResult;
	    }
	    if(!removed)
	    	tup++;
	    
	}
	return producedResult;
    } catch (InterruptedException e) {
    	System.err.println("Interrupted error.");
    	return false;
    }

    }
    
    /**
     * The orginal processPunctuation function is temporarily commented out, 
     * because it assumes punctuattion on the hashing attributes. Here, we assume 
     * linear punctuation on time. But eventually we should handle linear punctuation 
     * per group.  - Jenny 
     */
    protected void processPunctuation(Punctuation tuple,
		      int streamId)
		throws ShutdownException, InterruptedException {
		
		try {
		int pos = inputTupleSchemas[streamId].getPosition("timestamp"+(streamId+1));
		//Node tsNode = tuple.getAttribute(pos);
		BaseAttr tsNode = (BaseAttr)tuple.getAttribute(pos);
		//int ts = Integer.valueOf(tsNode.getFirstChild().getNodeValue()) + 1 - INTERVAL;
		//int ts = Integer.valueOf(tsNode.getFirstChild().getNodeValue()) + 1;
		int ts = Integer.valueOf(tsNode.toASCII()) + 1;
		
		// see if there are tuples to remove from the other hash table.
		// check both the partial list and the final list
		int otherStreamId = 1-streamId;
		
		Iterator keys = finalSourceTuples[otherStreamId].keySet().iterator();
		Vector hashEntry;
		Iterator list;
		
		while (keys.hasNext()) {
			hashEntry = finalSourceTuples[otherStreamId].get(keys.next());
			list = hashEntry.iterator();
	    	while (list.hasNext()) {
	    		//if ((ts - INTERVAL) > Integer.valueOf(((Tuple)list.next()).getAttribute(pos).getFirstChild().getNodeValue()))
	    		if ((ts - INTERVAL) > Integer.valueOf(((BaseAttr)((Tuple)list.next()).getAttribute(pos)).toASCII()))
	    			list.remove();
	    	}
	    	if (hashEntry.size() == 0)
	    		keys.remove();
		}
		
		// then, produce punctuation and add this punctuation
		// to the appropriate list of punctuations if necessary
		Iterator it = rgPunct[otherStreamId].iterator();
		pos = inputTupleSchemas[otherStreamId].getPosition("timestamp"+(otherStreamId+1));
		int otherTs;
		boolean fMatch = false;
		Punctuation punct;
		while (it.hasNext() && !fMatch) {
			punct = (Punctuation)it.next();
			tsNode = (BaseAttr)punct.getAttribute(pos);
			//otherTs = Integer.valueOf(tsNode.getFirstChild().getNodeValue()) + 1;
			otherTs = Integer.valueOf(tsNode.toASCII()) + 1;
			if ((ts - INTERVAL) == otherTs) {
				if (streamId == 0)
					producePunctuation(tuple, punct, 1);
				else
					producePunctuation(punct, tuple, 0);
				it.remove();
			} else if ((ts + INTERVAL) == otherTs) {
	    		if (streamId == 0)
	    			producePunctuation (tuple, punct, 0);
	    		else
	    			producePunctuation (punct, tuple, 1);

				fMatch = true;
			}
		}
		if (!fMatch)
			rgPunct[streamId].add(tuple);
		} catch (java.lang.ArrayIndexOutOfBoundsException ex) {
		//Not a punctuation for the join attribute. Ignore it.
		;
		}
		return;
    }
    
    /**
     * @param: punctSide indicates the attribute on which the produced punctuation will punctuate.
     * 	For example, when a left-side punctuation arrives, it is possilbe to produce a punctuation 
     *  on the ordering attribute of the right side.
     */
    protected void producePunctuation(
            Punctuation left,
            Punctuation right,
            int punctSide)
            throws ShutdownException, InterruptedException {
            Punctuation result;
            
            if (projecting) {
                result = (Punctuation)left.copy(outputTupleSchema.getLength(), leftAttributeMap);
                right.copyInto(result, leftAttributeMap.length, rightAttributeMap);
            } else {
                result = (Punctuation)left.copy(outputTupleSchema.getLength());
                result.appendTuple(right);
            }
            
            if (punctSide == 0) { // left side punctuation;
        		int pos = outputTupleSchema.getPosition(timestamp[1].getName());
        		Element n = doc.createElement(timestamp[1].getName());
        		n.appendChild(doc.createTextNode("*"));
        		result.setAttribute(pos, n);
            } else { // right side punctuation;
        		int pos = outputTupleSchema.getPosition(timestamp[0].getName());
        		Element n = doc.createElement(timestamp[0].getName());
        		n.appendChild(doc.createTextNode("*"));
        		result.setAttribute(pos, n);        	
            }	

            // Add the result to the output
            putTuple(result, 0);
        }


    private void purgeHashTable(DuplicateHashtable ht, int streamId,
				String[] rgstPunct) {
	Enumeration enKeys = ht.keys();
	while (enKeys.hasMoreElements()) {
	    String hashKey = (String) enKeys.nextElement();
	    hashers[streamId].getValuesFromKey(hashKey, rgstTValues);

	    boolean fMatch=true;
	    for (int i=0; i<rgstPunct.length && fMatch==true; i++) {
		fMatch = Punctuation.matchValue(rgstPunct[i],
							     rgstTValues[i]);
	    }

	    if (fMatch)
		ht.remove(hashKey);
	}
    }

    public boolean isStateful() {
	return true;
    }
    

/**
 * @see niagara.optimizer.colombia.PhysicalOp#FindLocalCost(ICatalog, LogicalProperty, LogicalProperty[])
 */
public Cost findLocalCost(
    ICatalog catalog,
    LogicalProperty[] inputLogProp) {
    float leftCard = inputLogProp[0].getCardinality();
    float rightCard = inputLogProp[1].getCardinality();
    float inputCard = leftCard + rightCard;
    float outputCard = logProp.getCardinality();

    double cost = inputCard * catalog.getDouble("tuple_reading_cost");
    cost += inputCard * catalog.getDouble("tuple_hashing_cost");
    cost += outputCard * constructTupleCost(catalog);
    Cost c = new Cost(cost);
    // XXX vpapad: We must compute the predicate on all the tuple combinations
    // that pass the equality predicates we're hashing on; but how do we
    // compute that? We'll just assume that's the same as the tuples that
    // appear in the output (best case)
    c.add(pred.getCost(catalog).times(outputCard));
    return c;
}


    /**
     * @see niagara.query_engine.PhysicalOperator#opInitialize()
     */
    protected void opInitialize() {
        Attrs leftAttrs = eqjoinPreds.getLeft();
        Attrs rightAttrs = eqjoinPreds.getRight();

        hashers = new Hasher[2];
        hashers[0] = new Hasher(leftAttrs);
        hashers[1] = new Hasher(rightAttrs);

        rgstPValues = new String[leftAttrs.size()];
        rgstTValues = new String[rightAttrs.size()];
        
        pred.resolveVariables(inputTupleSchemas[0], 0);
        pred.resolveVariables(inputTupleSchemas[1], 1);

        hashers[0].resolveVariables(inputTupleSchemas[0]);
        hashers[0].resolveVariables(inputTupleSchemas[1]);
        hashers[1].resolveVariables(inputTupleSchemas[0]);
        hashers[1].resolveVariables(inputTupleSchemas[1]);
        
        // Initialize the array of hash tables of partial source tuples - there are
        // two input stream, so the array is of size 2
        partialSourceTuples = new DuplicateHashtable[2];

        partialSourceTuples[0] = new DuplicateHashtable();
        partialSourceTuples[1] = new DuplicateHashtable();

        // Initialize the array of hash tables of final source tuples - there are
        // two input stream, so the array is of size 2
        finalSourceTuples = new DuplicateHashtable[2];

        finalSourceTuples[0] = new DuplicateHashtable();
        finalSourceTuples[1] = new DuplicateHashtable();

        //Initialize the punctuation lists
        rgPunct[0] = new ArrayList();
        rgPunct[1] = new ArrayList();
        
        timestamp[0].resolveVariables(inputTupleSchemas[0], 0);
        timestamp[1].resolveVariables(inputTupleSchemas[1], 1);
    }

    private void setWindow (WindowJoin join) {
        Attribute[] windowingAttrs = join.getWa();
        
        timestamp = new SimpleAtomicEvaluator[2];
              
        timestamp[0] = new SimpleAtomicEvaluator(windowingAttrs[0].getName());
        timestamp[1] = new SimpleAtomicEvaluator(windowingAttrs[1].getName());

        INTERVAL = join.getInterval();
    }
    
    /**
     * @see niagara.optimizer.colombia.Op#copy()
     */
    public Op opCopy() {
        PhysicalWindowJoin op = new PhysicalWindowJoin();
        op.pred = pred;
        op.joinPredicate = joinPredicate;
        op.eqjoinPreds = eqjoinPreds;
        op.extensionJoin = extensionJoin;
        op.timestamp = timestamp;
        return op;
    }
    
	protected final void flushCurrentResults(boolean partial)
	throws InterruptedException, ShutdownException {
		System.out.println("join test"+count);
	}

    public void setResultDocument(Document doc) {
    	this.doc = doc;
        }    

}

