/**********************************************************************
  $Id: PhysicalSelectF.java,v 1.1 2008/10/21 23:11:47 rfernand Exp $


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

import niagara.connection_server.*;
import niagara.utils.*;
import niagara.logical.Select;
import niagara.logical.predicates.Predicate;
import niagara.optimizer.colombia.*;
import niagara.physical.predicates.PredicateImpl;
import niagara.query_engine.*;

/**
 * Implementation of the Select operator.
 */

public class PhysicalSelectF extends PhysicalOperator {
	// No blocking source streams
	private static final boolean[] blockingSourceStreams = { false };

	boolean produceOutput;

	// tuple counter
	private int tupleCount = 0;

	// The is the predicate to apply to the tuples
	private Predicate pred;    
	private PredicateImpl predEval;

	public PhysicalSelectF() {
		setBlockingSourceStreams(blockingSourceStreams);
	}

	public void opInitFrom(LogicalOp logicalOperator) {
		pred = ((Select)logicalOperator).getPredicate();	
		predEval =  pred.getImplementation();
	}

	public Op opCopy() {
		PhysicalSelectF p = new PhysicalSelectF();
		p.pred = pred;
		p.predEval = predEval;
		return p;
	}

	/**
	 * This function processes a tuple element read from a source stream
	 * when the operator is non-blocking. This over-rides the corresponding
	 * function in the base class.
	 *
	 * @param tupleElement The tuple element read from a source stream
	 * @param streamId The source stream from which the tuple was read
	 *
	 * @exception ShutdownException query shutdown by user or execution error
	 */

	protected void processTuple (
			Tuple inputTuple, int streamId)
	throws ShutdownException, InterruptedException {
		// Evaluate the predicate on the desired attribute of the tuple
		if (predEval.evaluate(inputTuple, null)) {
			tupleCount++;
			String ctrlMessage = this.getName();
			if (produceOutput) {
				putTuple(inputTuple, 0);
				System.out.println(ctrlMessage + " has produced " + tupleCount + " tuples, with flag " + produceOutput);
				Thread.sleep(500);
			}
			else System.out.println(this.getName() + "has suppressed output of tuple " + tupleCount);
			if (tupleCount > 5) {
				if(NiagraServer.RJFM) System.err.println(this.getName() + " has sent a Control Message, flag is " + produceOutput);
				sendCtrlMsgUpStream(ControlFlag.MESSAGE, ctrlMessage, 0);
				Thread.sleep(500);
				if(NiagraServer.RJFM) System.err.println(this.getName() +" has returned from sendCtrlMsgUpStream.");
			}
		}
	}

	/**
	 * This function processes a punctuation element read from a source stream
	 * when the operator is non-blocking. This over-rides the corresponding
	 * function in the base class.
	 *
	 * Punctuations can simply be sent to the next operator from Select
	 *
	 * @param inputTuple The tuple element read from a source stream
	 * @param streamId The source stream from which the tuple was read
	 *
	 * @exception ShutdownException query shutdown by user or execution error
	 */
	protected void processPunctuation(Punctuation inputTuple,
			int streamId)
	throws ShutdownException, InterruptedException {
		putTuple(inputTuple, 0);
	}

	public boolean isStateful() {
		return false;
	}

	/**
	 * @see niagara.optimizer.colombia.PhysicalOp#FindLocalCost(ICatalog, LogicalProperty, LogicalProperty[])
	 */
	public Cost findLocalCost(
			ICatalog catalog,
			LogicalProperty[] InputLogProp) {
		float InputCard = InputLogProp[0].getCardinality();
		Cost cost = new Cost(InputCard * catalog.getDouble("tuple_reading_cost"));
		cost.add(predEval.getCost(catalog).times(InputCard));
		return cost;
	}

	public boolean equals(Object o) {
		if (o == null || !(o instanceof PhysicalSelectF))
			return false;
		if (o.getClass() != PhysicalSelectF.class)
			return o.equals(this);
		return pred.equals(
				((PhysicalSelectF) o).pred);
	}

	/**
	 * @see java.lang.Object#hashCode()
	 */
	public int hashCode() {
		return pred.hashCode();
	}

	/**
	 * @see niagara.optimizer.colombia.PhysicalOp#FindPhysProp(PhysicalProperty[])
	 */
	public PhysicalProperty findPhysProp(PhysicalProperty[] input_phys_props) {
		return input_phys_props[0];
	}

	/**
	 * @see niagara.query_engine.SchemaProducer#constructTupleSchema(TupleSchema[])
	 */
	public void constructTupleSchema(TupleSchema[] inputSchemas) {
		inputTupleSchemas = inputSchemas;
		outputTupleSchema = inputTupleSchemas[0];
	}

	/**
	 * @see niagara.query_engine.PhysicalOperator#opInitialize()
	 */
	protected void opInitialize() {
		predEval.resolveVariables(inputTupleSchemas[0], 0);
	}

	/**
	 * @see niagara.utils.SerializableToXML#dumpChildrenInXML(StringBuffer)
	 */
	public void dumpChildrenInXML(StringBuffer sb) {
		sb.append(">");
		pred.toXML(sb);
		sb.append("</").append(getName()).append(">");
	}
}



