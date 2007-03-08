/**********************************************************************
  $Id: PhysicalPunctQC.java,v 1.1 2007/03/08 22:34:29 tufte Exp $


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

import java.util.ArrayList;
import java.util.Vector;

import org.w3c.dom.*;

import niagara.logical.PunctQC;
import niagara.logical.PunctSpec;
import niagara.logical.PrefetchSpec;
import niagara.logical.SimilaritySpec;
import niagara.optimizer.colombia.*;
import niagara.query_engine.*;
import niagara.utils.*;

/**
 * <code>PhysicalPunctuateOperator</code> implements a punctuate operator for
 * an incoming stream;
 *
 * @see PhysicalOperator
 */
public class PhysicalPunctQC extends PhysicalOperator {
  
    // operator does not block on any source streams
    private static final boolean[] blockingSourceStreams = { false, false };

    // two input streams - one is data from db
    // second is punctuation from stream
    // assume index of dbdata is 0
    // index of punct is 1
    
		// attribute to punctuate on
    private Attribute pAttr;
		private PunctSpec pSpec;
    private int pIdx; // index of punctuation attr
    private SimilaritySpec sSpec;
    private PrefetchSpec pfSpec;

		// last timestamp - for on change punctuation
		private long lastts;

    // keep track of data types to be used to create
    // punctuation
    BaseAttr.Type dataType[];      
    
    private boolean dataStreamClosed = false;
    
    public PhysicalPunctQC() {
        setBlockingSourceStreams(blockingSourceStreams);
    }

    // probably should take punct index instead
    // index vs attr -get attr in load from xml, probably
    // can't get index there
    // probably take attr here, and convert to idx in
    // opInitialize
    public PhysicalPunctQC(Attribute pAttr,
				PunctSpec pSpec, SimilaritySpec sSpec,
        PrefetchSpec pfSpec) {
        setBlockingSourceStreams(blockingSourceStreams);

				this.pAttr = pAttr;
				this.pSpec = pSpec;
				this.sSpec = sSpec;
				this.pfSpec = pfSpec;
    }
    
    public void opInitFrom(LogicalOp logicalOperator) {
				PunctQC pop = (PunctQC) logicalOperator;
				pAttr = pop.getPunctAttr();
				pSpec = pop.getPunctSpec();
				sSpec = pop.getSimilaritySpec();
				pfSpec = pop.getPrefetchSpec();
    }

    public void opInitialize() {
        // initialize ts to -1 so we can detect the first
        // tuple 
				lastts= -1;

        // get index of attribute we are punctuating on
        pIdx = inputTupleSchemas[0].getPosition(pAttr.getName());

        // get the data types of all attributes
        int numAttrs = inputTupleSchemas[0].getLength();
        dataType = new BaseAttr.Type[numAttrs];
        for(int i = 0; i<numAttrs; i++) {
          dataType[i] = inputTupleSchemas[0].getVariable(i).getDataType();
        }
       
        // print to verify i've got the inputs set up right
        System.out.println("PunctQC - pidx: " + pIdx);
        System.out.println("PunctQC - len input 0 (db): " + inputTupleSchemas[0].getLength());
        System.out.println("PunctQC - first attr input 0: " + inputTupleSchemas[0].getVariable(0).getName());
        System.out.println("PunctQC - len input 1 (stream punct): " + inputTupleSchemas[1].getLength());
        System.out.println("PunctQC - first attr input 1: " + inputTupleSchemas[1].getVariable(0).getName());
    }

    /**
     * This function processes a tuple element read from a source stream
     * when the operator is non-blocking. This over-rides the corresponding
     * function in the base class.
     *
     * @param inputTuple The tuple element read from a source stream
     * @param streamId The source stream from which the tuple was read
     *
     * @exception ShutdownException query shutdown by user or execution error
     */
    protected void processTuple (
						 Tuple inputTuple,
						 int streamId)
						 
			throws ShutdownException, InterruptedException, OperatorDoneException {

      if(streamId == 1) {
        System.out.println("PunctQC dropping data from stream side");
        return;
      }
			
			// could enforce punctuation here - for now assume
			// input is ordered - jenny is going to hate me when
			// she sees this!!

			if(lastts == -1) {
				// this is the first tuple
				// record the tuple's timestamp 
				// grab a copy of the tuple for a punctuation
				// template
				// put the tuple in the output stream and continue
				lastts = getTupleTimestamp(inputTuple);
	      //tupleDataSample = inputTuple.copy();
				putTuple(inputTuple, 0);
				
			} else {
				// compare tuple timestamp to see if it has changed
				// if changed put punct, then tuple
				// else just put tuple
				long newts = getTupleTimestamp(inputTuple);
				if(newts != lastts) {
					putTuple(createPunctuation(), 0);
					lastts = newts;
				} 
				putTuple(inputTuple,0);
			}
		}

	  private long getTupleTimestamp(Tuple inputTuple){
      assert (inputTuple.getAttribute(pIdx)).getClass() == TSAttr.class : "bad punct attr type";
      TSAttr tsAttr = (TSAttr)inputTuple.getAttribute(pIdx);
      return tsAttr.extractEpoch();
    }


    /**
     * This function generates a punctuation based on the last ts value
     * using the template generated by setupDataTemplate
     */
    private Punctuation createPunctuation() {
	    // Create a punctuation based on the last timestamp value
      // punct should be last ts value plus *s for all other
      // attrs
      assert lastts > 0 : "uh-oh negative ts value!! KT ";

      // HERE - FIX - DON'T KNOW HOW TO CREATE NEW * ATTRS
      Punctuation punct = new Punctuation(false); // what does false mean?
      for (int i=0; i<dataType.length; i++) { 
        if (i != pIdx) {
          punct.appendAttribute(BaseAttr.createWildStar(dataType[i])); 
        } else {
          punct.appendAttribute(new TSAttr(lastts));	// ???
        }
      }
      return punct;
	  }


    /**
     * This function handles punctuations for the given operator. For
     * Punctuate, we can simply output any incoming punctuation.
     *
     * @param tuple The current input tuple to examine.
     * @param streamId The id of the source streams the partial result of
     *                 which are to be removed.
     *
     */

    protected void processPunctuation(Punctuation tuple,
				      int streamId)
      throws ShutdownException, InterruptedException {
        // FIX - process punct from stream
        assert streamId == 1 : "Shouldn't get punct from db side";
        System.out.println("DO SOMETHING HERE - PROCESS PUNCT FROM DB SIDE");
    }

    public boolean isStateful() {
      return false;
    }
    
    /**
     * @see niagara.optimizer.colombia.PhysicalOp#FindLocalCost(LogicalProperty, LogicalProperty[])
     */
    public Cost findLocalCost(ICatalog catalog, LogicalProperty[] inputLogProp) {
        double trc = catalog.getDouble("tuple_reading_cost");
        double sumCards = 0;
        for (int i = 0; i < inputLogProp.length; i++)
            sumCards += inputLogProp[i].getCardinality();
        return new Cost(trc * sumCards);
    }

    /**
     * @see niagara.optimizer.colombia.Op#copy()
     */
    public Op opCopy() {
        return new  PhysicalPunctQC(pAttr, pSpec, sSpec, pfSpec);
    }

    // HERE
    /**
     * @see niagara.query_engine.SchemaProducer#constructTupleSchema(TupleSchema[])
     */
    public void constructTupleSchema(TupleSchema[] inputSchemas) {
      inputTupleSchemas = inputSchemas;
      outputTupleSchema = inputTupleSchemas[0];
    }

    /**
     * @see java.lang.Object#equals(Object)
     */
    public boolean equals(Object o) {
      // FIX - this is garbage
        if (o == null || !(o instanceof PhysicalPunctQC))
            return false;
        if (o.getClass() != PhysicalPunctQC.class)
            return o.equals(this);
        return getArity() == ((PhysicalPunctuate) o).getArity();
    }

    public int hashCode() {
      // FIX - this is garbage
        return getArity(); // what is this ?? 
    }
    
}
    
