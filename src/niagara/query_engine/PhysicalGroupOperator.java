/**********************************************************************
  $Id: PhysicalGroupOperator.java,v 1.22 2003/03/19 00:36:09 tufte Exp $


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

package niagara.query_engine;

import java.util.*;

import niagara.optimizer.colombia.*;
import niagara.utils.*;
import niagara.xmlql_parser.op_tree.*;
import niagara.xmlql_parser.syntax_tree.*;

import org.w3c.dom.*;

/**
 * This is the <code>PhysicalGroupOperator</code> that extends
 * the basic PhysicalOperator with the implementation of the group
 * operator.
 *
 * @version 1.0
 *
 */

public abstract class PhysicalGroupOperator extends PhysicalOperator {
    // These are private nested classes used within the operator         

    /**
     * This is the class that stores the information in an entry of the hash
     * table
     */
    protected class HashEntry {
        // This is the object representing the final results
        private Object finalResult;

        // This is the object representing the partial results
        private Object partialResult;

        // This is the id of the currency of the partial results
        private int partialResultId;

        // This is a representative tuple of the hash entry
        StreamTupleElement representativeTuple;

        /**
         * This is the constructor of a hash entry that initialize it with
         * a representative tuple
         *
         * @param currPartialResultId The current id of partial results
         * @param representativeTuple A representative tuple for this hash entry
         */
        public HashEntry(
            int currPartialResultId,
            StreamTupleElement representativeTuple) {

            // Initialize the results to nothing
            finalResult = null;
            partialResult = null;

            // Initialize the partial result id
            this.partialResultId = currPartialResultId;

            // Initialize the representative tuple
            this.representativeTuple = representativeTuple;
        }

        /**
         * This function returns the final result associated with this entry
         *
         * @return The final result
         */

        public Object getFinalResult() {
            // Return the final result
            return finalResult;
        }

        /**
         * This function sets the final result of this entry
         *
         * @param finalResult The final result to be set
         */

        public void setFinalResult(Object finalResult) {
            this.finalResult = finalResult;
        }

        /**
         * This function returns the partial result associated with this entry
         *
         * @return The partial result
         */
        public Object getPartialResult() {
            return partialResult;
        }

        /**
         * This function sets the partial result of this entry
         *
         * @param partialResult The partial result to be set
         */

        public void setPartialResult(Object partialResult) {
            this.partialResult = partialResult;
        }

        /**
         * This function updates the partial result to make it consistent with the
         * current partial result id
         *
         * @param currPartialResultId The current partial result id of the operator
         */

        public void updatePartialResult(int currPartialResultId) {
            // If the stored partial id is less than the current partial id, then
            // clear the partial result and update the stored partial id
            if (partialResultId < currPartialResultId) {
                // Clear the partial results
                partialResult = null;

                // Update the stored partial result id
                partialResultId = currPartialResultId;
            }
        }

        /**
         * This function returns the representative tuple associated with
         * this hash entry
         *
         * @return The representative tuple associated with the hash entry
         */
        public StreamTupleElement getRepresentativeTuple() {
            // Return the representative tuple
            return representativeTuple;
        }
    }

    // source stream is blocking
    private static final boolean[] blockingSourceStreams = { true };

    // The logical operator for grouping
    protected groupOp logicalGroupOperator;

    // The list of attributes to group by
    protected Vector groupAttributeList;

    private Hasher hasher;

    // This is the hash table for performing grouping efficiently
    protected Hashtable hashtable;

    // Store the values that make up a hash key
    private String[] rgstPValues;
    private String[] rgstTValues;

    // This is the current partial id of the operator used to discard previous
    // partial results
    protected int currPartialResultId;

    protected Document doc;

    public PhysicalGroupOperator() {
        setBlockingSourceStreams(blockingSourceStreams);
    }

    public void initFrom(LogicalOp logicalOperator) {
        // Typecast to a group logical operator
        logicalGroupOperator = (groupOp) logicalOperator;
    }

    /**
     * This function initializes the data structures for an operator.
     * This over-rides the corresponding function in the base class.
     *
     * @return True if the operator is to continue and false otherwise
     */
    protected void opInitialize() {
        // Get the grouping attributes
        skolem grouping = logicalGroupOperator.getSkolemAttributes();
        groupAttributeList = grouping.getVarList();

        hasher = new Hasher(groupAttributeList);
        hasher.resolveVariables(inputTupleSchemas[0]);

        rgstPValues = new String[groupAttributeList.size()];
        rgstTValues = new String[groupAttributeList.size()];
        hashtable = new Hashtable();
        currPartialResultId = 0;

        // Ask subclasses to initialize
        this.initializeForExecution();
    }

    /**
     * This function processes a tuple element read from a source stream
     * when the operator is in a blocking state. This over-rides the
     * corresponding function in the base class.
     *
     * @param tupleElement The tuple element read from a source stream
     * @param streamId The source stream from which the tuple was read
     *
     * @exception ShutdownException query shutdown by user or execution error
     */
    protected void blockingProcessSourceTupleElement(     
	StreamTupleElement tupleElement,
        int streamId)
        throws ShutdownException {

        // First get the hash code for the grouping attributes
        String hashKey = hasher.hashKey(tupleElement);
        Object ungroupedResult = this.constructUngroupedResult(tupleElement);

        // Probe hash table to see whether result for this hashcode
        // already exist
        HashEntry prevResult = (HashEntry) hashtable.get(hashKey);

        if (prevResult == null) {
            // If it does not have the result, just create new one
            // with the current partial result id with the tupleElement
            // as the representative tuple
            prevResult = new HashEntry(currPartialResultId, tupleElement);

            // Add the entry to hash table
            hashtable.put(hashKey, prevResult);
        } else {
            // It did have the result - update partial results
            prevResult.updatePartialResult(currPartialResultId);
        }

        // Based on whether the tuple represents partial or final results
        // merge ungrouped result with previously grouped results
        if (tupleElement.isPartial()) {
            // Merge the partial result so far with current ungrouped result
            Object newPartialResult =
                this.mergeResults(
                    prevResult.getPartialResult(),
                    ungroupedResult);

            // Update the partial result
            prevResult.setPartialResult(newPartialResult);
        } else {

            // Merge the final result so far with current ungrouped result
            Object newFinalResult =
                this.mergeResults(prevResult.getFinalResult(), 
				  ungroupedResult);

            // Update the final result
            prevResult.setFinalResult(newFinalResult);
        }
    }

    /**
     * This function returns the current output of the operator. This
     * function is invoked only when the operator is blocking. This
     * over-rides the corresponding function in the base class.
     *
     * @param partial If this function call is due to a request for a
     *                partial result
     *
     * @return True if the operator is to continue and false otherwise
     */

    protected final void flushCurrentResults(boolean partial)
        throws InterruptedException, ShutdownException {
        // Get all the values in the hashtable and an iterator over the values
        Collection values = hashtable.values();
        Iterator iter = values.iterator();

        // If the iterator does not have any values, then call empty construct
        if (!iter.hasNext()) {
            Node emptyResult = constructEmptyResult();

            // If there is a non- empty result, then create tuple and add to
            // result
            if (emptyResult != null) {
                // Create tuple
                    StreamTupleElement tupleElement =
                        createTuple(
                            emptyResult,
                            null,
                // No representative tuple
    partial);

                // Add the tuple to the result
                putTuple(tupleElement, 0);
            }
            return;
        }

        // For each group, construct results
        //
        while (iter.hasNext()) {

            // Get the next element in the hash table
            //
            HashEntry hashEntry = (HashEntry) iter.next();

            // Update hash entry for partial results
            //
            hashEntry.updatePartialResult(currPartialResultId);

            // Get the result object if at least partial or final
            // result is not null
            //
            Object partialResult = hashEntry.getPartialResult();
            Object finalResult = hashEntry.getFinalResult();

            Node resultNode = null;

            if (partialResult != null || finalResult != null) {
                resultNode = this.constructResult(partialResult, finalResult);
            }

            // If there is a non- empty result, then create tuple and add to
            // result
            //
            if (resultNode != null) {

                // Create tuple
                //
                StreamTupleElement tupleElement =
                    createTuple(
                        resultNode,
                        hashEntry.getRepresentativeTuple(),
                        partial);

                // Add the tuple to the result
                putTuple(tupleElement, 0);
            }
        }
        return;
    }

    /**
     * This function handles punctuations for the given operator. The
     * group-by operator can unblock itself when a punctuation arrives that
     * matches a group (or groups). Groups that match can be output, and
     * their state can be purged.
     *
     * @param tuple The current input tuple to examine.
     * @param streamId The id of the source streams the partial result of
     *                 which are to be removed.
     *
     */

    protected void processPunctuation(
        StreamPunctuationElement inputTuple,
        int streamId)
        throws ShutdownException, InterruptedException {
        String stPunctGroupKey;
        try {
            stPunctGroupKey = hasher.hashKey(inputTuple);
        } catch (java.lang.ArrayIndexOutOfBoundsException ex) {
            //Not a punctuation for the group attribute. Ignore it.
            return;
        }

        Enumeration en = hashtable.keys();

        //see if the tuples we have match the incoming punctuation
        hasher.getValuesFromKey(stPunctGroupKey, rgstPValues);
        while (en.hasMoreElements()) {

            boolean fMatch = true;
            // Get the next element in the hash table
            //
            String key = (String) en.nextElement();

            hasher.getValuesFromKey(key, rgstTValues);
            for (int i = 0; i < rgstPValues.length && fMatch; i++) {
                fMatch =
                    StreamPunctuationElement.matchValue(
                        rgstPValues[i],
                        rgstTValues[i]);
            }

            if (fMatch) {
                // We found a group to output. Send its result on its way,
                // and remove it from the hashtable.

                // Get the result object if at least partial or final
                HashEntry hashEntry = (HashEntry) hashtable.get(key);
                Object finalResult = hashEntry.getFinalResult();
                Node resultNode = null;
                if (finalResult != null)
                    resultNode = this.constructResult(null, finalResult);

                // If there is a non- empty result, then create tuple and add
                // to result
                if (resultNode != null) {
                    StreamTupleElement tupleElement =
                        createTuple(
                            resultNode,
                            hashEntry.getRepresentativeTuple(),
                            false);

                    // Add the tuple to the result
                    putTuple(tupleElement, 0);
                }

                //Finally, remove this key from the hash table
                hashtable.remove(key);
            }
        }
    }

    /**
     * This function removes the effects of the partial results in a given
     * source stream. This over-rides the corresponding function in the
     * base class.
     *
     * @param streamId The id of the source streams the partial result of
     *                 which are to be removed.
     *
     */
    protected final void removeEffectsOfPartialResult(int streamId) {

        // Just increment the current partial id
        ++currPartialResultId;
    }

    /**
     * This function creates a group tuple given the grouped result
     *
     * @param groupedResult The grouped result
     * @param representativeTuple The representative tuple for the group
     * @param partial Whether the tuple is a partial or final result
     *
     * @return A tuple with the grouped result
     */

    protected StreamTupleElement createTuple(
        Node groupedResult,
        StreamTupleElement representativeTuple,
        boolean partial) {

        // Create a result tuple element tagged appropriately as
        // partial or final
        //
        StreamTupleElement tupleElement = new StreamTupleElement(partial);

        // NOTE: When grouping is implemented right at the logical operator tree
        // level, all that has to be done is to uncomment the following fragment
        // This ensures that the grouping attributes are part of the tuple result
        //

        // For each grouping attribute, add the corresponding element
        // to the result tuple from the representative tuple
        int numGroupingAttributes = groupAttributeList.size();

        for (int grp = 0; grp < numGroupingAttributes; ++grp) {
            // Get the group attribute
            Attribute a = (Attribute) groupAttributeList.get(grp);
            int attributeId = inputTupleSchemas[0].getPosition(a.getName());

            // Append the relevant attribute from the representative tuple
            // to the result
            if (representativeTuple != null)
                tupleElement.appendAttribute(
                    representativeTuple.getAttribute(attributeId));
            else
                tupleElement.appendAttribute(null);
        }

        // Add the grouped result as the attribute
        //
        tupleElement.appendAttribute(groupedResult);

        // Return the result tuple
        //
        return tupleElement;
    }

    public void setResultDocument(Document doc) {
        this.doc = doc;
    }

    /////////////////////////////////////////////////////////////////////////
    // These functions are the hooks that are used to implement specific   //
    // group operators                                                     //
    /////////////////////////////////////////////////////////////////////////

    /**
     * This function is called to initialize a grouping operator for execution
     * by setting up relevant structures etc.
     */

    protected abstract void initializeForExecution();

    /**
     * This function constructs a ungrouped result from a tuple
     *
     * @param tupleElement The tuple to construct the ungrouped result from
     *
     * @return The constructed object; If no object is constructed, returns
     *         null
     */

    protected abstract Object constructUngroupedResult(StreamTupleElement tupleElement) throws ShutdownException;

    /**
     * This function merges a grouped result with an ungrouped result
     *
     * @param groupedResult The grouped result that is to be modified (this can
     *                      be null)
     * @param ungroupedResult The ungrouped result that is to be grouped with
     *                        groupedResult (this can never be null)
     *
     * @return The new grouped result
     */

    protected abstract Object mergeResults(
        Object groupedResult,
        Object ungroupedResult);

    /**
     * This function returns an empty result in case there are no groups
     *
     * @return The result when there are no groups. Returns null if no
     *         result is to be constructed
     */

    protected abstract Node constructEmptyResult();

    /**
     * This function constructs a result from the grouped partial and final
     * results of a group. Both partial result and final result cannot be null.
     *
     * @param partialResult The partial results of the group (this can be null)
     * @param finalResult The final results of the group (this can be null)
     *
     * @return A results merging partial and final results; If no such result,
     *         returns null
     */

    protected abstract Node constructResult(
        Object partialResult,
        Object finalResult);

    public boolean isStateful() {
        return true;
    }

    public Cost findLocalCost(
        ICatalog catalog,
        LogicalProperty[] inputLogProp) {
        // XXX vpapad: really naive. Only considers the hashing cost
        float inpCard = inputLogProp[0].getCardinality();
        float outputCard = logProp.getCardinality();

        double cost = inpCard * catalog.getDouble("tuple_reading_cost");
        cost += inpCard * catalog.getDouble("tuple_hashing_cost");
        cost += outputCard * catalog.getDouble("tuple_construction_cost");
        return new Cost(cost);
    }
}
