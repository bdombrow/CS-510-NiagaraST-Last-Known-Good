/**********************************************************************
  $Id: PhysicalSortOperator.java,v 1.7 2003/07/03 19:56:52 tufte Exp $


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

import org.w3c.dom.*;

import java.util.*;

import niagara.logical.OldVariable;
import niagara.optimizer.colombia.*;
import niagara.utils.*;
import niagara.xmlql_parser.op_tree.*;
import niagara.xmlql_parser.syntax_tree.*;

/**
 * The <code>PhysicalSortOperator</code> class is derived from the abstract class
 * <code>PhysicalOperator</code>. It implements a Sort on a incoming tuple,
 * producing a new wider outgoing tuple.
 *
 */

public class PhysicalSortOperator extends PhysicalOperator {
    // Sort is blocking on its input stream
    private static final boolean[] blockingSourceStreams = { true };

    // A container that keeps itself sorted
    private TreeSet ts;
    // To distinguish between "equal" tuples. This is an ugly hack
    // what we really want here is a "sorted bag" implementation
    private int incomingTupleOrder;

    private Attribute sortingField;
    private short comparisonMethod;
    private boolean ascending;

    private class BoxedElement {
        StreamTupleElement ste;
        int order_id;
        public BoxedElement(StreamTupleElement ste, int order_id) {
            this.ste = ste;
            this.order_id = order_id;
        }
        public StreamTupleElement getSTE() {
            return ste;
        }
        public Integer getOrder() {
            return new Integer(order_id);
        }
    }

    public PhysicalSortOperator() {
        setBlockingSourceStreams(blockingSourceStreams);
    }

    public void opInitFrom(LogicalOp logicalOperator) {
        // Type cast the logical operator to a Sort operator
        SortOp logicalSortOperator = (SortOp) logicalOperator;
        sortingField = logicalSortOperator.getAttr();
        comparisonMethod = logicalSortOperator.getComparisonMethod();
        ascending = logicalSortOperator.getAscending();
    }

    /**
     * This function processes a tuple element read from a source stream
     * when the operator is non-blocking. This over-rides the corresponding
     * function in the base class.
     *
     * @param inputTuple The tuple element read from a source stream
     * @param streamId The source stream from which the tuple was read
     * @param result The result is to be filled with tuples to be sent
     *               to sink streams
     *
     * @return true if the operator is to continue and false otherwise
     */

    protected void blockingProcessSourceTupleElement(
        StreamTupleElement inputTuple,
        int streamId) {
        ts.add(new BoxedElement(inputTuple, incomingTupleOrder++));
    }

    protected void removeEffectsOfPartialResult(int streamId) {
        Iterator iter = ts.iterator();
        while (iter.hasNext()) {
            StreamTupleElement ste = ((BoxedElement) iter.next()).getSTE();
            if (ste.isPartial())
                iter.remove();
        }
    }

    protected void flushCurrentResults(boolean partial)
        throws ShutdownException, InterruptedException {
        Iterator iter = ts.iterator();
        while (iter.hasNext()) {
            putTuple(((BoxedElement) iter.next()).getSTE(), 0);
        }
    }

    class SortComparator implements Comparator {
        int sortingField;
        short comparisonMethod;
        boolean ascending;

        public SortComparator(
            int sortingField,
            short comparisonMethod,
            boolean ascending) {
            this.sortingField = sortingField;
            this.comparisonMethod = comparisonMethod;
            this.ascending = ascending;
        }

        public String stringValue(Object o) {
            if (o instanceof Element) {
                Element e = (Element) o;
                e.normalize();
                Node firstchild = e.getFirstChild();
                if (firstchild instanceof Text)
                    return ((Text) firstchild).getData();
                else
                    return stringValue(firstchild);
            } else { // Text
                return ((Text) o).getData();
            }
        }

        public double numValue(Object o) {
            try {
                return Double.parseDouble(stringValue(o));
            } catch (NumberFormatException e) {
                return 0d;
            }
        }

        public int compare(Object o1, Object o2) {
            Object obj1, obj2, v1, v2;
            StreamTupleElement e1 = ((BoxedElement) o1).getSTE();
            StreamTupleElement e2 = ((BoxedElement) o2).getSTE();

            if (ascending) {
                obj1 = e1.getAttribute(sortingField);
                obj2 = e2.getAttribute(sortingField);
            } else {
                obj1 = e2.getAttribute(sortingField);
                obj2 = e1.getAttribute(sortingField);
            }

            if (comparisonMethod == SortOp.NUMERIC_COMPARISON) {
                v1 = new Double(numValue(obj1));
                v2 = new Double(numValue(obj2));
            } else {
                v1 = stringValue(obj1);
                v2 = stringValue(obj2);
            }
            int return_value = ((Comparable) v1).compareTo(v2);
            // If the two elements are equal, we'll sort them on incoming order
            if (return_value == 0) {
                return_value =
                    ((BoxedElement) o1).getOrder().compareTo(
                        ((BoxedElement) o2).getOrder());
            }
            return return_value;
        }
    }

    public boolean isStateful() {
        return true;
    }

    public PhysicalProperty[] inputReqdProp(
        PhysicalProperty PhysProp,
        LogicalProperty InputLogProp,
        int InputNo) {
        if (PhysProp.equals(PhysicalProperty.ANY)) {
            return new PhysicalProperty[] {
            };
        }

        // XXX vpapad: i don't like this
        if (PhysProp.getOrder().isSorted()) {
            if ((InputLogProp).Contains(PhysProp.getOrderAttrNames()))
                return new PhysicalProperty[] {
            };
            else
                throw new PEException("Cannot sort on invisible attributes");
        } else
            throw new PEException("Cannot handle anything other than sorted");
    }

    /**
     * @see niagara.optimizer.colombia.PhysicalOp#FindLocalCost(ICatalog, LogicalProperty, LogicalProperty[])
     */
    public Cost findLocalCost(
        ICatalog catalog,
        LogicalProperty[] InputLogProp) {
        double InputCard = InputLogProp[0].getCardinality();
        return new Cost(
            InputCard * catalog.getDouble("tuple_reading_cost")
                + InputCard
                    * Math.log(InputCard)
                    * catalog.getDouble("tuple_hashing_cost"));
    }

    /**
     * @see niagara.query_engine.PhysicalOperator#opInitialize()
     */
    protected void opInitialize() {
        int sortingFieldPos =
            inputTupleSchemas[0].getPosition(sortingField.getName());
        // Create an appropriate Comparator object and TreeSet
        ts =
            new TreeSet(
                new SortComparator(
                    sortingFieldPos,
                    comparisonMethod,
                    ascending));
        incomingTupleOrder = 0;
    }

    public void constructTupleSchema(TupleSchema[] inputSchemas) {
        inputTupleSchemas = inputSchemas;
        outputTupleSchema = inputSchemas[0].copy();
    }
    /**
     * @see niagara.optimizer.colombia.Op#copy()
     */
    public Op opCopy() {
        PhysicalSortOperator op = new PhysicalSortOperator();
        op.sortingField = sortingField;
        op.comparisonMethod = comparisonMethod;
        op.ascending = ascending;
        return op;
    }

    public boolean equals(Object o) {
        if (o == null || !(o instanceof PhysicalSortOperator))
            return false;
        if (o.getClass() != PhysicalSortOperator.class)
            return o.equals(this);

        PhysicalSortOperator op = (PhysicalSortOperator) o;
        return sortingField.equals(op.sortingField)
            && comparisonMethod == op.comparisonMethod
            && ascending == op.ascending;
    }

    /**
     * @see java.lang.Object#hashCode()
     */
    public int hashCode() {
        return sortingField.hashCode() ^ comparisonMethod ^ (ascending ? 1 : 0);
    }
    
    /**
     * @see niagara.optimizer.colombia.PhysicalOp#findPhysProp(PhysicalProperty[])
     */
    public PhysicalProperty findPhysProp(PhysicalProperty[] input_phys_props) {
        Strings orderby = new Strings();
        orderby.add(sortingField.getName());
        return new PhysicalProperty(new Order(orderby));
    }
}
