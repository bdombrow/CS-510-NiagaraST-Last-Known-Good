/* $Id: PhysicalPredicatedUnnest.java,v 1.1 2003/12/24 01:49:03 vpapad Exp $ */
package niagara.physical;

import org.w3c.dom.*;
import java.util.ArrayList;
import niagara.utils.*;

import niagara.logical.Unnest;
import niagara.logical.Select;
import niagara.logical.path.RE;
import niagara.logical.predicates.Predicate;
import niagara.optimizer.colombia.*;
import niagara.physical.predicates.PredicateImpl;
import niagara.query_engine.*;

/** A physical operator for predicated unnest */
public class PhysicalPredicatedUnnest extends PhysicalOperator {
    // No blocking inputs
    private static final boolean[] blockingSourceStreams = { false };

    // Optimization-time attributes
    /**  The path expression to scan */
    private RE path;
    /** The attribute on which the scan is to be performed */
    private Attribute root;
    /** The resulting variable */
    private Attribute variable;
    /** Are we projecting attributes away? */
    private boolean projecting;
    /** Maps shared attribute positions between incoming and outgoing tuples */
    private int[] attributeMap;
    /** Predicate we apply to unnested node */
    private Predicate pred;

    // Runtime attributes
    private PathExprEvaluator pev;
    private NodeVector elementList;
    private int scanField;
    /** Runtime predicate implementation */
    private PredicateImpl predEval;

    public PhysicalPredicatedUnnest() {
        setBlockingSourceStreams(blockingSourceStreams);
    }

    /**
     * Initializes from the appropriate logical operator
     *
     * @param logicalOperator The logical operator that this operator implements
     */
    public void opInitFrom(LogicalOp logicalOperator) {
        if (logicalOperator instanceof Unnest) {
            Unnest logicalUnnest = (Unnest) logicalOperator;
            this.path = logicalUnnest.getPath();
            this.root = logicalUnnest.getRoot();
            this.variable = logicalUnnest.getVariable();
        } else {
            assert logicalOperator instanceof Select;
            Select logicalSelectOperator = (Select) logicalOperator;
            pred = logicalSelectOperator.getPredicate();
            // Make sure that the only referenced variable is 
            // the one we're unnesting
            ArrayList al = new ArrayList();
            pred.getReferencedVariables(al);
            if (al.size() != 1 || !al.get(0).equals(variable))
                throw new PEException("PredicatedUnnest can only evaluate predicates on the variable it's unnesting");
            predEval = pred.getImplementation();
        }
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

    protected void processTuple(
        Tuple inputTuple,
        int streamId)
        throws ShutdownException, InterruptedException {

        // Get the attribute to scan on
        Node attribute = inputTuple.getAttribute(scanField);

        // Get the nodes reachable using the path expression scanned
        pev.getMatches(attribute, elementList);

        int numNodes = elementList.size();

        if (numNodes == 0)
            return;

        int outSize = outputTupleSchema.getLength();

        for (int node = 0; node < numNodes; ++node) {
            Node n = elementList.get(node);
            if (!predEval.evaluate(n))
                continue;

            Tuple tuple;

            if (projecting) // We can project some attributes away
                tuple = inputTuple.copy(outSize, attributeMap);
            else // Just clone
                tuple = inputTuple.copy(outSize);

            // XXX vpapad: bug (or feature): Since PredicatedUnnest is 
            // a physical operator, projection pushing will not remove
            // attributes that are needed in the predicate, but not in
            // any operator above us.

            // Append a reachable node to the output tuple
            // and put the tuple in the output stream
            tuple.setAttribute(outSize - 1, n);

            putTuple(tuple, 0);
        }

        elementList.clear();
    }

    public boolean isStateful() {
        return false;
    }

    public boolean equals(Object o) {
        if (o == null || !(o instanceof PhysicalPredicatedUnnest))
            return false;
        if (o.getClass() != getClass())
            return o.equals(this);

        PhysicalPredicatedUnnest op = (PhysicalPredicatedUnnest) o;
        return (
            path.equals(op.path)
                && variable.equals(op.variable)
                && equalsNullsAllowed(getLogProp(), op.getLogProp())
                && pred.equals(op.pred));
    }

    public int hashCode() {
        return path.hashCode()
            ^ variable.hashCode()
            ^ hashCodeNullsAllowed(getLogProp())
            ^ pred.hashCode();
    }

    /**
     * @see niagara.optimizer.colombia.PhysicalOp#FindLocalCost(ICatalog, LogicalProperty, LogicalProperty[])
     */
    public Cost findLocalCost(
        ICatalog catalog,
        LogicalProperty[] InputLogProp) {
        double inputCard = InputLogProp[0].getCardinality();
        double outputCard = logProp.getCardinality();
        Cost cost =
            new Cost(
                inputCard * catalog.getDouble("tuple_reading_cost")
                    + outputCard * catalog.getDouble("dom_unnesting_cost")
                    + outputCard * constructTupleCost(catalog));
        cost.add(predEval.getCost(catalog).times(inputCard));
        return cost;
    }

    /**
     * @see niagara.optimizer.colombia.Op#copy()
     */
    public Op opCopy() {
        PhysicalPredicatedUnnest op = new PhysicalPredicatedUnnest();
        op.path = path;
        op.root = root;
        op.variable = variable;
        op.pred = pred;
        op.predEval = predEval;
        return op;
    }

    /**
     * @see niagara.query_engine.PhysicalOperator#opInitialize()
     */
    protected void opInitialize() {
        scanField = inputTupleSchemas[0].getPosition(root.getName());
        predEval.resolveVariables(outputTupleSchema, 0);

        pev = new PathExprEvaluator(path);
        elementList = new NodeVector();
    }

    /**
     * This function processes a punctuation element read from a source stream
     * when the operator is non-blocking. This over-rides the corresponding
     * function in the base class.
     *
     * Punctuations can simply be sent to the next operator from Scan
     *
     * @param inputTuple The tuple element read from a source stream
     * @param streamId The source stream from which the tuple was read
     *
     * @exception ShutdownException query shutdown by user or execution error
     */
    protected void processPunctuation(
        Punctuation inputTuple,
        int streamId)
        throws ShutdownException, InterruptedException {
        // XXX vpapad: Pete, I don't know how to modify this
        // to handle the extra predicate and projections - HELP!!!
        try {
            // Get the attribute to scan on
            Node attribute = inputTuple.getAttribute(scanField);

            // Get the nodes reachable using the path expression scanned
            pev.getMatches(attribute, elementList);

            // Append all the nodes returned to the inputTuple and add these
            // to the result
            int numNodes = elementList.size();

            if (numNodes != 0) {
                for (int node = 0; node < numNodes; ++node) {
                    // Clone the input tuple to create an output tuple
                    // Append a reachable node to the output tuple
                    // and put the tuple in the output stream
                    Punctuation outputTuple =
                        (Punctuation) inputTuple.clone();
                    outputTuple.appendAttribute(elementList.get(node));
                    putTuple(outputTuple, 0);
                }
            } else {
                //I still want the punctuation to live on, even if it doesn't
                // have the element we're scanning for.
                putTuple(inputTuple, streamId);
            }
            elementList.clear();
        } catch (java.lang.ArrayIndexOutOfBoundsException ex) {
            //the scan field doesn't exist for this punctuation. We
            // still want the tuple to live on.
            putTuple(inputTuple, streamId);
        }
    }

    public void constructTupleSchema(TupleSchema[] inputSchemas) {
        super.constructTupleSchema(inputSchemas);
        // Without projection, (length of output tuple) = (length of input tuple + 1)
        projecting =
            (inputSchemas[0].getLength() + 1 > outputTupleSchema.getLength());
        if (projecting)
            attributeMap = inputSchemas[0].mapPositions(outputTupleSchema);
    }

    /**
     * @see niagara.utils.SerializableToXML#dumpChildrenInXML(StringBuffer)
     */
    public void dumpChildrenInXML(StringBuffer sb) {
        sb.append(">");
        pred.toXML(sb);
        sb.append("</").append(getName()).append(">");
    }

    public void dumpAttributesInXML(StringBuffer sb) {
        sb.append(" variable='").append(variable.getName()).append("'");
        super.dumpAttributesInXML(sb);
    }
}
