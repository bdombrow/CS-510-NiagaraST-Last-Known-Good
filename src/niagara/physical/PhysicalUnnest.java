/* $Id: PhysicalUnnest.java,v 1.2 2004/05/20 22:10:22 vpapad Exp $ */
package niagara.physical;

import org.w3c.dom.*;
import niagara.utils.*;
import niagara.logical.path.RE;

import niagara.logical.Unnest;
import niagara.optimizer.colombia.*;
import niagara.query_engine.*;

public class PhysicalUnnest extends PhysicalOperator implements NodeConsumer {
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
    /** indicates if tuples should be dropped or kept if they do not
     * match path expression
     */
    private boolean outer;

    // Runtime attributes
    protected PathExprEvaluator pev;
    protected NodeVector elementList;
    protected int scanField;
    //Current input tuple
    private Tuple inputTuple;
    private boolean matchFound;
    /** Are we really adding a new attribute to the output tuple?*/
    private boolean reallyUnnesting;
    /** Position of new attribute in the output schema */
    private int outputPos;

    public PhysicalUnnest() {
        setBlockingSourceStreams(blockingSourceStreams);
    }

    /**
     * Initializes from the appropriate logical operator
     *
     * @param logicalOperator The logical operator that this operator implements
     */
    public void opInitFrom(LogicalOp logicalOperator) {
        Unnest logicalUnnest = (Unnest) logicalOperator;
        this.path = logicalUnnest.getPath();
        this.root = logicalUnnest.getRoot();
        this.variable = logicalUnnest.getVariable();
        this.outer = logicalUnnest.outer();
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
        this.inputTuple = inputTuple;
        matchFound = false;

        // Get the nodes reachable from the path expression 
        pev.produceMatches(inputTuple.getAttribute(scanField), this);

        if (outer && !matchFound)
            consume(null);

        this.inputTuple = null;
    }

    public void consume(Node n)
        throws InterruptedException, ShutdownException {
        int outSize = outputTupleSchema.getLength();

        if (!matchFound && projecting)
            // We can project some attributes away
            inputTuple = inputTuple.copy(outSize, attributeMap);
        else // Just clone
            inputTuple = inputTuple.copy(outSize);

        // Append a reachable node to the output tuple
        // and put the tuple in the output stream
        if (reallyUnnesting)
            inputTuple.setAttribute(outputPos, n);
        putTuple(inputTuple, 0);
        matchFound = true;
    }

    public boolean isStateful() {
        return false;
    }

    public boolean equals(Object o) {
        if (o == null || !(o instanceof PhysicalUnnest))
            return false;
        if (o.getClass() != getClass())
            return o.equals(this);

        PhysicalUnnest op = (PhysicalUnnest) o;
        return (path.equals(op.path) && variable.equals(op.variable))
            && root.equals(op.root) && outer == op.outer;
    }

    public int hashCode() {
        return path.hashCode()
            ^ variable.hashCode()
            ^ root.hashCode() 
            ^ (outer ? 0 : 1);
    }

    /**
     * @see niagara.optimizer.colombia.PhysicalOp#FindLocalCost(ICatalog, LogicalProperty, LogicalProperty[])
     */
    public Cost findLocalCost(
        ICatalog catalog,
        LogicalProperty[] InputLogProp) {
        double inputCard = InputLogProp[0].getCardinality();
        double outputCard = logProp.getCardinality();
        return new Cost(
            inputCard * catalog.getDouble("tuple_reading_cost")
                + outputCard * catalog.getDouble("dom_unnesting_cost")
                + outputCard * constructTupleCost(catalog));
    }

    /**
     * @see niagara.optimizer.colombia.Op#copy()
     */
    public Op opCopy() {
        PhysicalUnnest op = new PhysicalUnnest();
        op.path = path;
        op.root = root;
        op.variable = variable;
        op.outer = outer;
        return op;
    }

    /**
     * @see niagara.query_engine.PhysicalOperator#opInitialize()
     */
    protected void opInitialize() {
        scanField = inputTupleSchemas[0].getPosition(root.getName());
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
        // to handle projections - HELP
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
        // Without projection, (length of output tuple) < (length of input tuple + 1)
        projecting =
            (inputSchemas[0].getLength() + 1 > outputTupleSchema.getLength());
        if (projecting)
            attributeMap = inputSchemas[0].mapPositions(outputTupleSchema);
        reallyUnnesting = outputTupleSchema.contains(variable.getName());
        if (reallyUnnesting)
            outputPos = outputTupleSchema.getPosition(variable.getName());
    }

    public void dumpAttributesInXML(StringBuffer sb) {
        sb.append(" variable='").append(variable.getName()).append("'");
        super.dumpAttributesInXML(sb);
    }

}
