/* $Id: PhysicalStore.java,v 1.1 2003/12/24 01:49:02 vpapad Exp $ */
package niagara.physical;

import niagara.utils.*;

import niagara.data_manager.DataManager;
import niagara.logical.Store;
import niagara.ndom.saxdom.DocumentImpl;
import niagara.ndom.saxdom.SAXDOMWriter;
import niagara.optimizer.colombia.*;

public class PhysicalStore extends PhysicalOperator {
    // XXX vpapad PhysicalStore should require its input to be produced by
    // SAXDOM

    private static final boolean[] blockingSourceStreams = { true };

    // Optimization-time attributes
    /** The attribute we're storing */
    private Attribute root;
    /** The resource name */
    private String resource;

    // Run-time variables
    /** The data manager */
    DataManager dm;

    /** Root attribute position */
    private int rootField;

    /** The SAXDOMWriter object used to store the data */
    private SAXDOMWriter sio;

    public PhysicalStore() {
        setBlockingSourceStreams(blockingSourceStreams);
    }

    /**
	 * Initializes from the appropriate logical operator
	 * 
	 * @param logicalOperator
	 *                  The logical operator that this operator implements
	 */
    public void opInitFrom(LogicalOp logicalOperator) {
        Store logicalStore = (Store) logicalOperator;
        this.root = logicalStore.getRoot();
        this.resource = logicalStore.getResource();
    }

    protected void blockingProcessTuple(Tuple inputTuple, int streamId)
        throws ShutdownException {
        if (sio == null)
            sio = new SAXDOMWriter(dm.getOutputStreamFor(resource));
        // Ignore partial tuples
        if (!inputTuple.isPartial())
            sio.writeDocument(
                (DocumentImpl) inputTuple.getAttribute(rootField));
    }

    public boolean isStateful() {
        return true;
    }

    public boolean equals(Object o) {
        if (o == null || !(o instanceof PhysicalStore))
            return false;
        if (o.getClass() != getClass())
            return o.equals(this);

        PhysicalStore op = (PhysicalStore) o;
        return (root.equals(op.root) && resource.equals(op.resource));
    }

    public int hashCode() {
        return root.hashCode() ^ resource.hashCode();
    }

    /**
	 * @see niagara.optimizer.colombia.PhysicalOp#FindLocalCost(ICatalog,
	 *          LogicalProperty, LogicalProperty[])
	 */
    public Cost findLocalCost(
        ICatalog catalog,
        LogicalProperty[] inputLogProp) {
        double inputCard = inputLogProp[0].getCardinality();
        return new Cost(inputCard * catalog.getDouble("document_storage_cost"));
    }

    /**
	 * @see niagara.optimizer.colombia.Op#copy()
	 */
    public Op opCopy() {
        PhysicalStore op = new PhysicalStore();
        op.root = root;
        op.resource = resource;
        return op;
    }

    /**
	 * @see niagara.query_engine.PhysicalOperator#opInitialize()
	 */
    protected void opInitialize() {
        rootField = inputTupleSchemas[0].getPosition(root.getName());
    }

    public void dumpAttributesInXML(StringBuffer sb) {
        sb.append(" root='").append(root.getName()).append("'");
        sb.append(" resource='").append(resource).append("'");
        super.dumpAttributesInXML(sb);
    }

    public void dataManagerInit(DataManager dm) {
        this.dm = dm;
    }

    protected void flushCurrentResults(boolean partial)
        throws ShutdownException {
        // XXX vpapad: Is this the right way to do this?
        // I want to flush only if/when everything is done and
        // no errors occured
        if (!partial) {
            sio.flush();
            dm.urnDone(resource);
        }
    }
}
