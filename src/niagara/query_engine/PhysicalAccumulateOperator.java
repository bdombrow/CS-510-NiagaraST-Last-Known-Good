/* $Id: PhysicalAccumulateOperator.java,v 1.19 2003/03/03 08:20:13 tufte Exp $ */
package niagara.query_engine;

import org.w3c.dom.*;

import niagara.ndom.*;
import niagara.optimizer.colombia.*;

import org.xml.sax.*;
import java.io.*;

import niagara.ndom.*;
import niagara.utils.*;
import niagara.xmlql_parser.op_tree.*;
import niagara.data_manager.*;
import niagara.connection_server.NiagraServer;

/**
 * This is the <code>PhysicalAccumulateOperator </code> that extends
 * the basic PhysicalOperator with the implementation of the Accumulate
 * operator.
 *				
 * @author Kristin Tufte
 */
public class PhysicalAccumulateOperator extends PhysicalOperator {
    /*
      * Data members of the PhysicalAccumulateOperator Class
      */

    /*  
     * The array having information about blocking and non-blocking
     * streams.
     */

    private static final boolean[] blockingSourceStreams = { true };

    /* The merge tree  */
    private MergeTree mergeTree;
    private Attribute mergeAttr;
    private int mergeIndex;
    private String initialAccumFile;
    private String afName;
    private boolean clear;
    private int tupleCount;

    /* The object into which things are being accumulated */
    private Document accumDoc;
    private boolean recdData;

    public PhysicalAccumulateOperator() {
        setBlockingSourceStreams(blockingSourceStreams);
    }

    public void initFrom(LogicalOp logicalOperator) {
        // Type cast the logical operator to a Accumulate operator
        AccumulateOp logicalAccumulateOperator = (AccumulateOp) logicalOperator;

        mergeTree = logicalAccumulateOperator.getMergeTree();
        mergeAttr = logicalAccumulateOperator.getMergeAttr();
        initialAccumFile = logicalAccumulateOperator.getInitialAccumFile();
        afName = logicalAccumulateOperator.getAccumFileName();
        clear = logicalAccumulateOperator.getClear();
    }

    public void opInitialize() {
        if (clear && !initialAccumFile.equals("")) {
            createAccumulatorFromDisk(initialAccumFile);
        } else if (!afName.equals("") && CacheUtil.isAccumFile(afName)) {
            if (!clear) {
                createAccumulatorFromDoc(afName);
            } else {
                /* delete existing accumulated file and prepare
                 * to create a new one 
                 */
                DataManager.AccumFileDir.remove(afName);
                createEmptyAccumulator();
            }
        } else {
            /* create an empty accumulator */
            createEmptyAccumulator();
        }
	mergeTree.setAccumulator(accumDoc);
        recdData = false;
	tupleCount = 0;
    }

    /**
     * This function processes a tuple element read from a source stream
     * when the operator is blocking. This over-rides the corresponding
     * function in the base class.
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

	if(MergeTree.TRACE)
	    System.out.println("KT: Phys Accum Processing Tuple");

	if((tupleCount % 1000) == 0) {
	    Runtime r = Runtime.getRuntime();
	    System.out.println("KT: PhysAccum " + tupleCount + " tuples " +
			       " Free mem " + r.freeMemory() +
			       " Total mem " + r.totalMemory() +
			       " Used mem " + (r.totalMemory() -
					       r.freeMemory()));
	}

        /* get the fragment to be merged from the tuple, convert it
         * to an element if necessary, then pass the work off to the 
	 * merge tree
         */
        Element fragment = convertAttrToElement(tupleElement);

        // merge the fragment into the accumulated document
        mergeTree.accumulate(fragment);

        recdData = true;
	tupleCount++;
    }

    /**
     * This function removes the effects of the partial results in a given
     * source stream. This function over-rides the corresponding function
     * in the base class.
     *
     * @param streamId The id of the source streams the partial result of
     *                 which are to be removed.
     */

    protected void removeEffectsOfPartialResult(int streamId) {

        /* delete the results accumulated so far?? - what
         * about a combo of partial and final tuples coming in??
         * I think I should skip this one for now since I'm
         * aiming at an accumulate file implementation and
         * I don't know what a partial result is in the context
         * of an accumulate file
         */
        /* what if I just create a new accumDoc??  note we can
         * ignore multiple input stream issues since
         * accumulate is a unary operator 
         */
        createEmptyAccumulator();
    }

    protected void flushCurrentResults(boolean partial)
        throws ShutdownException, InterruptedException {

	System.out.println("KT PhysAccumOp flushCurrentResults called");

	if(NiagraServer.RUNNING_NIPROF) {
	    System.out.println("KT requesting data dump");
	    System.gc();
	    JProf.requestDataDump();
	}

        if (recdData == false) {
            System.out.println(
                "PhysAccumOp: WARNING - OUTPUTTING BEFORE DATA RECEIVED");
        }

        /* put the accumulated tree into the results */
        StreamTupleElement result = new StreamTupleElement(partial);

        /* think it is most intuitive to have the result of this
         * operator be a whole document - so scans on the
         * result work intuitively. KT was accumDoc.getDocumentElement()
         */
        result.appendAttribute(accumDoc);
        putTuple(result, 0);
    }

    private Element convertAttrToElement(StreamTupleElement tupleElement)
        throws ShutdownException {
        Object attr = tupleElement.getAttribute(mergeIndex);

        Element domElt;
        if (attr instanceof Element) {
            domElt = (Element) attr;
        } else if (attr instanceof Document) {
            domElt = ((Document) attr).getDocumentElement();

            /* Ok, this is really stupid, if there is a parsing problem,
             * I just get a null document element here (basically an
             * empty document). This is due to some brilliance in
             * the URLFetchThread - I don't understand why it has to
             * be done this way!!!!
             * So what I do is just throw an exception
             */
            if (domElt == null) {
                throw new ShutdownException("Got null document element - must mean there is a parsing problem with the fragment");
            }

        } else {
            throw new ShutdownException("Invalid instance type");
        }

        return domElt;
    }

    private void createEmptyAccumulator() {
        accumDoc = DOMFactory.newDocument();
	if(accumDoc == null)
	    throw new PEException("num accumulator");
        return;
    }

    private void createAccumulatorFromDoc(String afName) {
        accumDoc = (Document) DataManager.AccumFileDir.get(afName);
        if (accumDoc == null) {
            throw new PEException("AccumFileDir.get returned null in createAccumulatorFromDoc");
        }
        return;
    }

    private void createAccumulatorFromDisk(String initialAF) {

        try {
            niagara.ndom.DOMParser p = DOMFactory.newParser();

            /* Parse the initial accumulate	file */
            if (initialAF.startsWith("<?xml")) {
                p.parse(
                    new InputSource(
                        new ByteArrayInputStream(initialAF.getBytes())));
            } else {
                FileInputStream f = new FileInputStream(initialAF);
                p.parse(new InputSource(f));
            }

            accumDoc = p.getDocument();
            if (accumDoc.getDocumentElement() == null) {
                System.out.println("Doc elt null");
            }
            return;
        } catch (java.io.IOException e) {
            System.err.println(
                "Initial Accumulate File Corrupt - creating empty accumulator "
                    + e.getMessage());
            createEmptyAccumulator();
            return;
        } catch (org.xml.sax.SAXException e) {
            System.err.println(
                "Initial Accumulate File Corrupt - creating empty accumulator");
            createEmptyAccumulator();
            return;
        }
    }

    public boolean isStateful() {
        return true;
    }

    public String getAccumFileName() {
        return afName;
    }

    /**
     * @see niagara.optimizer.colombia.Op#copy()
     */
    public Op copy() {
        PhysicalAccumulateOperator op = new PhysicalAccumulateOperator();
        op.mergeTree = mergeTree;
        op.mergeAttr = mergeAttr;
        op.initialAccumFile = initialAccumFile;
        op.afName = afName;
        op.clear = clear;
        return op;
    }

    /**
     * @see java.lang.Object#equals(Object)
     */
    public boolean equals(Object o) {
        if (o == null || !(o instanceof PhysicalAccumulateOperator))
            return false;
        if (o.getClass() != PhysicalAccumulateOperator.class)
            return o.equals(this);
        PhysicalAccumulateOperator other = (PhysicalAccumulateOperator) o;
        return mergeTree.equals(other.mergeTree)
            && mergeAttr.equals(other.mergeAttr)
            && initialAccumFile.equals(other.initialAccumFile)
            && afName.equals(other.afName)
            && clear == other.clear;
    }

    /**
     * @see java.lang.Object#hashCode()
     */
    public int hashCode() {
        return mergeTree.hashCode()
            ^ mergeAttr.hashCode()
            ^ initialAccumFile.hashCode()
            ^ afName.hashCode()
            ^ (clear ? 0 : 1);
    }

    /**
     * @see niagara.query_engine.SchemaProducer#constructTupleSchema(TupleSchema[])
     */
    public void constructTupleSchema(TupleSchema[] inputSchemas) {
        inputTupleSchemas = inputSchemas;
        // There is no output schema!
        outputTupleSchema = new TupleSchema();
    }

    /**
     * @see niagara.optimizer.colombia.PhysicalOp#FindLocalCost(ICatalog, LogicalProperty[])
     */
    public Cost findLocalCost(
        ICatalog catalog,
        LogicalProperty[] InputLogProp) {
        // Assuming fixed cost per tuple
        float inputCard = InputLogProp[0].getCardinality();
        double cost =
            inputCard
                * (catalog.getDouble("tuple_reading_cost")
                    + catalog.getDouble("tuple_accumulation_cost"));
        return new Cost(cost);
    }
}
