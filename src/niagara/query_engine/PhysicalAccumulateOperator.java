/* $Id: PhysicalAccumulateOperator.java,v 1.13 2002/10/27 03:08:04 vpapad Exp $ */
package niagara.query_engine;

import java.util.Vector;
import org.w3c.dom.*;

import niagara.ndom.*;
import niagara.optimizer.colombia.Attribute;
import niagara.optimizer.colombia.LogicalOp;

import org.xml.sax.*;
import java.io.*;

import niagara.ndom.*;
import niagara.utils.*;
import niagara.xmlql_parser.op_tree.*;
import niagara.xmlql_parser.syntax_tree.*;
import niagara.data_manager.*;

/**
 * This is the <code>PhysicalAccumulateOperator </code> that extends
 * the basic PhysicalOperator with the implementation of the Accumulate
 * operator.
 *				
 * @version 1.0
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
    
    /* The object into which things are being accumulated */
    private Document accumDoc;
    private boolean recdData;


    public PhysicalAccumulateOperator() {
        setBlockingSourceStreams(blockingSourceStreams);
    }
    
    public void initFrom(LogicalOp logicalOperator) {
	// Type cast the logical operator to a Accumulate operator
	AccumulateOp logicalAccumulateOperator = 
	    (AccumulateOp) logicalOperator;

	mergeTree = logicalAccumulateOperator.getMergeTree();
	mergeAttr = logicalAccumulateOperator.getMergeAttr();

	initialAccumFile = logicalAccumulateOperator.getInitialAccumFile();
	afName = logicalAccumulateOperator.getAccumFileName();

	clear = logicalAccumulateOperator.getClear();
    }

    public void opInitialize() {
	if(clear && !initialAccumFile.equals("")) {
	    createAccumulatorFromDisk(initialAccumFile);
	} else if(!afName.equals("") && CacheUtil.isAccumFile(afName)) {
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

	recdData = false;
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

    protected void blockingProcessSourceTupleElement (
					StreamTupleElement tupleElement,
							 int streamId) 
	throws ShutdownException, UserErrorException {

	/* get the fragment to be merged from the tuple, convert it
	 * to an element if necessary, then pass the work off to the merge tree
	 */
	Element fragment = convertAttrToElement(tupleElement);
	
	// merge the fragment into the accumulated document
	mergeTree.accumulate(accumDoc, fragment);
	
	recdData = true;
    }

    
    /**
     * This function removes the effects of the partial results in a given
     * source stream. This function over-rides the corresponding function
     * in the base class.
     *
     * @param streamId The id of the source streams the partial result of
     *                 which are to be removed.
     */

    protected void removeEffectsOfPartialResult (int streamId) {

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
	throws ShutdownException, InterruptedException{
	if(recdData == false) {
	    System.out.println("PhysAccumOp: WARNING - OUTPUTTING BEFORE DATA RECEIVED");
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
	throws UserErrorException {
	Object attr = tupleElement.getAttribute(mergeIndex);

	Element domElt;
	Document domDoc;
	if(attr instanceof Element) {
	    domElt = (Element)attr;
	} else if (attr instanceof Document) {
	    domElt = ((Document)attr).getDocumentElement();

	    /* Ok, this is really stupid, if there is a parsing problem,
	     * I just get a null document element here (basically an
	     * empty document). This is due to some brilliance in
	     * the URLFetchThread - I don't understand why it has to
	     * be done this way!!!!
	     * So what I do is just throw an exception
	     */
	    if(domElt == null) {
		throw new UserErrorException("Got null document element - must mean there is a parsing problem with the fragment");
	    }

	} else {
	    throw new UserErrorException("Invalid instance type");
	}
	
	return domElt;
    }

    private void createEmptyAccumulator()  {
	accumDoc = DOMFactory.newDocument();
	/* MTND Document domDoc = DOMFactory.newDocument(); 
	 * accumDoc.initialize(mapTable, domDoc); 
	 */
	return;
    }

    private void createAccumulatorFromDoc(String afName) {
	accumDoc = (Document)DataManager.AccumFileDir.get(afName);

	if(accumDoc == null) {
	    throw new PEException("AccumFileDir.get returned null in createAccumulatorFromDoc");
	}

	/* MTND - don't think I need any of this
	 *	accumDoc = DOMFactory.newDocument();
	 * accumDoc.initialize(mapTable, accumDomDoc);

	* now clone the accum doc itself so I can write to it 
	 * code always assumes that document itself has been
	 * cloned - this clone is not deep - just clones
	 * the document and references the document element
	 *

	 *accumDoc = accumDoc.cloneDocRefDocElt(false);
	 */

	return;
    }

    private void createAccumulatorFromDisk(String initialAF) {

	try {
	    niagara.ndom.DOMParser p = DOMFactory.newParser();
	    
	    /* Parse the initial accumulate	file */
	    if(initialAF.startsWith("<?xml")) {
		p.parse(new InputSource(new ByteArrayInputStream(initialAF.getBytes())));
	    } else { 
		FileInputStream f = new FileInputStream(initialAF);
		p.parse(new InputSource(f));
	    } 
	
	    accumDoc = p.getDocument(); 
	    if(accumDoc.getDocumentElement() == null) {
		System.out.println("Doc elt null");
	    } 

	    /* MTND - dont need 
	     * accumDoc = DOMFactory.newDocument();
	     * accumDoc.initialize(mapTable, accumDomDoc);
	     *
	     * now clone the accum doc itself so I can write to it 
	     * code always assumes that document itself has been
	     * cloned - this clone is not deep - just clones
	     * the document and references the document element
	     *
	     * accumDoc = accumDoc.cloneDocRefDocElt(false);
	     * System.out.println("createAccum done");
	    */

	    return;
	} catch (java.io.IOException e) {
	    System.err.println("Initial Accumulate File Corrupt - creating empty accumulator " + e.getMessage());
	    createEmptyAccumulator();
	    return;
	} catch (org.xml.sax.SAXException e) {
	    System.err.println("Initial Accumulate File Corrupt - creating empty accumulator");
	    createEmptyAccumulator();
	    return;
	}
    }

    public boolean isStateful() {
	return true;
    }

}



