/*
 * $RCSfile:
 * $Revision:
 * $Date:
 * $Author:
 */

package niagara.query_engine;

/**
 * Implementation of a <code> MergeTree </code> class which
 * merges two XML documents together.  The merge tree
 * is constructed of MergeTreeNodes.  MergeTree is a wrapper for the
 * tree constructed of MergeTreeNodes and also handles the treewalking.
 * MergeTreeNodes make up the tree structure and the Merge and Match
 * objects which are members of each MergeTreeNode, do the real merging
 * and matching work.
 *
 * @version 1.0
 *
 * @author Kristin Tufte
 */

import java.util.*;
import org.w3c.dom.*;
import org.xml.sax.*;
import java.io.*;
import niagara.utils.*;
import niagara.ndom.*;

public class MergeTree {
    
    /*
     * DATA MEMBERS
     */
    private MergeTreeNode mergeTreeRoot;
    private boolean       validForAccumulate;
    private int           treeMergeType;
    private int           treeDomSide;
    private HashMap       rootedKeyMap;
    private HashSet       tagList;
    private ArrayStack    tempArrayStacks;
    
    /* values for MergeType and DominantSide */
    public static final int MT_OUTER = 1;
    public static final int MT_LEFTOUTER = 2;
    public static final int MT_RIGHTOUTER = 3;
    public static final int MT_INNER = 4;
    
    public static final int DS_LEFT = 1;
    public static final int DS_RIGHT = 2;
    public static final int DS_EXACTMATCH = 3;

    public static final String DEF_ATTR_STRING = "ATTR_DEFAULT";

    private boolean TRACE = false;

    /*
     * METHODS
     */
    
   /**
     * Constructor 
     *				
     */
    
    public MergeTree() { 
	reset();
	tempArrayStacks = new ArrayStack();
    }

   /**
     * Creates the MergeTree instance from a string.  This function
     * is responsible for parsing the string appropriately and
     * then calling the create function which takds a DOM document
     * as an argument.
     *
     * @param mergeTemplate String representing the XML merge template.
     *       Can be a string containing the merge template, a file name,
     *          or a URI
     * @param checkAccumConstraints true/false depending on whether you
     *        want create to verify that this tree can be used
     *
     * @return Returns nothing.
     */    
    public void create(String mergeTemplateStr, boolean checkAccumConstraints) 
	throws MTException {

	try {

	    niagara.ndom.DOMParser p = DOMFactory.newParser();
	    Document mergeTemplate = null;
	    
	    /* Parse the mergeTemplate file or string */
	    if(mergeTemplateStr.startsWith("<?xml")) {
		p.parse(new InputSource(new ByteArrayInputStream(mergeTemplateStr.getBytes())));
	    } else {
		p.parse(new InputSource(mergeTemplateStr));
	    }
	    create(p.getDocument(), checkAccumConstraints);
	} catch (java.io.IOException e) {
	    throw new MTException("Merge String appears corrupted: " + 
				  e.getMessage());
	} catch (org.xml.sax.SAXException e) {
	    throw new MTException("Error parsing merge template: " +
				  e.getMessage());
	}
	return;
    }

    /**
     * Creates the MergeTree instance from a DOM document
     * consisting of the MergeTemplate. Traverses the merge
     * template and creates the appropriate MergeTreeNodes.
     *
     * @param mergeTemplate DOM document containing the XML merge template
     * @param checkAccumConstraints true/false depending on whether you
     *        want create to verify that this tree can be used
     *        for accumulate - some configurations aren't allowed
     *        by accumulate and if accumConstraints is true, the
     *        existence of these conditions will cause an error to be thrown
     *
     * @return Returns nothing.
     */
    private void create(Document mergeTemplate, boolean checkAccumConstraints) 
	throws MTException {
	/* creates root MergeTreeNode and recursively creates 
	 * MergeTreeNodes (one for each element in the merge template) 
	 * to creates the whole merge tree.
	 */
	rootedKeyMap = new HashMap();
	tagList = new HashSet();
	
	Element docElement = mergeTemplate.getDocumentElement();	

	setMergeType(docElement);
	setDominantSide(docElement);
	
	/* Find the first element child */
	Element mergeElt = DOMHelper.getFirstChildElement(docElement);
	
	if(mergeElt == null) {
	    throw new MTException("Document element of merge template doesn't have any element children");
	}

	mergeTreeRoot = new MergeTreeNode(mergeElt, treeMergeType,
					  treeDomSide, checkAccumConstraints);

	/* make sure that was the only child */
	mergeElt = DOMHelper.getNextSiblingElement(mergeElt);
	if(mergeElt != null) {
	    throw new MTException("Unexpected Child Element of document element of merge template");
	}
	
	/* now that creation succeeded, indicate if this tree is
	 * valid to be used as an accumulate tree or not 
	 */
	validForAccumulate = checkAccumConstraints;
	return;
    }

    private void setMergeType(Element docElement) 
	throws MTException {
	setDominantSide(docElement);

	/* know we won't get empty strings here because these
	 * attrs have default values 
	 */
	String mergeType = docElement.getAttribute("MergeType");
	if(mergeType.equals("outer")) {
	    treeMergeType = MT_OUTER;
	} else if (mergeType.equals("left-outer")) {
	    treeMergeType = MT_LEFTOUTER; 
	} else if (mergeType.equals("right-outer")) {
	    treeMergeType = MT_RIGHTOUTER;
	} else if (mergeType.equals("inner")) {
	    treeMergeType = MT_INNER;
	} else {
	    throw new MTException("Invalid merge type");
	}
	return;
    }

    private void setDominantSide(Element docElement) 
	throws MTException {
	String domSide =  docElement.getAttribute("DominantSide");
	
	if(domSide.equals("left")) {
	    treeDomSide = DS_LEFT;
	} else if (domSide.equals("right")) {
	    treeDomSide = DS_RIGHT;
	} else if (domSide.equals("exactMatch")) {
	    treeDomSide = DS_EXACTMATCH;
	} else {
	    throw new MTException("Invalid Dominant Side");
	}
	return;
    }

    /**
     * Resets the instance variables to null.
     */
    public void reset() {
	mergeTreeRoot = null;
	treeMergeType = MT_OUTER;
	treeDomSide = DS_RIGHT;
	validForAccumulate = false;
	rootedKeyMap = null;
	return;
    }

    /**
     * Merges the an XML Element into an XML Document as specified
     * by the merge tree associated with the class.
     * For now, this is not general enough to handle the join (merge two
     * fragments and create a new result) - maybe it will be some day
     *
     * @param accumDoc     The document into which the fragment
     *                       will be merged
     * @param fragment     The fragment to be merged into the document
     *                       later this may be of type XMLFragment or
     *                       something - for now it is just a normal tree
     */

    void accumulate(Document accumDoc, Element fragment)
	throws UserErrorException {

	/*
         * make it left and right frag and make treewalking code
	 * general enough to handle the join code?
	 */
	Element accumFragRoot = accumDoc.getDocumentElement(); 
	Element mergeFragRoot = fragment; 

	/* have to handle the case when we start with an empty
	 * accumulator - in this case accumFragRoot is null, so
	 * we make an empty document element for the accumulator // 
	 * and root add it to the tree
	 */
	if(accumFragRoot == null) {
	    if(TRACE) {
		System.out.println("KT: Empty Accumulator - creating accum root");
	    }
	    accumFragRoot = accumDoc.
		createElement(mergeTreeRoot.getAccumTagName());
	    accumDoc.appendChild(accumFragRoot);
	} else {
	    if(TRACE) {
		System.out.println("KT: using existing doc for accumulator");
	    }
	}

	/* 
	 * call accumulate. for 2nd argument pass in the key value for 
	 * the accumRoot - we pass in an empty RootedKeyVal to 
	 * createRootedKeyVal for the key of parent of accumRoot
	 */
	ArrayStack accumFragRootKeyVal =  getTempArrayStack();
	ArrayStack localKeyVal = getTempArrayStack();

	mergeTreeRoot.createLocalKeyValue(accumFragRoot, localKeyVal);
	accumFragRootKeyVal.push(localKeyVal);

	do_accumulate(accumFragRoot, 
		      accumFragRootKeyVal,
		      mergeFragRoot, mergeTreeRoot,
		      accumDoc);

	accumFragRootKeyVal.pop();
	returnTempArrayStack(localKeyVal);
	returnTempArrayStack(accumFragRootKeyVal);
    }

    /** Recursive function to walk the merge tree, accumulator (result),
     *  and the fragment and do the merge.
     * The actual merging of elements and matching
     * is done by merge and match objects.  The objective of this function
     * is to handle the tree walking and locating which elements to call
     * merge and match on. 
     *
     * @param accumElt The current element of the accumulate document (result)
     * @param fragElt The current element of the fragment
     * @param mergeTreeNode The current node of the merge tree
     *
     */

    private void do_accumulate(Element accumElt, ArrayStack accumEltKeyVal,
			       Element fragElt, 
			       MergeTreeNode mergeTreeNode,
			       Document accumDoc)
	throws UserErrorException {

	boolean isBottom = true;

	/* in all cases, I get in two elements that should
	 * immediately be merged, we know that accumElt and
	 * fragElt "match" if applicable
	 */

	/* calls the merger (MergeObject) in the MergeTreeNode
	 * to do the merge, just does locally what is needed to
	 * merge these two objects - recursion is done below
	 */
	if(TRACE) {
	  System.out.println("KT: do_accumulate: " +
	 		   DOMHelper.nameAndValue(accumElt));
	}
	mergeTreeNode.accumulate(accumElt, fragElt);

	if(!mergeTreeNode.isDeepMerge()) {
	    /* now call recursively - we scan the subelements of fragElt 
	     * and look for subelements of the mergeElt with the same tag 
	     * name - if found, then get the subelement from accumElt with
	     * matching tag, if available, and make the recursive call.  
	     *
	     * If no existing subelement in accumElt, then do what
	     * is appropriate - insert or ignore based on Merge Type
	     *
	     * for now, we guess this is the most efficient 
	     * (see 5-24-00 notes)
	     */

	    Element nextFragElt = DOMHelper.getFirstChildElement(fragElt);
	    if(TRACE) {
		System.out.println("   KT: next frag elt: "
				   + DOMHelper.nameAndValue(fragElt));
	    }
	    Element nextAccumElt;
	    while(nextFragElt != null) {
		String nextFragTagName = nextFragElt.getTagName();
		
		/* could avoid this wierd lookup by scanning merge tree 
		 * first - but that could be inefficient??
		 */
		MergeTreeNode nextMergeTreeNode = 
		    mergeTreeNode.getChildWithFragTagName(nextFragTagName);

		/* skip the fragment if there is no merge info for it */
		if(nextMergeTreeNode != null) {

		    String nextAccumTagName = nextMergeTreeNode.getAccumTagName();

		    /* this function checks to make sure that kids with tag
		     * nextAccumTagName for this accumElt haven't already
		     * been added to the rooted key map.
		     */
		    /* !!! must do this before creating the key for
		     * nextAccumElt since that modifies accumEltKeyVal
		     */
		    if(nextMergeTreeNode.isNever()) {
			if(TRACE) {
			    System.out.println("    KT: cloning and appending");
			}
			accumElt.appendChild(nextFragElt.cloneNode(true));
		    } else {
			if(!nextMergeTreeNode.isTag()) {
			    addChildrenToRootedKeyMap(accumElt, accumEltKeyVal,
						      nextAccumTagName,
						      nextMergeTreeNode);
			}
			    
			/* create a rooted key value which can be used to probe
			 * the rootedKeyMap for an accum elt matching this 
			 * fragment, thus we use values from the fragment, but 
			 * the tag name for the accumulator side (ugly)
			 */
			/* NOTE: To avoid allocation - we modify accumEltKeyVal
			 * so accumEltKeyVal is NOT valid until we do the pop
			 * below
			 */
			ArrayStack nextAccumEltKeyVal = accumEltKeyVal;
			ArrayStack temp = getTempArrayStack();
			nextMergeTreeNode.createLocalKeyValue(nextFragElt,
							      nextAccumTagName,
							      temp);
			nextAccumEltKeyVal.push(temp);
			    
			/* We assume that only one child of accumFrag
			 * will match the nextFragElt - although there might
			 * be multiple children of accumFrag that have
			 * the tag accumName
			 */		
			    
			/* get nextAccumElt. Look by key if there is a key,
			 * otherwise just use the tag
			 */
			if(!nextMergeTreeNode.isTag()) {
			    if(TRACE) {	
				System.out.print("   KT: Looking in RootedKeyMap for: ");
				nextAccumEltKeyVal.print();
			    }


			    // && nextAccumTagName.equals("STATS")) { ???
			    nextAccumElt = (Element)rootedKeyMap.get(nextAccumEltKeyVal);
			} else {
			    if(TRACE) {
				System.out.println("    KT: getting first child " + nextAccumTagName);
			    }
			    nextAccumElt = 
				DOMHelper.getFirstChildEltByTagName(accumElt,
							   nextAccumTagName);
			}

			if(TRACE) {
			    if(nextAccumElt == null) {
				System.out.println("   KT: Next accum elt is null");
			    } else {
				System.out.println("   KT: Next accum elt is " +
						   DOMHelper.nameAndValue(nextAccumElt));
			    }
			}
			    
			/* have to handle the case when we start with an empty
			 * accumulator - in this case nextAccumElt is null, so
			 * we make an empty element as a holder and add it
			 * to the accumulator tree 
			 */
			boolean addToRootedKeyMap = false;
			if(nextAccumElt == null) {
			    nextAccumElt = accumDoc.
				createElement(nextMergeTreeNode.
					      getAccumTagName());
				/* set value to value of frag elt
				 * shouldn't need to do this - merger
				 * should handle it
				 */
				/*
				 * DOMHelper.setTextValue(nextAccumElt,
				 *	  DOMHelper.getTextValue(nextFragElt),
				 *			accumDoc);
				 */
			    accumElt.appendChild(nextAccumElt);
			    if(TRACE) {
				System.out.println("   KT: created new nextAccumElt "
						   + DOMHelper.nameAndValue(nextAccumElt));
				System.out.println("   KT: added nextAccumElt as child of " 
						   + DOMHelper.nameAndValue(accumElt));
			    }
			    addToRootedKeyMap = true;
			} 
			
			/* the recursive call */
			do_accumulate(nextAccumElt, nextAccumEltKeyVal, 
				      nextFragElt, nextMergeTreeNode, accumDoc);
			
			/* have to add to the rooted key map after we do the
			 * recursive call since we don't have the key until
			 * after the recursive call. This should be fine since
			 * no one will "merge" with this node until after this
			 * call is completed (have to get latching correct)
			 */
			/* OK, done with child, so return accumEltKeyVal to
			 * it's correct value 
			 */
			/* pops tempLocalKeyVal off the stack */
			accumEltKeyVal.pop();
			returnTempArrayStack(temp);
			if(addToRootedKeyMap == true) {
			    addChildToRootedKeyMap(nextAccumElt, accumEltKeyVal,
						   nextMergeTreeNode, false);
			}
		    }
		}
		/* now process the next sibling element */
		nextFragElt = DOMHelper.getNextSiblingElement(nextFragElt);
	    }
	}
	if(TRACE) {
	    System.out.println("KT: do_accumulate return: " + 
			       accumElt.getTagName() + "(" + 
			       DOMHelper.getTextValue(accumElt) + ")");	
	}
	return;
    }

    /**
     * addChildrenToRootedKeyMap adds all children of an element with a given
     * tag name to the rootedKeyMap.
     *
     * @param parent Parent whose children are to be inserted into hash map
     * @param parentKeyVal The rootedKey value of the parent
     * @param tagName Only children with this tag name are to be added
     *      to the hash map
     * @param mergeTreeNode The mergeTreeNode containing key
     *          information for the child
     */
    private void addChildrenToRootedKeyMap(Element parent, 
					   ArrayStack parentKeyVal,
					   String tagName, 
					   MergeTreeNode mergeTreeNode) 
	throws UserErrorException {

	if(mergeTreeNode.isNever()) {
	    return;
	}

	if(TRACE) {
	    System.out.println("   KT: Adding children with tag " + tagName +
			       " of " + DOMHelper.nameAndValue(parent) +
			       " to rooted key map");
	}

	/* check if already done, if so, just return */
    	parentKeyVal.push(tagName);
	if(TRACE) {
	    System.out.print("KT: Checking in tag list for " + parentKeyVal.hashCode() + " ");
	    parentKeyVal.print();
	}
	if(tagList.contains(parentKeyVal)) {
	    parentKeyVal.pop();
	    return; 
	} 
	parentKeyVal.pop();

	/* get an iterator over all children of parent with tagname tagName */
	ListIterator childrenIter = 
	    (DOMHelper.getChildElementsByTagName(parent,tagName))
	                  .listIterator();

	while(childrenIter.hasNext()) {
	    addChildToRootedKeyMap((Element) childrenIter.next(), 
				   parentKeyVal, mergeTreeNode, true);
	}

	/* when done with all kids - add the tagName to the tagname list */
    	parentKeyVal.push(tagName);
	tagList.add(parentKeyVal.clone());
	if(TRACE) {
	    System.out.print("KT: Adding to tag list: " + parentKeyVal.hashCode() + " ");
	    parentKeyVal.print();
	}
	parentKeyVal.pop();
    }

    /**
     * addChildToRootedKeyMap adds a parent's child to the
     * rootedKeyMap
     *
     * @param elt The element which is to be inserted into rooted key map
     * @param parentKeyVal The parent's key value
     * @param mergeTreeNode The merge tree node with key information 
     *         necessary for creating the child's rooted key
     * @param multiKey True if this child is allowed to have
     *         multiple key - values - this allows us to lift the
     *         restriction that matches aren't allowed on set values
     */
    private void addChildToRootedKeyMap(Element child,
					ArrayStack parentKeyVal,
					MergeTreeNode mergeTreeNode,
					boolean multiKey) 
	throws UserErrorException {

	if(TRACE) {
	    System.out.println("KT AddChildToRootedKeyMap " + 
			       DOMHelper.nameAndValue(child));
	}

	if(child == null) {
	    throw new PEException("null child in addChildToRootedKeyMap");
	}

	/* IMPORTANT - have to clone everything and create new
	 * local key values before putting in the hash map
	 */
	if(!multiKey) {
	    /* create the rooted key value for the child */
	    /* KT - wow this looks expensive!! */
	    ArrayStack rootedKeyVal = (ArrayStack)parentKeyVal.clone();
	    ArrayStack lkv = new ArrayStack();
	    mergeTreeNode.createLocalKeyValue(child, lkv);
	    rootedKeyVal.push(lkv);
	    
	    /* put the child and it's "key" - the value list
	     * in the rootedKeyMap - we trust Java to make a good
	     * hash value out of the rkVal - which is really an
	     * ArrayList. rkVal.hashCode() will return the
	     * hash code value returned by calling hashCode() on the
	     * internal array list (inside RootedKeyValue)
	     */
	    if(TRACE) {
		System.out.print("KT: Putting in rooted Key map: ");
		rootedKeyVal.print();
	    }
	    rootedKeyMap.put(rootedKeyVal, child);
	} else {
	    ArrayStack lKVList = getTempArrayStack();

	    mergeTreeNode.createLocalKeyValues(child, lKVList);
	    
	    for(int i=0; i<lKVList.size(); i++) {
		ArrayStack rkv = (ArrayStack)parentKeyVal.clone();
		rkv.push(lKVList.get(i));
		if(TRACE) {
		    System.out.print("KT: Putting in rooted Key map ");
		    rkv.print();
		}
		rootedKeyMap.put(rkv, child);
	    }

	}
	return;
    }
       
    /**
     * Merges two XML Documents together to produce a new result
     * XML Document as specified by the merge tree associated with
     * this class.
     *
     * @param lDoc "left" document to be merged
     * @param rDoc "right" document to be merged
     *
     * @return Returns the new result document 
     */

    Document merge(Document lDoc, Document rDoc) {
	return lDoc;  /* make the compiler happy for now */
    }

    public void dump(PrintStream os) {
	os.print("Merge Tree");
	if(validForAccumulate) {
	    os.print("(accumulate): ");
	}
	os.print("MergeType:");
	switch(treeMergeType) {
	case MT_OUTER:
	    os.print("outer");
	    break;
	case MT_LEFTOUTER:
	    os.print("leftouter");
	    break;
	case MT_RIGHTOUTER:
	    os.print("rightouter");
	    break;
	case MT_INNER:
	    os.print("inner");
	    break;
	default:
	    throw new PEException("Invalid merge type");
	}
	os.print(", Dominant Side:");
	switch(treeDomSide) {
	case DS_LEFT:
	    os.print("left");
	    break;
	case DS_RIGHT:
	    os.print("right");
	    break;
	case DS_EXACTMATCH:
	    os.print("exactmatch");
	    break;
	default:
	    throw new PEException("Invalid dominant side");
	}
	os.println();
	
	mergeTreeRoot.dump(os);
	os.println("End MergeTree");
	return;
    }

    public String toString() {
	String myString = "Merge Tree ";

	if(validForAccumulate) {
	    myString +="(accumulate) ";
	}

	myString += "Root Result Name: ";
	myString += mergeTreeRoot.getResultTagName();
	return myString;
    }

    private ArrayStack getTempArrayStack() {
	if(tempArrayStacks.size() == 0) {
	    return new ArrayStack();
	} else {
	    return (ArrayStack)tempArrayStacks.pop();
	}
    }

    private void returnTempArrayStack(ArrayStack temp) {
	temp.quickReset();
	tempArrayStacks.push(temp);
    }
}






