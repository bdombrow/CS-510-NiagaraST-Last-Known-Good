package niagara.utils.nitree;

/*
 * Class that (theoretically) extends DOM - I am concerned that it will be lots
 * of grunge work and possibly not worth the effort to support the full DOM
 * interface.  NIDocument and the associated other classes which will form
 * the NIDOM interface will support copy on write
 *
 * Initial implementation of NIDocument and associated classes
 * will be based on DOM trees
 * NIDocument should be an interface - prototype as a class
 * and then convert to an interface (named NIDocument - rename the 
 * class) once interface is established
 */

import org.w3c.dom.*;
import com.ibm.xml.parser.*;

import niagara.utils.*;

public class NIDocument extends NINode {
    
    /* 
     * Each NIDocument has an associated MapTable which maintains
     * associations between DOM Elements and NIElements
     * Map table will be implemented with those reference counting
     * stuff so that NIElements for DOM trees that have gone away
     * are deleted properly (have to remove reference to NIElement
     * in MapTable so that reference count for NIElement can go to 
     * zero).
     * MapTable variable must be set in constructor or initialization
     */

    private MapTable mapTable; 
    private Document domDoc;

    /*
     * permission flags (for copy on write) are kept in the NIDocument so it is
     * easy to set/unset all of a tree's permissions at one time
     */
    private Boolean[] writeable;
    private int writeableIndex;

    private boolean initialized;

    /**
     * Creates an unitialized NIDocument.  Must be initialized before
     * it is used.
     */
    public NIDocument() {
	mapTable = null;
	domDoc = null;
	writeableIndex = 0;
	initialized = false;
        return;
    }

    /**
     * Initialize a NIDocument with appropriate variables.
     *
     * @param _mapTable  The map table to be associated with the document.
     * @param _domDoc    The DOM doc to be associated with this NIDocument.
     */
    public void initialize(MapTable _mapTable, Document _domDoc) {
	mapTable = _mapTable;
	domDoc = _domDoc;
	writeableIndex = 0;
	initialized = true;
	return;
    }

    /**
     * Tells whether the NIDocument has been initialized or not.
     */
    public boolean isInitialized() { return initialized; }

    /**
     * resets all the writable bits to false
     */
    public void globalSetWriteableFalse() {
	for(int i = 0; i<writeable.length; i++) {
	    writeable[i] = Boolean.FALSE;
	}
	return;
    }
    
    /* 
     * returns the Dom Document associated with this NIDocument
     *
     */
    public Document getDomDoc() { return domDoc; }

    /*
     * Returns the NIElement associated with a given
     * DOM element - if no associated DOM element exists one is created.
     *
     * @param _domElement - The element for which we will find the
     *                       associated NIElement
     *
     * @return The NIElement associated with the _domeElement passed as
     *          a parameter
     */
    public NIElement getAssocNIElement(Element domElt) {
	
	/*
	 * Check to see if this element is in the map table already
	 * and if not add it to the table
	 */
	Object o = mapTable.lookup(domElt);
        if(o != null) {
            return (NIElement)o;
	}

	/* else no association found, must create a new NIElement */
	return createNIElement(domElt);
    }

    /*
     * Returns the NIAttribute associated with a given
     * DOM attribute - if no associated DOM attribute exists one is created.
     *
     * @param domAttr - The attribute for which we will find the
     *                       associated NIAttribute
     *
     * @return The NIAttribute associated with the domAttr passed as
     *          a parameter
     */

    public NIAttribute getAssocNIAttr(Attr domAttr) {
	
	/*
	 * Check to see if this element is in the map table already
	 * and if not add it to the table
	 */

	Object o = mapTable.lookup(domAttr);
	if(o != null) {
             return (NIAttribute)o;
	}

	/* else have to create a new NIAttribute */
	NIAttribute niAttr = new NIAttribute();
	niAttr.initialize(domAttr);
	
	return niAttr;
    }

    /**
     * Returns the NIDocument associated with a given DOM document -
     * if no such NIDocument exists, will create one and return it.
     *
     * @param _domDoc   The dom doc for which we will return an associated 
     *                     NIDoc.
     * @param _mapTable The map table to search for an existing association
     *                   - note has to be a parameter since this is a 
     *                   static function.
     *
     * @return The NIDocument associated with the param <code> _domDoc <\code>
     */

    public static NIDocument getAssocNIDocument(Document _domDoc, 
						MapTable _mapTable) {
        Object o = _mapTable.lookup(_domDoc);
	if(o != null) {
             return (NIDocument)o;
	}

	/* else have to create a new NIDocument */
	NIDocument niDoc = new NIDocument();
       
        niDoc.initialize(_mapTable, _domDoc);
	
	return niDoc;
    }

    /* DOM Interface Functions */

    /**
     * Function defined in DOM.
     */
    public NIElement getDocumentElement() {
	return getAssocNIElement(domDoc.getDocumentElement());
    }

    /**
     * Creates a new NIElement from scratch - also creates the DOM element
     * associated with the NIElement
     *
     * @param tagName The tagName of the new element.
     *
     * @return Returns the newly-created NIElement
     */
    public NIElement createNIElement(String tagName) {
	Element domElt = domDoc.createElement(tagName);

	/* creates and initializes a new NIElt and adds an 
	 * association in the MapTable
	 */
	return createNIElement(domElt);
    }

    /**
     * Creates a new NIElement to be associated with a particular
     * dom element 
     *
     * @param domElement The dom element to be associated with 
     *        the new NIElement 
     *
     * @return Returns the newly-created NIElement
     */
    public NIElement createNIElement(Element domElement) {

	/* creates and initializes a new NIElt and adds an 
	 * association in the MapTable
	 */
	NIElement niElt = new NIElement();

	/* initialize the element - we can update any elements
	 * we create
	 */
	synchronized (this) {
	    writeableIndex++;
	    writeable[writeableIndex] = new Boolean(true);
	    niElt.initialize((TXElement)domElement, 
			     writeable[writeableIndex], this);
	}

	/* and insert into the map table to make association */
	mapTable.insert(domElement, niElt);

	return niElt;
    }

    /* NOTE - this is important - we assume that a document is
     * always writeable, that it has been cloned appropriately
     * before any operator that can update it is allowed to work
     * on it
     */
    public void replaceChild(NINode oldChild, NINode newChild) {

	/* now do the update */
	domDoc.replaceChild(((NIElement)oldChild).getDomElement(), 
			    ((NIElement)newChild).getDomElement());
	return;
    }

}
