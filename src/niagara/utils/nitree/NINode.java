/*
 * $RCSfile:
 * $Revision:
 * $Date:
 * $Author:
 */


package niagara.utils.nitree;

public abstract class NINode {

    /* functions to get and set the "value" of a node, for attributes,
     * returns the attribute value, for elements, returns the
     * value of the text element, not supported for documents
     */
    public abstract String myGetNodeValue();
    public abstract void mySetNodeValue(String nodeValue) 
	throws NITreeException;

    /* DOM Functions */
    public abstract String getNodeName();

    public abstract void appendChild(NIElement child)
	throws NITreeException;

    public abstract void replaceChild(NIElement oldChild, NIElement newChild)
	throws NITreeException;

}
