/* $Id: TestExpression.java,v 1.3 2006/12/07 00:08:25 jinli Exp $ */
package niagara.physical;

import niagara.utils.*;
import niagara.logical.*;
import niagara.ndom.*;
import niagara.query_engine.*;

import org.w3c.dom.*;

/** Sample user-defined expression class */
public class TestExpression implements ExpressionIF {
    private int aPos;
    private int bPos;
    /*public Node processTuple(Tuple ste) {
        Document doc = DOMFactory.newDocument();
        Node aNode = ste.getAttribute(aPos);
        Node bNode = ste.getAttribute(bPos);
        if(aNode == null || bNode == null)
        	return null;
        int aNum, bNum;
        try {
            aNum = Integer.parseInt(aNode.getFirstChild().getNodeValue());
            bNum = Integer.parseInt(bNode.getFirstChild().getNodeValue());
        } catch (NumberFormatException nfe) {
            aNum = bNum = 0;
        }
	Element res = doc.createElement("AminusB");
	res.appendChild(doc.createTextNode(String.valueOf(aNum - bNum)));
	return res;
    }*/
    public BaseAttr processTuple(Tuple ste) {
        Document doc = DOMFactory.newDocument();
        BaseAttr aNode = (BaseAttr)ste.getAttribute(aPos);
        BaseAttr bNode = (BaseAttr)ste.getAttribute(bPos);
        if(aNode == null || bNode == null)
        	return null;
        int aNum, bNum;
        try {
            aNum = Integer.parseInt(aNode.toASCII());
            bNum = Integer.parseInt(bNode.toASCII());
        } catch (NumberFormatException nfe) {
            aNum = bNum = 0;
        }
	//Element res = doc.createElement("AminusB");
	//res.appendChild(doc.createTextNode(String.valueOf(aNum - bNum)));
        BaseAttr res = new StringAttr(aNum - bNum);
	return res;
    }

    public void setupSchema(TupleSchema ts) {
        aPos = ts.getPosition("a");
        bPos = ts.getPosition("b");
    }
}
