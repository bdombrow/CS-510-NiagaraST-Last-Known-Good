/* $Id$ */
package niagara.query_engine;

import niagara.xmlql_parser.op_tree.*;
import niagara.utils.*;
import niagara.ndom.*;

import org.w3c.dom.*;

/** Sample user-defined expression class */
public class TestExpression implements ExpressionIF {
    private int aPos;
    private int bPos;
    public Node processTuple(StreamTupleElement ste) {
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
    }
    public void setupSchema(TupleSchema ts) {
        aPos = ts.getPosition("a");
        bPos = ts.getPosition("b");
    }
}
