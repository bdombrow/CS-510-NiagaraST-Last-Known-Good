/* $Id: DOLTOEURExpression.java,v 1.3 2003/09/30 21:28:05 ptucker Exp $ */
package niagara.query_engine;

import niagara.xmlql_parser.op_tree.*;
import niagara.utils.*;
import niagara.ndom.*;

import org.w3c.dom.*;
import java.util.*;
import java.text.*;

/** Sample user-defined expression class */
public class DOLTOEURExpression implements ExpressionIF {
    private int pricePos;
    private static Locale loc = new Locale("fr");
    private static NumberFormat nf = NumberFormat.getCurrencyInstance(loc);
	private Document doc = DOMFactory.newDocument();

    public Node processTuple(StreamTupleElement ste) {
        Node priceNode = ste.getAttribute(pricePos);
        if(priceNode == null)
        	return null;
		float priceNum;
        try {
            priceNum = Float.parseFloat(priceNode.getFirstChild().getNodeValue());
        } catch (NumberFormatException nfe) {
	    priceNum = 0;
        }
		Element res = doc.createElement("bid");
		res.appendChild(doc.createTextNode(nf.format(priceNum*1.08380)));
		return res;
    }
    public void setupSchema(TupleSchema ts) {
        pricePos = ts.getPosition("price");
    }
}
