/* $Id: DOLTOEURExpression.java,v 1.1 2003/02/22 08:08:50 tufte Exp $ */
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

    public Node processTuple(StreamTupleElement ste) {
        Document doc = DOMFactory.newDocument();
        Node priceNode = ste.getAttribute(pricePos);
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
