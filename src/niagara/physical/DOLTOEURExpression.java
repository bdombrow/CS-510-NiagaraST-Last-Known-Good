/* $Id: DOLTOEURExpression.java,v 1.1 2003/12/24 01:49:03 vpapad Exp $ */
package niagara.physical;

import niagara.utils.*;
import niagara.logical.*;
import niagara.ndom.*;
import niagara.query_engine.*;

import org.w3c.dom.*;
import java.util.*;
import java.text.*;

/** Sample user-defined expression class */
public class DOLTOEURExpression implements ExpressionIF {
    private int pricePos;
    private static Locale loc = new Locale("fr");
    private static NumberFormat nf = NumberFormat.getCurrencyInstance(loc);
	private Document doc = DOMFactory.newDocument();

    public Node processTuple(Tuple ste) {
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
