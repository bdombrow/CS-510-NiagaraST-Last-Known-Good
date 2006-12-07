/* $Id: DOLTOEURExpression.java,v 1.3 2006/12/07 00:08:26 jinli Exp $ */
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

    public BaseAttr processTuple(Tuple ste) {
        //Node priceNode = ste.getAttribute(pricePos);
    	BaseAttr priceNode = (BaseAttr)ste.getAttribute(pricePos);
        if(priceNode == null)
        	return null;
		float priceNum;
        try {
            //priceNum = Float.parseFloat(priceNode.getFirstChild().getNodeValue());
        	priceNum = Float.parseFloat(priceNode.toASCII());
        } catch (NumberFormatException nfe) {
	    priceNum = 0;
        }
        BaseAttr res = new StringAttr(nf.format(priceNum*1.08380));
		//Element res = doc.createElement("bid");
		//res.appendChild(doc.createTextNode(nf.format(priceNum*1.08380)));
		return res;
    }
    public void setupSchema(TupleSchema ts) {
        pricePos = ts.getPosition("price");
    }
}
