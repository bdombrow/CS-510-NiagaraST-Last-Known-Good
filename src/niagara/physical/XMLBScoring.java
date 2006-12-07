/* $Id: XMLBScoring.java,v 1.2 2006/12/07 00:08:25 jinli Exp $ */
package niagara.physical;

import niagara.utils.*;
import niagara.logical.*;
import niagara.ndom.*;
import niagara.query_engine.*;

import org.w3c.dom.*;
import java.util.HashMap;

public class XMLBScoring implements ExpressionIF {
    static final String runscored = "runsscored";
    static final String bases = "bases";
    static final String strikeout = "strikeouts";
    static final String runallowed = "runsallowed";
    static final String win = "wins";
    static final String steal = "steals";

    private Document doc;
    public XMLBScoring() {
        doc = DOMFactory.newDocument();
        varTable = new HashMap();    
    }

    HashMap varTable;
    
    public void setupSchema(TupleSchema ts) {
        varTable.put(runscored, new Integer(ts.getPosition(runscored)));
        varTable.put(bases, new Integer(ts.getPosition(bases)));
        varTable.put(strikeout, new Integer(ts.getPosition(strikeout)));
        varTable.put(runallowed, new Integer(ts.getPosition(runallowed)));
        varTable.put(win, new Integer(ts.getPosition(win)));
        varTable.put(steal, new Integer(ts.getPosition(steal)));
    }

    private int getInt(Tuple ste, String v) {
		Node n = (Node) ste.getAttribute(((Integer) varTable.get(v)).intValue());
		if(n == null)
			return -1;
	    if (n instanceof Text) {
		return Integer.parseInt(n.getNodeValue());
	    }
	    else { // This better be instanceof Element
		return Integer.parseInt(((Text) n.getChildNodes().item(0)).getNodeValue());
	    }
    }

    public BaseAttr processTuple(Tuple ste) {
	// This is a complete and utter hack
	int final_score = getInt(ste, runscored)
	    + getInt(ste, bases)
	    + getInt(ste, steal)  
	    + getInt(ste, win) * 10 
	    + getInt(ste, strikeout) 
	    + getInt(ste, runallowed) * (-1);
	//Element txe = doc.createElement("score");
	//txe.appendChild(doc.createTextNode(Integer.toString(final_score)));
	BaseAttr txe = new StringAttr(final_score);
	return txe;
    }
}

