package niagara.query_engine;

import niagara.xmlql_parser.op_tree.*;
import niagara.utils.*;

import org.w3c.dom.*;
import com.ibm.xml.parser.*;
import java.util.HashMap;

public class XMLBScoring implements ExpressionIF {
    static final String runscored = "$runsscored";
    static final String bases = "$bases";
    static final String strikeout = "$strikeouts";
    static final String runallowed = "$runsallowed";
    static final String win = "$wins";
    static final String steal = "$steals";

    private HashMap varTable;
    public void setupVarTable(HashMap varTable) {
	this.varTable = varTable;
    }

    private int getInt(StreamTupleElement ste, String v) {
	Node n = (Node) ste.getAttribute(((Integer) varTable.get(v)).intValue());
	try {
	    if (n instanceof Text) {
		return Integer.parseInt(n.getNodeValue());
	    }
	    else { // This better be instanceof Element
		return Integer.parseInt(((Text) n.getChildNodes().item(0)).getNodeValue());
	    }
	}
	catch (Exception e) {
	    return 0;
	}
    }

    public Node processTuple(StreamTupleElement ste) {
	// This is a complete and utter hack
	int final_score = getInt(ste, runscored)
	    + getInt(ste, bases)
	    + getInt(ste, steal)  
	    + getInt(ste, win) * 10 
	    + getInt(ste, strikeout) 
	    + getInt(ste, runallowed) * (-1);
	TXElement txe = new TXElement("score");
	txe.appendChild(new TXText(Integer.toString(final_score)));
	return txe;
    }
}

