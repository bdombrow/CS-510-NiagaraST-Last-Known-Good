package niagara.query_engine;

import niagara.xmlql_parser.op_tree.*;
import niagara.utils.*;

import org.w3c.dom.*;
import com.ibm.xml.parser.*;
import java.util.HashMap;

public class XMLBScoring implements ExpressionIF {
    static final String runscored = "$s_runscored";
    static final String bases = "$s_bases";
    static final String strikeout = "$s_strikeout";
    static final String runallowed = "$s_runallowed";
    static final String win = "$s_win";
    static final String steal = "$s_steal";

    private HashMap varTable;
    public void setupVarTable(HashMap varTable) {
	this.varTable = varTable;
    }

    private int getInt(StreamTupleElement ste, HashMap varTable, String v) {
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
	int final_score = getIntVar(ste, runscored)
	    + getIntVar(ste, bases)
	    + getIntVar(ste, steal)  
	    + getIntVar(ste, win) * 10 
	    + getIntVar(ste, strikeout) 
	    + getIntVar(ste, runallowed) * (-1);
	TXElement txe = new TXElement("score");
	txe.appendChild(new TXText(Integer.toString(final_score)));
	return txe;
    }
}

