package niagara.query_engine;

import niagara.xmlql_parser.op_tree.*;
import niagara.utils.*;
import niagara.ndom.*;

import org.w3c.dom.*;
import java.util.*;

public class TestExpression implements ExpressionIF {
    public Node processTuple(StreamTupleElement ste) {
        Document doc = DOMFactory.newDocument();
	Element res = doc.createElement("aNumber");
	res.appendChild(doc.createTextNode("100"));
	return res;
    }
    public void setupVarTable(HashMap varTable) {
	// Do nothing
    }
}
