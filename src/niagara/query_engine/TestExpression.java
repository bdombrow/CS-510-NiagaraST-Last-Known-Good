package niagara.query_engine;

import niagara.xmlql_parser.op_tree.*;
import niagara.utils.*;

import org.w3c.dom.*;
import com.ibm.xml.parser.*;

public class TestExpression implements ExpressionIF {
    public Node processTuple(StreamTupleElement ste) {
	Element res = new TXElement("aNumber");
	res.appendChild(new TXText("100"));
	return res;
    }
}
