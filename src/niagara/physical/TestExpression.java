package niagara.physical;

import niagara.logical.ExpressionIF;
import niagara.query_engine.TupleSchema;
import niagara.utils.BaseAttr;
import niagara.utils.StringAttr;
import niagara.utils.Tuple;

/** Sample user-defined expression class */
public class TestExpression implements ExpressionIF {
	private int aPos;
	private int bPos;

	/*
	 * public Node processTuple(Tuple ste) { Document doc =
	 * DOMFactory.newDocument(); Node aNode = ste.getAttribute(aPos); Node bNode
	 * = ste.getAttribute(bPos); if(aNode == null || bNode == null) return null;
	 * int aNum, bNum; try { aNum =
	 * Integer.parseInt(aNode.getFirstChild().getNodeValue()); bNum =
	 * Integer.parseInt(bNode.getFirstChild().getNodeValue()); } catch
	 * (NumberFormatException nfe) { aNum = bNum = 0; } Element res =
	 * doc.createElement("AminusB");
	 * res.appendChild(doc.createTextNode(String.valueOf(aNum - bNum))); return
	 * res; }
	 */
	public BaseAttr processTuple(Tuple ste) {
		// Document doc = DOMFactory.newDocument();
		BaseAttr aNode = (BaseAttr) ste.getAttribute(aPos);
		BaseAttr bNode = (BaseAttr) ste.getAttribute(bPos);
		if (aNode == null || bNode == null)
			return null;
		int aNum, bNum;
		try {
			aNum = Integer.parseInt(aNode.toASCII());
			bNum = Integer.parseInt(bNode.toASCII());
		} catch (NumberFormatException nfe) {
			aNum = bNum = 0;
		}
		// Element res = doc.createElement("AminusB");
		// res.appendChild(doc.createTextNode(String.valueOf(aNum - bNum)));
		BaseAttr res = new StringAttr(aNum - bNum);
		return res;
	}

	public void setupSchema(TupleSchema ts) {
		aPos = ts.getPosition("a");
		bPos = ts.getPosition("b");
	}
}
