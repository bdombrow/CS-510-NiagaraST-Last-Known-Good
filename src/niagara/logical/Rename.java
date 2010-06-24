package niagara.logical;

import java.util.StringTokenizer;
import java.util.Vector;

import org.w3c.dom.Element;

import niagara.connection_server.Catalog;
import niagara.connection_server.InvalidPlanException;
import niagara.optimizer.colombia.Attribute;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;
import niagara.xmlql_parser.skolem;

/**
 * @author rfernand
 * @version 1.0
 * 
 * The <code>Rename</code> logical operator renames input schema elements.
 *
 */
public class Rename extends UnaryOperator {

	private Vector<Attribute> attributesToRename;
	private Vector<String> newNames;

	public Rename() {
		attributesToRename = new Vector<Attribute>();
		newNames = new Vector<String>();
	}


	public Vector<Attribute> getAttributesToRename() {
		return attributesToRename;
	}

	public Vector<String> getNewNames() {
		return newNames;
	}


	@Override
	public LogicalProperty findLogProp(ICatalog catalog, LogicalProperty[] input) {

		LogicalProperty result = input[0].copy();

		for(int i = 0; i < attributesToRename.size(); i++) {
			String sourceName = attributesToRename.get(i).toString();
			result.getAttr(sourceName).setName(newNames.get(i).toString());
		}
		return result;
	}


	public void loadFromXML(Element e, LogicalProperty[] inputProperties, Catalog catalog) throws InvalidPlanException {

		String sourceStr = e.getAttribute("source");
		String destStr   = e.getAttribute("dest");

		// Parse the source attributes 
		Vector<Attribute> sourceAttrs = new Vector<Attribute>();
		StringTokenizer st = new StringTokenizer(sourceStr);
		while (st.hasMoreTokens()) {
			String varName = st.nextToken();
			Attribute attr = Variable.findVariable(inputProperties, varName);
			sourceAttrs.addElement(attr);
		}

		// Parse the source attributes 
		Vector<String> destAttrs = new Vector<String>();
		StringTokenizer stk = new StringTokenizer(destStr);
		while (stk.hasMoreTokens()) {
			String varName = stk.nextToken();
			//Attribute attr = Variable.findVariable(inputProperties, varName);
			destAttrs.addElement(varName);
		}

		attributesToRename = sourceAttrs;
		newNames = destAttrs;
	}


	@Override
	public boolean equals(Object other) {

		if(other == null || !(other instanceof Rename))
			return false;
		if(other.getClass() != Rename.class)
			return other.equals(this);

		Rename ot = (Rename)other;

		if (ot.attributesToRename == null || ot.newNames == null)
			return false;

		if(!this.attributesToRename.equals(ot.attributesToRename) || !this.newNames.equals(ot.newNames))
			return false;

		return true;

	}

	public void dump() {
		System.out.println("Rename :");
		for (Attribute s : attributesToRename) {
			System.out.println("Source : " + s.getName());
		}
		for (String s : newNames) {
			System.out.println("Dest : " + s);
		}
	}

	public String toString() {
		return " rename " ;//+ pred.toString();
	}

	@Override
	public int hashCode() {
		return attributesToRename.hashCode() ^ newNames.hashCode();
	}

	@Override
	public Op opCopy() {
		Rename op = new Rename();
		op.attributesToRename = attributesToRename;
		op.newNames = newNames;
		return op;	
	}
}
