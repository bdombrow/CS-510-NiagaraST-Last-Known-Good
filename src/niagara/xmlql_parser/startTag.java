package niagara.xmlql_parser;

import java.util.Vector;

import niagara.logical.Variable;
import niagara.optimizer.colombia.Attrs;
import niagara.utils.CUtil;
import niagara.utils.PEException;

/**
 * This class is used to represent the tag name with attributes and skolem
 * function in the construct part of XML-QL.
 * 
 */

@SuppressWarnings("unchecked")
public class startTag {
	private data sdata; // tag name (identifier or variable)
	private skolem skolemId; // for skolem function
	private Vector attrList; // list of attribute-value pairs

	/**
	 * Constructor
	 * 
	 * @param tag
	 *            name
	 * @param skolem
	 *            function
	 * @param list
	 *            of attribute-value pairs
	 */

	public startTag(data d, skolem s, Vector v) {
		sdata = d;
		skolemId = s;
		attrList = v;
	}

	/**
	 * @return the tag name
	 */

	public data getSdata() {
		return sdata;
	}

	/**
	 * replaces the occurences of variables in the tag name, attributes and
	 * skolem function with their corresponding schemaAttributes
	 * 
	 * @param the
	 *            variable table that maps variable to their schemaAttribute
	 */

	public void replaceVar(varTbl vt) {

//		String var;
		int type;
		schemaAttribute sa;
		attr attribute;

		type = sdata.getType();
		if (type == dataType.VAR) {
			sa = vt.lookUp((String) sdata.getValue());
			sdata = new data(dataType.ATTR, sa);
		}

		for (int i = 0; i < attrList.size(); i++) {
			attribute = (attr) attrList.elementAt(i);
			attribute.replaceVar(vt);
		}

		if (skolemId != null)
			skolemId.replaceVar(vt);
	}

	/**
	 * @return skolem function
	 */

	public skolem getSkolemId() {
		return skolemId;
	}

	/**
	 * @return the list attribute-value pair
	 */

	public Vector getAttrList() {
		return attrList;
	}

	/**
	 * prints to the standard output
	 * 
	 * @param number
	 *            of tabs at the beginning of each line
	 */

	public void dump(int depth) {
		CUtil.genTab(depth);
		System.out.println("start_tag:");
		sdata.dump(depth);
		if (skolemId != null)
			skolemId.dump();
		for (int i = 0; i < attrList.size(); i++)
			((attr) attrList.elementAt(i)).dump(depth);
	}

	public Attrs requiredInputAttributes(Attrs attrs) {
		Attrs reqAttrs = new Attrs();
		if (sdata.type == dataType.VAR)
			reqAttrs.add(new Variable((String) sdata.getValue()));
		for (int i = 0; i < attrList.size(); i++)
			((attr) attrList.get(i)).addRequiredVariables(reqAttrs);
		if (skolemId != null)
			throw new PEException("Cannot handle skolems yet");
		return reqAttrs;
	}
}
