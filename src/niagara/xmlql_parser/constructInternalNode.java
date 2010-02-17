package niagara.xmlql_parser;

import java.util.Vector;

import niagara.optimizer.colombia.Attrs;
import niagara.utils.CUtil;

/**
 * 
 * This class is used to represent tags and subelements in construct part. The
 * value of a leaf element is represented by another class called
 * constructLeafNode.
 * 
 */

@SuppressWarnings("unchecked")
public class constructInternalNode extends constructBaseNode {

	// children or subelements of this node
	private Vector children;

	// tagnames with attributes and skolem function
	private startTag st;

	/**
	 * Constructor
	 * 
	 * @param start
	 *            tag that representa tag name, attributes, and skolem
	 * @param list
	 *            of children
	 */

	public constructInternalNode(startTag s, Vector v) {
		st = s;
		children = v;
	}

	/**
	 * @return list of children
	 */
	public Vector getChildren() {
		return children;
	}

	/**
	 * @param set
	 *            children to this list
	 */
	public void setChildren(Vector child) {
		children = child;
	}

	/**
	 * @return get the start tag
	 */
	public startTag getStartTag() {
		return st;
	}

	/**
	 * @return tag name
	 */
	public data getTagData() {
		return st.getSdata();
	}

	/**
	 * @return skolem function
	 */
	public skolem getSkolem() {
		return st.getSkolemId();
	}

	/**
	 * @return list of attribute-value pair
	 */
	public Vector getAttrList() {
		return st.getAttrList();
	}

	/**
	 * replaces the occurences of variables with their corresponding schema
	 * attribute
	 * 
	 * @param the
	 *            var table that stores the mapping between variables and schema
	 *            attribute
	 */

	public void replaceVar(varTbl vt) {
		st.replaceVar(vt);

		constructBaseNode child;
		for (int i = 0; i < children.size(); i++) {
			child = (constructBaseNode) children.elementAt(i);
			child.replaceVar(vt);
		}
	}

	// UGLY---------------will have to go

	public void truncate() {
		children = new Vector();
		children.addElement(new constructLeafNode(new data(dataType.ATTR,
				new schemaAttribute(0, varType.ELEMENT_VAR, null))));
	}

	// -----------------------------------------------------------------------

	/**
	 * prints to the standard output
	 * 
	 * @param number
	 *            of tabs before each line
	 */

	public void dump(int depth) {
		CUtil.genTab(depth);
		System.out.println("CHILDREN");
		(getStartTag()).dump(depth);
		Object bn;
		for (int i = 0; i < children.size(); i++) {
			bn = children.elementAt(i);
			if (bn instanceof constructLeafNode)
				((constructLeafNode) bn).dump(depth + 1);
			else if (bn instanceof constructInternalNode)
				((constructInternalNode) bn).dump(depth + 1);
			else if (bn instanceof schemaAttribute)
				((schemaAttribute) bn).dump(depth + 1);
			else if (bn instanceof Integer)
				System.out.println(((Integer) bn).intValue());
		}
	}

	public Attrs requiredInputAttributes(Attrs attrs) {
		Attrs reqAttrs = new Attrs();
		reqAttrs.merge(st.requiredInputAttributes(attrs));
		for (int i = 0; i < children.size(); i++)
			reqAttrs.merge(((constructBaseNode) children.get(i))
					.requiredInputAttributes(attrs));
		return reqAttrs;
	}
}
