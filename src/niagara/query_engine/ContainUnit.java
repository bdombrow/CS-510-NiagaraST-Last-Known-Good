package niagara.query_engine;

import java.util.Vector;

import niagara.xmlql_parser.regExp;
import niagara.xmlql_parser.regExpDataNode;
import niagara.xmlql_parser.regExpOpNode;

/**
 * A ContainUnit corresponds to a SchemaUnit. Unlike SchemaUnit which records a
 * parent pointer, a ContainUnit records a list of children pointers, as well as
 * selection predicates
 */
@SuppressWarnings("unchecked")
class ContainUnit {
	private regExp tagExp;
	private Vector children; // of Integer's
	private int parentIndex = -1;
	private boolean usedInConstructTree = false;

	public ContainUnit(regExp tag, int parent) {
		tagExp = tag;
		children = new Vector();
		parentIndex = parent;
	}

	public void addChild(int index) {
		children.addElement(new Integer(index));
	}

	public regExp getTagExpression() {
		return tagExp;
	}

	public int numChildren() {
		return children.size();
	}

	public int getChild(int i) {
		return ((Integer) children.elementAt(i)).intValue();
	}

	public Vector getChildren() {
		return children;
	}

	public int getParent() {
		return parentIndex;
	}

	public boolean isUsedInConstructTree() {
		return usedInConstructTree;
	}

	public void setUsedInConstructTree() {
		usedInConstructTree = true;
	}

	public void dump() {
		System.out.print("regexp:");
		if (tagExp == null)
			System.out.println("NULL");
		else if (tagExp instanceof regExpDataNode) {
			System.out.print("regExpDataNode: ");
			tagExp.dump(0);
		} else if (tagExp instanceof regExpOpNode) {
			System.out.print("regExpOpNode: ");
			tagExp.dump(0);
		}

		System.out.println("children: " + children);
		System.out.println("parent: " + parentIndex);
	}
}
