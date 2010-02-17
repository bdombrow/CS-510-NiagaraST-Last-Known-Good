package niagara.xmlql_parser;

import java.util.Vector;

import niagara.logical.NodeDomain;
import niagara.optimizer.colombia.Attribute;
import niagara.query_engine.TupleSchema;

@SuppressWarnings("unchecked")
/**
 * This class is used to store mapping between variable and attribute
 *
 */
public class varTbl {
	private Vector varList; // of varToAttr

	public varTbl() {
		varList = new Vector();
	}

	public varTbl(TupleSchema ts) {
		varList = new Vector();
		for (int i = 0; i < ts.getLength(); i++) {
			String varName = ts.getVariableName(i);
			// XXX vpapad: ugh... Really disgusting hack follows
			// In the presence of non-XML types we need to mess around
			// with the "domain" of the variable
			Attribute attr = (ts.getVariable(i));
			if (!(attr instanceof NodeDomain))
				varList.add(new varToAttr(varName, new schemaAttribute(i,
						NodeDomain.getDomain(varType.ELEMENT_VAR).getType(),
						varName)));
			else {
				int varType = ((NodeDomain) ts.getVariable(i).getDomain())
						.getType();
				varList.add(new varToAttr(varName, new schemaAttribute(i,
						varType, varName)));
			}
		}
	}

	/**
	 * Constructor
	 * 
	 * @param variable
	 *            table to make a copy of
	 */

	public varTbl(varTbl vt) {
		varList = new Vector();
		for (int i = 0; i < vt.varList.size(); i++)
			varList.addElement(new varToAttr((varToAttr) vt.varList
					.elementAt(i)));
	}

	/**
	 * @return number of variables
	 */

	public int size() {
		return varList.size();
	}

	/**
	 * @param index
	 *            into the table
	 * @return the Nth entry of the table
	 */

	public varToAttr getVarToAttr(int i) {
		return (varToAttr) varList.elementAt(i);
	}

	/**
	 * @param the
	 *            variable to look for
	 * @param the
	 *            schemaAttribute for the given variable
	 */

	public schemaAttribute lookUp(String var) {
		varToAttr entry;

		for (int i = 0; i < varList.size(); i++) {
			entry = (varToAttr) varList.elementAt(i);
			if (var.equals(entry.getVar()))
				return entry.getAttribute();
		}
		return null;
	}

	/**
	 * to check if this table contains a given set of variables
	 * 
	 * @param the
	 *            given set of variables
	 * @return true if it contains the given set of variables, false otherwise
	 */

	public boolean contains(Vector variables) {
		String var;
		for (int i = 0; i < variables.size(); i++) {
			var = (String) variables.elementAt(i);
			if (lookUp(var) == null)
				return false;
		}
		return true;
	}

	/**
	 * add a new entry to the table
	 * 
	 * @param the
	 *            name of the variable
	 * @param associated
	 *            schemaAttribute
	 */

	public void addVar(String var, schemaAttribute _attr) {
		varToAttr entry;

		for (int i = 0; i < varList.size(); i++) {
			entry = (varToAttr) varList.elementAt(i);
			if (var.equals(entry.getVar())) {
				entry.addAttribute(_attr);
				return;
			}
		}

		varList.addElement(new varToAttr(var, _attr));
	}

	/**
	 * to find the intersection with another varTbl
	 * 
	 * @param another
	 *            variable table to intersect with
	 * @return list of variables coomon in the two var tables
	 */

	public Vector intersect(varTbl rightVarTbl) {
		Vector rightVarList = rightVarTbl.varList;
		Vector leftVarList = varList;
		Vector commonVar = new Vector();
		varToAttr leftvar, rightvar;
		String var;

		for (int i = 0; i < leftVarList.size(); i++) {
			leftvar = (varToAttr) leftVarList.elementAt(i);
			for (int j = 0; j < rightVarList.size(); j++) {
				rightvar = (varToAttr) rightVarList.elementAt(j);
				var = leftvar.getVar();
				if (var.equals(rightvar.getVar()))
					commonVar.addElement(var);
			}
		}
		return commonVar;
	}

	public String getVars() {
		String toReturn = " vars='";
		String[] vars = new String[varList.size()];

		for (int i = 0; i < varList.size(); i++) {
			varToAttr vta = (varToAttr) varList.get(i);
			String varName = vta.getVar();
			schemaAttribute sa = (schemaAttribute) vta.getAttributeList()
					.get(0);
			int position = sa.getAttrId();
			vars[position] = varName;
		}

		for (int i = 0; i < vars.length; i++) {
			toReturn += vars[i] + " ";
		}
		toReturn += "' ";
		return toReturn;
	}

	/**
	 * print to the standard output
	 */

	public void dump() {
		for (int i = 0; i < varList.size(); i++)
			((varToAttr) varList.elementAt(i)).dump();
	}
}
