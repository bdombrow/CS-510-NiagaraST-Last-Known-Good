package niagara.logical;

import niagara.connection_server.Catalog;
import niagara.connection_server.InvalidPlanException;
import niagara.optimizer.colombia.Attribute;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;
import niagara.xmlql_parser.varType;

import org.w3c.dom.Element;

/**
 * This class is used to represent the join operator.
 * 
 */

public class Punctuate extends BinaryOperator {

	// The global timestamp attribute
	public final static String STTIMESTAMPATTR = "TIMESTAMP";

	// Track which input keeps the timer, and which keeps the data.
	// Default to 1.
	private int iDataInput = 1;
	// The timer value attribute
	private Attribute attrTimer;
	// The data value corresponding to the timer value
	private Attribute attrDataTimer;

	public Punctuate() {
	}

	public Punctuate(int iDI, Attribute aTimer, Attribute aDT) {
		this.iDataInput = iDI;
		this.attrTimer = aTimer;
		this.attrDataTimer = aDT;
	}

	/**
	 * print the operator to the standard output
	 */
	public void dump() {
		System.out.println("Punctuate : ");
	}

	/**
	 * dummy toString method
	 * 
	 * @return the String representation of the operator
	 */
	public String toString() {
		StringBuffer strBuf = new StringBuffer();
		strBuf.append("Punctuate");

		return strBuf.toString();
	}

	public int getDataInput() {
		return iDataInput;
	}

	public Attribute getTimerAttr() {
		return attrTimer;
	}

	public Attribute getDataTimer() {
		return attrDataTimer;
	}

	public void dumpAttributesInXML(StringBuffer sb) {

	}

	public void dumpChildrenInXML(StringBuffer sb) {
		sb.append(">");

		sb.append("</punctuate>");
	}

	/**
	 * @see niagara.optimizer.colombia.LogicalOp#findLogProp(ICatalog,
	 *      LogicalProperty[])
	 */
	public LogicalProperty findLogProp(ICatalog catalog, LogicalProperty[] input) {
		LogicalProperty timer = input[1 - iDataInput];
		LogicalProperty data = input[iDataInput];

		LogicalProperty result = data.copy();

		result.setHasLocal(timer.hasLocal() || data.hasLocal());
		result.setHasRemote(timer.hasRemote() || data.hasRemote());

		// The output schema is exactly the schema of the data input
		// plus the TIMESTAMP attribute

		result.addAttr(new Variable(STTIMESTAMPATTR, varType.CONTENT_VAR));

		return result;
	}

	public Op opCopy() {
		return new Punctuate(this.iDataInput, this.attrTimer,
				this.attrDataTimer);
	}

	public boolean equals(Object obj) {
		if (obj == null || !(obj instanceof Punctuate))
			return false;
		if (obj.getClass() != Punctuate.class)
			return obj.equals(this);
		Punctuate op = (Punctuate) obj;
		return iDataInput == op.iDataInput
				&& equalsNullsAllowed(attrTimer, op.attrTimer)
				&& equalsNullsAllowed(attrDataTimer, op.attrDataTimer);
	}

	public int hashCode() {
		return iDataInput ^ hashCodeNullsAllowed(attrTimer)
				^ hashCodeNullsAllowed(attrDataTimer);
	}

	public void loadFromXML(Element e, LogicalProperty[] inputProperties,
			Catalog catalog) throws InvalidPlanException {
		String id = e.getAttribute("id");

		String stDataInput = e.getAttribute("datainput");
		if (stDataInput.length() != 0)
			iDataInput = Integer.parseInt(stDataInput);

		String stAttr = e.getAttribute("timer");
		if (stAttr.length() == 0)
			throw new InvalidPlanException("Bad value for 'timer' for : " + id);
		attrTimer = Variable.findVariable(inputProperties[1 - iDataInput],
				stAttr);

		stAttr = e.getAttribute("dataattr");
		if (stAttr.length() == 0)
			throw new InvalidPlanException("Invalid datattr: " + id);
		// If they want the system timestamp to be punctuated,
		// leave this null
		if (stAttr.endsWith(STTIMESTAMPATTR) == false)
			attrDataTimer = Variable.findVariable(inputProperties[iDataInput],
					stAttr);

	}
}
