/* PartitionMax.java, Jenny$Id */
package niagara.logical;

import org.w3c.dom.Element;

import niagara.connection_server.InvalidPlanException;
import niagara.optimizer.colombia.Attribute;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;
import niagara.xmlql_parser.syntax_tree.*;

import java.util.Vector;
import java.util.StringTokenizer;

public class PartitionMax extends PartitionGroup {
	private Attribute maxAttribute;
	private Double emptyGroupValue;	
	private Integer range;
	private Boolean landmark;

	public PartitionMax() {}
    
	public PartitionMax(skolem skolemAttributes, Attribute maxAttribute) {
		super(skolemAttributes);
		this.maxAttribute = maxAttribute;
	}

	public Double getEmptyGroupValue() {
	return emptyGroupValue;
	}
	
	public Attribute getMaxAttribute() {
		return maxAttribute;
	}
	
	public Boolean landmark() {
		return landmark;
	}
	
	public Integer getRange () {
		return range;
	}
	
	public boolean outputOldValue() {
		return true;
	}

	public void dump() {System.out.println(getName());}

	public Op opCopy() {
		PartitionMax op = new PartitionMax(skolemAttributes, maxAttribute);
		op.emptyGroupValue = emptyGroupValue;
		op.landmark = landmark;
		op.range = range;
		return op;
	}
    
	public boolean equals(Object o) {
		if (o == null || !(o instanceof PartitionMax))
			return false;
		if (o.getClass() != PartitionMax.class)
			return o.equals(this);
		PartitionMax ia = (PartitionMax) o;
		if (range != null)
			if (!range.equals(ia.range))
				return false;
		
		return skolemAttributes.equals(ia.skolemAttributes)
			&& maxAttribute.equals(ia.maxAttribute)
			&& landmark.equals(ia.landmark);

	}
    
	public int hashCode() {
		return skolemAttributes.hashCode() ^ maxAttribute.hashCode();
	}
 
	public void loadFromXML(Element e, LogicalProperty[] inputProperties)
		throws InvalidPlanException {
		String id = e.getAttribute("id");
		String groupby = e.getAttribute("groupby");
		String maxattr = e.getAttribute("maxattr");
		String emptyGroupValueAttr = e.getAttribute("emptygroupvalue");
		String rangeAttr = e.getAttribute("range");
		String landmarkAttr = e.getAttribute("landmark");

		LogicalProperty inputLogProp = inputProperties[0];
            
		// Parse the groupby attribute to see what to group on
		Vector groupbyAttrs = new Vector();
		StringTokenizer st = new StringTokenizer(groupby);
		while (st.hasMoreTokens()) {
			String varName = st.nextToken();
			Attribute attr = Variable.findVariable(inputLogProp, varName);
			groupbyAttrs.addElement(attr);
		}

		maxAttribute = Variable.findVariable(inputLogProp, maxattr);
		setSkolemAttributes(new skolem(id, groupbyAttrs));
		emptyGroupValue = Double.valueOf(emptyGroupValueAttr);
		if (rangeAttr != "")
			range = Integer.valueOf(rangeAttr);
		landmark = Boolean.valueOf(landmarkAttr);
	}
}

