/* PartitionAvg.java, Jenny$Id */
package niagara.logical;

import org.w3c.dom.Element;

import niagara.connection_server.InvalidPlanException;
import niagara.optimizer.colombia.Attribute;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;
import niagara.xmlql_parser.syntax_tree.*;

import java.util.Vector;
import java.util.StringTokenizer;

public class PartitionAvg extends PartitionGroup {
	private Attribute avgAttribute;
	private Integer range = null;
	private Boolean landmark = null;

	public PartitionAvg() {}
    
	public PartitionAvg(skolem skolemAttributes, Attribute avgAttribute) {
		super(skolemAttributes);
		this.avgAttribute = avgAttribute;
	}

/*	public Double getEmptyGroupValue() {
	return emptyGroupValue;
	}*/
	
	public Attribute getAvgAttribute() {
		return avgAttribute;
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
		PartitionAvg op = new PartitionAvg(skolemAttributes, avgAttribute);
		op.landmark = landmark;
		op.range = range;
		return op;
	}
    
	public boolean equals(Object o) {
		if (o == null || !(o instanceof PartitionAvg))
			return false;
		if (o.getClass() != PartitionAvg.class)
			return o.equals(this);
		PartitionAvg ia = (PartitionAvg) o;
		if (range != null)
			if (!range.equals(ia.range))
				return false;
		return skolemAttributes.equals(ia.skolemAttributes)
			&& avgAttribute.equals(ia.avgAttribute)
			&& landmark.equals(ia.landmark);
			//&& range.equals(ia.range);
	}
    
	public int hashCode() {
		return skolemAttributes.hashCode() ^ avgAttribute.hashCode();
	}
 
	public void loadFromXML(Element e, LogicalProperty[] inputProperties)
		throws InvalidPlanException {
		String id = e.getAttribute("id");
		String groupby = e.getAttribute("groupby");
		String avgattr = e.getAttribute("avgattr");
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

		avgAttribute = Variable.findVariable(inputLogProp, avgattr);
		setSkolemAttributes(new skolem(id, groupbyAttrs));
		if (rangeAttr != "")
			range = Integer.valueOf(rangeAttr);
		landmark = Boolean.valueOf(landmarkAttr);
	}
}

