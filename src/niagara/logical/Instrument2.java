package niagara.logical;

import niagara.connection_server.Catalog;
import niagara.connection_server.InvalidPlanException;
import niagara.optimizer.colombia.Attribute;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;

import org.w3c.dom.Element;

/**
 * @author rfernand
 * @version 1.0
 * 
 * The <code>Instrument2</code> logical operator drives feedback punctuation generation.
 *
 */
public class Instrument2 extends UnaryOperator {

	private long interval;
	private Boolean logging;
	private Boolean propagate;

	private Attribute tsAttr;
	private Attribute idAttr;
	
	public Instrument2() {
		interval = 0;
		logging = false;
		propagate = false;
	}

	public long getInterval(){
		return interval;
	}
	
	public Boolean getLogging() {
		return logging;
	}
	
	public Boolean getPropagate(){
		return propagate;
	}
	
	public Attribute getTsAttr() {
		return tsAttr;
	}
	
	public Attribute getIdAttr() {
		return idAttr;
	}


	public void loadFromXML(Element e, LogicalProperty[] inputProperties, Catalog catalog) throws InvalidPlanException {
		interval = Long.parseLong(e.getAttribute("interval"));
		String l = e.getAttribute("log").toString();
		String p = e.getAttribute("propagate").toString();
		
		if(l.equals("yes"))
			logging = true;
		else logging = false;
		
		if(p.equals("yes"))
			propagate = true;
		else propagate = false;

		String fAttrStr = e.getAttribute("fattrs");
		if (fAttrStr.length() == 0)
			throw new InvalidPlanException("Bad value for 'fattrs' for : "
					+ id);

		String[] punctAttrs = fAttrStr.split("[\t| ]+");
		if (punctAttrs.length != 2)
			throw new InvalidPlanException("Bad value for 'fattrs' for : "
					+ id);

		tsAttr = Variable.findVariable(inputProperties[0], punctAttrs[0]);
		idAttr = Variable.findVariable(inputProperties[0], punctAttrs[1]);
	
	}


	@Override
	public boolean equals(Object other) {

		if(other == null || !(other instanceof Instrument2))
			return false;
		if(((Instrument2)other).logging != this.logging)
			return false;
		if(((Instrument2)other).propagate != this.propagate)
			return false;
		if(((Instrument2)other).tsAttr != this.tsAttr)
			return false;
		if(((Instrument2)other).idAttr != this.idAttr)
			return false;
		
		if(other.getClass() != Instrument2.class)
			return other.equals(this);

		Instrument2 ot = (Instrument2)other;

		if(ot.interval != this.interval) 
		return false;

		return true;

	}

	
	public void dump() {
		System.out.println("Expensive :");
		System.out.println("Interval: " + String.valueOf(interval));
		System.out.println("Log: " + logging);
		System.out.println("Propagate: " + propagate);
	}

	public String toString() {
		return " expensive " ;
	}

	@Override
	public int hashCode() {
		return String.valueOf(interval).hashCode() ^ tsAttr.hashCode() ^ idAttr.hashCode();
	}

	@Override
	public Op opCopy() {
		Instrument2 op = new Instrument2();
		op.interval = interval;
		op.propagate = propagate;
		op.logging = this.logging;
		op.tsAttr = tsAttr;
		op.idAttr = idAttr;
		return op;	
	}

	@Override
	public LogicalProperty findLogProp(ICatalog catalog, LogicalProperty[] input) {
		return input[0].copy();
	}
}
