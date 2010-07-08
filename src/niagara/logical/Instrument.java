package niagara.logical;

import niagara.connection_server.Catalog;
import niagara.connection_server.InvalidPlanException;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;

import org.w3c.dom.Element;

/**
 * @author rfernand
 * @version 1.0
 * 
 * The <code>Instrument</code> logical operator drives feedback punctuation generation.
 *
 */
public class Instrument extends UnaryOperator {

	private long interval;
	private Boolean logging;
	private Boolean propagate;

	public Instrument() {
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
	}


	@Override
	public boolean equals(Object other) {

		if(other == null || !(other instanceof Instrument))
			return false;
		if(((Instrument)other).logging != this.logging)
			return false;
		if(((Instrument)other).propagate != this.propagate)
			return false;
		if(other.getClass() != Instrument.class)
			return other.equals(this);

		Instrument ot = (Instrument)other;

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
		return String.valueOf(interval).hashCode();
	}

	@Override
	public Op opCopy() {
		Instrument op = new Instrument();
		op.interval = interval;
		op.propagate = propagate;
		op.logging = this.logging;
		return op;	
	}

	@Override
	public LogicalProperty findLogProp(ICatalog catalog, LogicalProperty[] input) {
		return input[0].copy();
	}
}
