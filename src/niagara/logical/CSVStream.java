package niagara.logical;

import java.lang.reflect.Array;

import org.w3c.dom.Element;

import niagara.connection_server.Catalog;
import niagara.connection_server.InvalidPlanException;
import niagara.optimizer.colombia.Attribute;
import niagara.optimizer.colombia.Attrs;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;

public class CSVStream extends NullaryOperator 
{
	protected CSVSpec csvStreamSpec;
	protected Attribute[] variables;

	//Required zero-argument constructor
  public CSVStream() {}
	
	public CSVSpec getSpec() 
	{
		return csvStreamSpec;
	}
	
	public CSVStream(CSVSpec spec, Attribute[] variables)
	{
	  this.csvStreamSpec = spec;
	  this.variables = variables;
	}

	public Attribute[] getVariables() 
	{
		return variables;
	}

	public boolean isSourceOp() 
	{
		return true;
	}
	
	public void dump() 
	{
    System.out.println("CSVStream Operator: ");
    csvStreamSpec.dump(System.out);
    System.out.println();
  }

	public LogicalProperty findLogProp(ICatalog catalog, LogicalProperty[] input) 
	{
    return new LogicalProperty(1, new Attrs(variables, Array
        .getLength(variables)), true);
  }

  public Op opCopy() 
  {
    return new CSVStream((CSVSpec) csvStreamSpec, variables);
  }

  public boolean equals(Object obj) 
  {
    if (obj == null || !(obj instanceof CSVStream))
      return false;
    if (obj.getClass() != CSVStream.class)
      return obj.equals(this);
    CSVStream other = (CSVStream) obj;
    return csvStreamSpec.equals(other.csvStreamSpec)
        && variables.equals(other.variables);

  }

  public int hashCode() 
  {
    return csvStreamSpec.hashCode() ^ variables.hashCode();
  }

  public void loadFromXML(Element e, LogicalProperty[] inputProperties,
      Catalog catalog) throws InvalidPlanException 
  {
    String fileName = e.getAttribute("file_name");
    String[] attrNames = parseInputAttrs(e.getAttribute("attr_names"));
    String[] attrTypes = parseInputAttrs(e.getAttribute("attr_types"));
    String type = e.getAttribute("type");
    
    csvStreamSpec = new CSVSpec(fileName, attrNames, attrTypes, type);
    
    int numAttrs = Array.getLength(attrNames);
    variables = new Attribute[numAttrs];
    
    for (int i = 0; i < numAttrs; i++) 
    {
      variables[i] = new Variable(attrNames[i]);
    }
  }
  
}