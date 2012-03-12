package niagara.data_manager;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Scanner;

import niagara.logical.CSVSpec;
import niagara.logical.CSVStream;
import niagara.optimizer.colombia.Attribute;
import niagara.optimizer.colombia.Attrs;
import niagara.optimizer.colombia.Cost;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalOp;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;
import niagara.query_engine.TupleSchema;
import niagara.utils.BaseAttr;
import niagara.utils.IntegerAttr;
import niagara.utils.LongAttr;
import niagara.utils.PEException;
import niagara.utils.Punctuation;
import niagara.utils.ShutdownException;
import niagara.utils.SinkTupleStream;
import niagara.utils.StringAttr;
import niagara.utils.TSAttr;
import niagara.utils.Tuple;
import niagara.utils.XMLAttr;
import niagara.utils.CsvReader;

/**
 * 
 *
 */
public class CSVThread extends SourceThread
{

  // Optimization-time attributes
  private Attribute[] variables;
  public CSVSpec spec;
  
  private SinkTupleStream outputStream;
  
  
  /* (non-Javadoc)
   * @see java.lang.Runnable#run()
   */
  @Override
  public void run()
  {
//    try
//    {
//      File inputFile = new File(spec.getFileName());
//      
//      processLineByLine(inputFile);
//      
//      outputStream.endOfStream();
//    }
//    catch (InterruptedException e)
//    {
//      e.printStackTrace();
//    }
//    catch (ShutdownException e)
//    {
//      e.printStackTrace();
//    }
//    catch (IOException e)
//    {
//      // TODO Auto-generated catch block
//      e.printStackTrace();
//    }
    
    try
    {
      CsvReader reader = new CsvReader(spec.getFileName());
      reader.readHeaders();
      int specNumAttrs = spec.getNumAttrs();
      
      if (reader.getHeaderCount() != specNumAttrs)
      {
        System.err.println("Number of attributes in CSV file and Query file do not match.");
        System.err.println("Spec is: ");
        for(int i=0; i<spec.getNumAttrs(); i++)
        {
          System.out.println(i + ": " + spec.getAttrName(i));
        }
        
        System.err.println("CSV File is: ");
        for(int i=0; i<reader.getHeaderCount(); i++)
        {
          System.out.println(i + ": " + reader.getHeader(i));
        }
        
        System.exit(1);
      }
      
      // Check to see that attributes match
      for(int i=0; i<reader.getHeaderCount(); i++)
      {
        if (!reader.getHeader(i).equalsIgnoreCase(spec.getAttrName(i)))
        {
          System.err.println("Attribute '" + spec.getAttrName(i) + "' does not match attribute found in file '" + reader.getHeader(i) + "'");
          
          System.err.println("Spec is: ");
          for(int j=0; j<specNumAttrs; j++)
          {
            System.out.println(j + ": " + spec.getAttrName(j));
          }
          
          System.err.println("CSV File is: ");
          for(int j=0; j<reader.getHeaderCount(); j++)
          {
            System.out.println(j + ": " + reader.getHeader(j));
          }
          
          
          System.exit(1);
        }
      }
      
      //System.out.println("Reading records...");
      int recordCount = 1;
      
      while(reader.readRecord())
      {    
        //System.out.print(recordCount++ + ": ");
        
        // Check for punctuation?
        if(reader.getRawRecord().contains("*"))
        {
          Punctuation pTuple = new Punctuation(false, reader.getHeaderCount());
          
          for (int i=0; i<specNumAttrs; i++)
          {
            if(reader.get(i).equals("*"))
            {
              pTuple.appendAttribute(BaseAttr.createWildStar(spec.getAttrType(i)));
              //System.out.print(", " + reader.get(i));
            }
            else
            {
              switch (spec.getAttrType(i))
              {            
                case Int:
                  pTuple.appendAttribute(new IntegerAttr(reader.get(i)));
                  //System.out.print(", " + reader.get(i));
                  break;
                case Long:
                  pTuple.appendAttribute(new LongAttr(reader.get(i)));
                  //System.out.print(", " + reader.get(i));
                  break;
                case TS:
                  pTuple.appendAttribute(new TSAttr(reader.get(i)));
                  //System.out.print(", " + reader.get(i));
                  break;
                case String:
                  pTuple.appendAttribute(new StringAttr(reader.get(i)));
                  //System.out.print(", " + reader.get(i));
                  break;
                case XML:
                  pTuple.appendAttribute(new XMLAttr(reader.get(i)));
                  //System.out.print(", " + reader.get(i));
                  break;
                default:
                  throw new PEException("Invalid type " + spec.getAttrType(i));  
                  
              } 
            }
          }
          
          outputStream.putTuple(pTuple);
        }
        else
        {
          Tuple outputTuple = new Tuple(false, specNumAttrs);    
          
          for (int i = 0; i<specNumAttrs; i++)
          {
            switch (spec.getAttrType(i))
            {            
              case Int:
                outputTuple.appendAttribute(new IntegerAttr(reader.get(i)));
                //System.out.print(", " + reader.get(i));
                break;
              case Long:
                outputTuple.appendAttribute(new LongAttr(reader.get(i)));
                //System.out.print(", " + reader.get(i));
                break;
              case TS:
                outputTuple.appendAttribute(new TSAttr(reader.get(i)));
                //System.out.print(", " + reader.get(i));
                break;
              case String:
                outputTuple.appendAttribute(new StringAttr(reader.get(i)));
                //System.out.print(", " + reader.get(i));
                break;
              case XML:
                outputTuple.appendAttribute(new XMLAttr(reader.get(i)));
                //System.out.print(", " + reader.get(i));
                break;
              default:
                throw new PEException("Invalid type " + spec.getAttrType(i));  
                
            }          
          }
          
          outputStream.putTuple(outputTuple);
        }
        
        
        
        //System.out.println();
      }
      
      reader.close();
      //System.out.println("Finished reading records.");
      outputStream.endOfStream();
      
    }
    catch (FileNotFoundException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    catch (IOException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    catch (InterruptedException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    catch (ShutdownException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    
  }

  /* (non-Javadoc)
   * @see niagara.query_engine.SchemaProducer#constructTupleSchema(niagara.query_engine.TupleSchema[])
   */
  @Override
  public void constructTupleSchema(TupleSchema[] inputSchemas)
  {

  }

  /* (non-Javadoc)
   * @see niagara.query_engine.SchemaProducer#getTupleSchema()
   */
  @Override
  public TupleSchema getTupleSchema() {
    TupleSchema ts = new TupleSchema();
    ts.addMappings(new Attrs(variables, Array.getLength(variables)));
    return ts;
  }

  /* (non-Javadoc)
   * @see niagara.utils.SerializableToXML#dumpAttributesInXML(java.lang.StringBuffer)
   */
  @Override
  public void dumpAttributesInXML(StringBuffer sb) {
    sb.append(" var='");
    for (int i = 0; i < variables.length; i++) {
      sb.append(variables[i].getName());
      if (i != variables.length - 1)
        sb.append(", ");
    }
    sb.append("'/>");
  }

  /* (non-Javadoc)
   * @see niagara.utils.SerializableToXML#dumpChildrenInXML(java.lang.StringBuffer)
   */
  @Override
  public void dumpChildrenInXML(StringBuffer sb) {}

  /* (non-Javadoc)
   * @see niagara.data_manager.SourceThread#plugIn(niagara.utils.SinkTupleStream, niagara.data_manager.DataManager)
   */
  @Override
  public void plugIn(SinkTupleStream outputStream, DataManager dm)
  {
    this.outputStream = outputStream;
  }

  /* (non-Javadoc)
   * @see niagara.data_manager.SourceThread#opInitFrom(niagara.optimizer.colombia.LogicalOp)
   */
  @Override
  protected void opInitFrom(LogicalOp lop)
  {
    CSVStream op = (CSVStream) lop;
    spec = op.getSpec();
    variables = op.getVariables();

  }

  /* (non-Javadoc)
   * @see niagara.optimizer.colombia.PhysicalOp#findLocalCost(niagara.optimizer.colombia.ICatalog, niagara.optimizer.colombia.LogicalProperty[])
   */
  @Override
  public Cost findLocalCost(ICatalog catalog, LogicalProperty[] InputLogProp)
  {
    // XXX vpapad: totally bogus flat cost for stream scans
    return new Cost(catalog.getDouble("stream_scan_cost"));
  }

  /* (non-Javadoc)
   * @see niagara.optimizer.colombia.Op#opCopy()
   */
  @Override
  public Op opCopy()
  {
    CSVThread csvt = new CSVThread();
    csvt.spec = spec;
    csvt.variables = variables;
    return csvt;
  }

  /* (non-Javadoc)
   * @see niagara.optimizer.colombia.Op#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object o)
  {
    if (o == null || !(o instanceof CSVThread))
      return false;
    if (o.getClass() != getClass())
      return o.equals(this);
    // XXX vpapad: Spec.equals is Object.equals
    return spec.equals(((CSVThread) o).spec)
        ^ equalsNullsAllowed(spec, ((CSVThread) o).spec);
  }

  /* (non-Javadoc)
   * @see niagara.optimizer.colombia.Op#hashCode()
   */
  @Override
  public int hashCode()
  {
 // XXX vpapad: spec's hashCode is Object.hashCode()
    return spec.hashCode();
  }
  
  public final void processLineByLine(File inputFile) throws IOException, InterruptedException, ShutdownException
  {
    Scanner scanner = new Scanner(inputFile);
    try
    {
      processFirstLine(scanner.nextLine());
      
      while(scanner.hasNextLine())
      {
        processLine(scanner.nextLine());
      }
    }
    finally
    {
      scanner.close();
    }
  }
    
  protected void processFirstLine(String firstLine) throws IOException
  {
    // Make sure the attribute names in the csv file match those in the query plan
    
    Scanner scanner = new Scanner(firstLine);
    scanner.useDelimiter(",");
    
    int numAttrs = spec.getNumAttrs();
    
    for (int i=0; i<numAttrs; i++)
    {
      String attributeInFile = scanner.next();
      String attributeInFileWithoutQuotes = attributeInFile.replaceAll("\"", "");
      
      if (!attributeInFileWithoutQuotes.equalsIgnoreCase(spec.getAttrName(i)))
      {
        System.err.println("Attribute '" + spec.getAttrName(i) + "' does not match attribute found in file '" + attributeInFile + "'");
        System.exit(1);
      }
    }
  }
    
  protected void processLine(String aLine) throws IOException, InterruptedException, ShutdownException
  {
    Scanner scanner = new Scanner(aLine);
    scanner.useDelimiter(",");
    
    int numAttrs = spec.getNumAttrs();
    Tuple outputTuple = new Tuple(false, numAttrs);
    
    for (int i = 1; i<= numAttrs; i++)
    {
      switch (spec.getAttrType(i - 1))
      {
        case Int:
          outputTuple.appendAttribute(new IntegerAttr(scanner.next()));
          break;
        case Long:
          outputTuple.appendAttribute(new LongAttr(scanner.next()));
          break;
        case TS:
          outputTuple.appendAttribute(new TSAttr(scanner.next()));
          break;
        case String:
          outputTuple.appendAttribute(new StringAttr(scanner.next()));
          break;
        case XML:
          outputTuple.appendAttribute(new XMLAttr(scanner.next()));
          break;
        default:
          throw new PEException("Invalid type " + spec.getAttrType(i));            
      }
    }
    
    outputStream.putTuple(outputTuple);
  }
}
