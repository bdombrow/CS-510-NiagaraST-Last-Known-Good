/**********************************************************************
  $Id: PhysicalPunctQC.java,v 1.4 2007/05/24 03:46:49 jinli Exp $


  NIAGARA -- Net Data Management System                                 
                                                                        
  Copyright (c)    Computer Sciences Department, University of          
                       Wisconsin -- Madison                             
  All Rights Reserved.                                                  
                                                                        
  Permission to use, copy, modify and distribute this software and      
  its documentation is hereby granted, provided that both the           
  copyright notice and this permission notice appear in all copies      
  of the software, derivative works or modified versions, and any       
  portions thereof, and that both notices appear in supporting          
  documentation.                                                        
                                                                        
  THE AUTHORS AND THE COMPUTER SCIENCES DEPARTMENT OF THE UNIVERSITY    
  OF WISCONSIN - MADISON ALLOW FREE USE OF THIS SOFTWARE IN ITS "        
  AS IS" CONDITION, AND THEY DISCLAIM ANY LIABILITY OF ANY KIND         
  FOR ANY DAMAGES WHATSOEVER RESULTING FROM THE USE OF THIS SOFTWARE.   
                                                                        
  This software was developed with support by DARPA through             
   Rome Research Laboratory Contract No. F30602-97-2-0247.  
**********************************************************************/


package niagara.physical;

import java.util.ArrayList;
import java.util.Vector;
import java.sql.Timestamp;
import java.util.GregorianCalendar;
import java.util.Calendar;

import org.w3c.dom.*;

import niagara.logical.PunctQC;
import niagara.logical.PunctSpec;
import niagara.logical.PrefetchSpec;
import niagara.logical.SimilaritySpec;
import niagara.logical.SimilaritySpec.SimilarityType;
import niagara.optimizer.colombia.*;
import niagara.query_engine.*;
import niagara.utils.*;

/**
 * <code>PhysicalPunctuateOperator</code> implements a punctuate operator for
 * an incoming stream;
 *
 * @see PhysicalOperator
 */
public class PhysicalPunctQC extends PhysicalOperator {
  
    // operator does not block on any source streams
    private static final boolean[] blockingSourceStreams = { false, false };

    // two input streams - one is data from db
    // second is punctuation from stream
    // assume index of dbdata is 0
    // index of punct is 1
    
	// attribute to punctuate on
    private Attribute pAttr;
	private PunctSpec pSpec;
    //private SimilaritySpec sSpec;
    //private PrefetchSpec pfSpec;
    
    // punctuating attribute of the input stream, which we rely 
    // on to retrieve data from db
    private Attribute spAttr;
    
    private int [] pIdx; // index of punctuation attr
    
	// last timestamp - for on change punctuation
	private long lastts;

    // keep track of data types to be used to create
    // punctuation
    BaseAttr.Type dataType[];      
    
    // queryInterval should specify the coverage of each database query; 
    //private int queryGranularity = 1*20; //each database query covers 100 second data;
    
    // the time attribute of db data  
    private String timeAttr;
    
    private String queryString;
    
    private Long lastPunct = Long.MIN_VALUE;
    
    // private int count = 0;
    // high watermark on db data;
    // private long highWatermark;
    

    public PhysicalPunctQC() {
        setBlockingSourceStreams(blockingSourceStreams);
    }

    // probably should take punct index instead
    // index vs attr -get attr in load from xml, probably
    // can't get index there
    // probably take attr here, and convert to idx in
    // opInitialize
    public PhysicalPunctQC(Attribute pAttr, String timeAttr, String queryString,
				PunctSpec pSpec) {
        setBlockingSourceStreams(blockingSourceStreams);

				this.pAttr = pAttr;
				this.pSpec = pSpec;
				//this.sSpec = sSpec;
				//this.pfSpec = pfSpec;
				this.timeAttr = timeAttr;
				this.queryString = queryString;
    }
    
    public void setSPAttr(Attribute spAttr) {
    	this.spAttr = spAttr;
    }
    
    public void opInitFrom(LogicalOp logicalOperator) {
				PunctQC pop = (PunctQC) logicalOperator;
				pAttr = pop.getPunctAttr();
				pSpec = pop.getPunctSpec();
				//sSpec = pop.getSimilaritySpec();
				//pfSpec = pop.getPrefetchSpec();
				spAttr = pop.getStreamPunctAttr();
				timeAttr = pop.getTimeAttr();
				queryString = pop.getQueryString();
				
    }

    public void opInitialize() {
        // initialize ts to -1 so we can detect the first
        // tuple 
		lastts= Long.MIN_VALUE;

		pIdx = new int[2];
		
        // get index of attribute we are punctuating on
        pIdx[0] = inputTupleSchemas[0].getPosition(pAttr.getName());
        pIdx[1] = inputTupleSchemas[1].getPosition(spAttr.getName());

        // get the data types of all attributes
        int numAttrs = inputTupleSchemas[0].getLength();
        dataType = new BaseAttr.Type[numAttrs];
        for(int i = 0; i<numAttrs; i++) {
          dataType[i] = inputTupleSchemas[0].getVariable(i).getDataType();
        }
       
        // print to verify i've got the inputs set up right
        System.out.println("PunctQC - pidx[0]: " + pIdx[0]);
        System.out.println("PunctQC - pidx[1]: " + pIdx[1]);
        System.out.println("PunctQC - len input 0 (db): " + inputTupleSchemas[0].getLength());
        System.out.println("PunctQC - first attr input 0: " + inputTupleSchemas[0].getVariable(0).getName());
        System.out.println("PunctQC - len input 1 (stream punct): " + inputTupleSchemas[1].getLength());
        System.out.println("PunctQC - first attr input 1: " + inputTupleSchemas[1].getVariable(0).getName());
    }

    /**
     * This function processes a tuple element read from a source stream
     * when the operator is non-blocking. This over-rides the corresponding
     * function in the base class.
     *
     * @param inputTuple The tuple element read from a source stream
     * @param streamId The source stream from which the tuple was read
     *
     * @exception ShutdownException query shutdown by user or execution error
     */
    protected void processTuple (
						 Tuple inputTuple,
						 int streamId)
						 
			throws ShutdownException, InterruptedException, OperatorDoneException {

      if(streamId == 1) {
        //System.out.println("PunctQC dropping data from stream side");
          return;
    	
    	/**
    	 * JUST FOR TEST PURPOSE. EVERY TUPLE IS USED AS A PUNCTUATION; 
    	 */
    	  
          /*long start, end;
          long ts = getTupleTimestamp(inputTuple, 1);

          	start = ts;
          	end = start + queryGranularity;
      
          	String startTS = new Timestamp(start*1000).toString();
          	startTS = startTS.substring(0, startTS.length()-2);
      
          	String endTS = new Timestamp(end*1000).toString();
          	endTS = endTS.substring(0, endTS.length()-2);
          	
          	IntegerAttr sensorid = (IntegerAttr)inputTuple.getAttribute(5);
          	//String ctrlMsg = String.valueOf(start)+"\t"+String.valueOf(end);
          	String ctrlMsg = startTS + "\t" + endTS +"\t"+sensorid.toASCII();
          	System.err.println(ctrlMsg);

          	int ctrlFlag = CtrlFlags.CHANGE_QUERY;
          	System.err.println("putting a CHANGE_QUERY control msg");
          	sendCtrlMsgUpStream(ctrlFlag, ctrlMsg, 0);*/
      }
			
			// could enforce punctuation here - for now assume
			// input is ordered - jenny is going to hate me when
			// she sees this!!

			if(lastts == Long.MIN_VALUE) {
				// this is the first tuple
				// record the tuple's timestamp 
				// grab a copy of the tuple for a punctuation
				// template
				// put the tuple in the output stream and continue
				lastts = getTupleTimestamp(inputTuple, 0);
	      //tupleDataSample = inputTuple.copy();
				putTuple(inputTuple, 0);
				
			} else {
				// compare tuple timestamp to see if it has changed
				// if changed put punct, then tuple
				// else just put tuple
				long newts = getTupleTimestamp(inputTuple, 0);
				if(newts != lastts) {
					if (newts < lastts)
						System.err.println("data from db is not ordered *******************");

					putTuple(createPunctuation(), 0);
					lastts = newts;
				} 
				putTuple(inputTuple,0);
			}
		}

    /*
     * Assume timestamp is in seconds;
     */ 
    private long getTupleTimestamp(Tuple inputTuple, int streamId){
      assert (inputTuple.getAttribute(pIdx[streamId])).getClass() == TSAttr.class : "bad punct attr type";
      TSAttr tsAttr = (TSAttr)inputTuple.getAttribute(pIdx[streamId]);
      return tsAttr.extractEpoch();
    }


    /**
     * This function generates a punctuation based on the last ts value
     * using the template generated by setupDataTemplate
     */
    private Punctuation createPunctuation() {
	    // Create a punctuation based on the last timestamp value
      // punct should be last ts value plus *s for all other
      // attrs
      //assert lastts > 0 : "uh-oh negative ts value!! KT ";

      // HERE - FIX - DON'T KNOW HOW TO CREATE NEW * ATTRS
      Punctuation punct = new Punctuation(false); // what does false mean?
      for (int i=0; i<dataType.length; i++) { 
        if (i != pIdx[0]) {
          punct.appendAttribute(BaseAttr.createWildStar(dataType[i])); 
        } else {
          punct.appendAttribute(new TSAttr(lastts));	// ???
        }
      }
      return punct;
	  }


    /**
     * This function handles punctuations for the given operator. For
     * Punctuate, we can simply output any incoming punctuation.
     *
     * @param tuple The current input tuple to examine.
     * @param streamId The id of the source streams the partial result of
     *                 which are to be removed.
     *
     */

    protected void processPunctuation(Punctuation tuple,
				      int streamId)
      throws ShutdownException, InterruptedException {
    	
        // FIX - process punct from stream
        assert streamId == 1 : "Shouldn't get punct from db side";
        if (streamId == 0) {
        	System.err.println("DO SOMETHING HERE - PROCESS PUNCT FROM DB SIDE");
        	return;
        }
        
        long ts = getTupleTimestamp(tuple, 1);
        synchronized (lastPunct) {
        	lastPunct = ts; 
        }
        
    	int ctrlFlag = CtrlFlags.CHANGE_QUERY;

    	sendCtrlMsgUpStream(ctrlFlag, String.valueOf(ts), 0);

    	
        /*long start, end;
        
        int prefetch = pfSpec.getPrefetchVal();
        
        if (highWatermark < ts+prefetch) {
            int queryCoverage = pfSpec.getCoverage();

            if (count == 0) {
            	start = ts;
            	//start = ts - sSpec.getNumOfMins()*60;
            	count++;
            } else
            	start = highWatermark;
        	end = (start + queryCoverage);
        	
        	String ctrlMsg = start + " " + end; //newQuery(start, end);
    
        	System.err.println(ctrlMsg);
        	highWatermark = end;
        	int ctrlFlag = CtrlFlags.CHANGE_QUERY;

        	sendCtrlMsgUpStream(ctrlFlag, ctrlMsg, 0);
        }       */ 
    }

	/**
	 * @param start
	 * @param end
	 * @return
	 */
	/*private String newQuery(long start, long end) {
		Calendar calendar = GregorianCalendar.getInstance();
		
		//calendar.setTimeInMillis(start*1000);
		//String startTS = calendar.getTime().toString();
		//calendar.setTimeInMillis(end*1000);
		//String endTS = calendar.getTime().toString();

		System.err.println("start at: "+start+"   end at: "+end);
		StringBuffer query = new StringBuffer("(");
		
		int numDays = sSpec.getNumOfDays();
		int numMins = sSpec.getNumOfMins();
		
		//queryString.replace ("NUMOFDAYS", "interval '1 day'");
		//query.append(queryString);
		String upper, lower;
		switch (sSpec.getSimilarityType()) {
		case AllDays:
			for (int i = 1; i <= numDays; i++) {
				if (i > 1)
					query.append(" union all ");
				
				//query.append(queryString.replace("NUMOFDAYS", " '" +i+" days'"));
				
				calendar.setTimeInMillis(start*1000);											
				calendar.roll(Calendar.DAY_OF_YEAR, -i);				
				//calendar.add(Calendar.MINUTE, -numMins);				
				lower = calendar.getTime().toString();
				
				calendar.setTimeInMillis(end*1000);			
				calendar.roll(Calendar.DAY_OF_YEAR, -i);				
				//calendar.add(Calendar.MINUTE, numMins);				
				upper = calendar.getTime().toString();
				
				query.append(queryString.replace("NUMOFDAYS", " '" +i+" days'").replace("TIMEPREDICATE", timePredicate(upper, lower)));
				
			}
			query.append(") order by panetime");
			break;
			
		case SameDayOfWeek:
			for (int i = 1; i <= numDays; i++) {				
				//query.append(queryString.replace("NUMOFDAYS", " '" +i+" days'"));
				
				calendar.setTimeInMillis(start*1000);											
				calendar.roll(Calendar.DAY_OF_YEAR, -i*DAYS_PER_WEEK);				
				//calendar.add(Calendar.MINUTE, -numMins);				
				lower = calendar.getTime().toString();
				
				calendar.setTimeInMillis(end*1000);			
				calendar.roll(Calendar.DAY_OF_YEAR, -i*DAYS_PER_WEEK);				
				//calendar.add(Calendar.MINUTE, numMins);				
				upper = calendar.getTime().toString();
				
				if (i > 1)
					query.append(" union all ");

				query.append(queryString.replace("NUMOFDAYS", " '" +i*DAYS_PER_WEEK+" days'").replace("TIMEPREDICATE", timePredicate(upper, lower)));
				
			}
			query.append(") order by panetime");
			break;
	
		case WeekDays:	
			int i = 1, offset = 1, weekday;
			while (i <= numDays) {
				calendar.setTimeInMillis(start*1000);											
				calendar.roll(Calendar.DAY_OF_YEAR, -offset);
				weekday = calendar.get(Calendar.DAY_OF_WEEK);
				if (weekday == Calendar.SUNDAY || weekday == Calendar.SATURDAY) {
					offset++;
					continue;
				}
				//calendar.add(Calendar.MINUTE, -numMins);				
				lower = calendar.getTime().toString();
				
				calendar.setTimeInMillis(end*1000);			
				calendar.roll(Calendar.DAY_OF_YEAR, -offset);
				if (weekday == Calendar.SUNDAY || weekday == Calendar.SATURDAY) {
					offset++;
					continue;
				}
				//calendar.add(Calendar.MINUTE, numMins);				
				upper = calendar.getTime().toString();

				if (i > 1)
					query.append(" union all ");

				query.append(queryString.replace("NUMOFDAYS", " '" +offset+" days'").replace("TIMEPREDICATE", timePredicate(upper, lower)));
				i++;
				offset++;
			}
			query.append(") order by panetime");
			break;
			
		default:
			System.err.println("unsupported similarity type: "+sSpec.getSimilarityType().toString());
		
		}
		return query.toString();
	}*/
    
    public void streamClosed( int streamId) 
    throws ShutdownException {
    try {
    if (streamId == 1) // the stream ends;
    	//send a READY_TO_FINISH msg to its left input source, which is the dbthread;
    	System.err.println("the stream is going to end. Sending out the Shutdown msg ..");
    	sendCtrlMsgUpStream(CtrlFlags.READY_TO_FINISH, null, 0);
    } catch (InterruptedException e) {
    	;
    }
    }


    public boolean isStateful() {
      return false;
    }
    
    /**
     * @see niagara.optimizer.colombia.PhysicalOp#FindLocalCost(LogicalProperty, LogicalProperty[])
     */
    public Cost findLocalCost(ICatalog catalog, LogicalProperty[] inputLogProp) {
        double trc = catalog.getDouble("tuple_reading_cost");
        double sumCards = 0;
        for (int i = 0; i < inputLogProp.length; i++)
            sumCards += inputLogProp[i].getCardinality();
        return new Cost(trc * sumCards);
    }

    /**
     * @see niagara.optimizer.colombia.Op#copy()
     */
    public Op opCopy() {
        PhysicalPunctQC another = new  PhysicalPunctQC(pAttr, timeAttr, queryString, pSpec);
        another.setSPAttr(spAttr);
        return another;
    }

    // HERE
    /**
     * @see niagara.query_engine.SchemaProducer#constructTupleSchema(TupleSchema[])
     */
    public void constructTupleSchema(TupleSchema[] inputSchemas) {
      inputTupleSchemas = inputSchemas;
      outputTupleSchema = inputTupleSchemas[0];
    }

    /**
     * @see java.lang.Object#equals(Object)
     */
    public boolean equals(Object o) {
      // FIX - this is garbage
        if (o == null || !(o instanceof PhysicalPunctQC))
            return false;
        if (o.getClass() != PhysicalPunctQC.class)
            return o.equals(this);
        return getArity() == ((PhysicalPunctuate) o).getArity();
    }

    public int hashCode() {
      // FIX - this is garbage
        return getArity(); // what is this ?? 
    }
    
    public void getInstrumentationValues(ArrayList<String> instrumentationNames, ArrayList<Object> instrumentationValues) {
    	instrumentationNames.add("now");
    	synchronized (lastPunct) {
    		instrumentationValues.add(String.valueOf(lastPunct));
    	}
    
    	//super.getInstrumentationValues(instrumentationNames, instrumentationValues);
    }
}
    
