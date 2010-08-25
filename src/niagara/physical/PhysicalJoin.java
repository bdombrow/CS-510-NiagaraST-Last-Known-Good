package niagara.physical;

import java.util.ArrayList;
import java.util.Iterator;

import niagara.logical.Join;
import niagara.logical.predicates.Predicate;
import niagara.optimizer.colombia.LogicalOp;
import niagara.query_engine.TupleSchema;
import niagara.utils.ControlFlag;
import niagara.utils.FeedbackType;
import niagara.utils.Guard;
import niagara.utils.PEException;
import niagara.utils.ShutdownException;
import niagara.utils.Tuple;

import niagara.utils.FeedbackPunctuation;

import niagara.utils.Log;
import niagara.utils.FeedbackPunctuation.Comparator;

/** Common functionality for join implementations */
abstract public class PhysicalJoin extends PhysicalOperator {
	/** Are we projecting attributes away? */
	protected boolean projecting;
	/** Maps shared attribute positions between incoming and outgoing tuples */
	protected int[] leftAttributeMap;
	protected int[] rightAttributeMap;

	/** All predicates */
	protected Predicate joinPredicate;
	protected boolean extensionJoin[];
	
//	 Propagate
	Boolean propagate = false;

//	 Exploit
	Boolean exploit = false;
	
	// FP attributes
	String fattrsL = "";
	String fattrsR = "";
	
	// FP attribute names and positions
	int[] positions;
	String[] names;
	
//	 Feedback
	private Guard outputGuard = new Guard();
	
//	 logging test
	int tupleOut = 0;
	int tupleDrop = 0;
	
	public final void opInitFrom(LogicalOp logicalOperator) {
		
		//System.out.println(this.getName() + " # ");
		
		Join join = (Join) logicalOperator;
		initJoin(join);
		
		propagate = ((Join) logicalOperator).getPropagate();
		exploit = ((Join) logicalOperator).getExploit();
		logging = ((Join) logicalOperator).getLogging();
		if (logging) {
			log = new Log(this.getName());
		}
		
		fattrsL = ((Join) logicalOperator).getFAttrsL();
		fattrsR = ((Join) logicalOperator).getFAttrsR();
		
	}

	abstract protected void initJoin(Join join);

	protected void produceTuple(Tuple left, Tuple right)
			throws ShutdownException, InterruptedException {
		Tuple result;
		if (projecting) {
			result = left.copy(outputTupleSchema.getLength(), leftAttributeMap);
			right.copyInto(result, leftAttributeMap.length, rightAttributeMap);
		} else {
			result = left.copy(outputTupleSchema.getLength());
			result.appendTuple(right);
		}

		if (exploit) 
		{
			// get attribute positions from tuple to check against guards
//			int[] positions = new int[2];
//			//String[] names = { "timestamp", "milepost" };
//			String[] names = {fattrsL, fattrsR}; // fattrs.split(" ");
//
//			for (int i = 0; i < names.length; i++) {
//				//positions[i] = inputTupleSchemas[0].getPosition(names[i]);
//				positions[i] = outputTupleSchema.getPosition(names[i]);
//			}

			// check against guards
			Boolean guardMatch = false;
			for (FeedbackPunctuation fp : outputGuard.elements()) {
				guardMatch = guardMatch
						|| fp
								.match(positions, result
										.getTuple());
			}

			if (!guardMatch) {
				putTuple(result, 0);
				tupleOut++;
			}
		} 
		else
		{
			putTuple(result, 0);
			tupleOut++;
		}
		
						
		if (logging) {
			//tupleOut++;
			log.Update("TupleOut", String.valueOf(tupleOut));
		}
	//System.out.println(this.getName() + tupleOut);
		
	}

	public boolean equals(Object o) {
		if (o == null || !(o.getClass().equals(getClass())))
			return false;
		
		if (o == null || !(o instanceof PhysicalBucket))
			return false;
		if (o.getClass() != PhysicalBucket.class)
			return false;
		if(((PhysicalJoin)o).propagate != propagate)
			return false;
		if(((PhysicalJoin)o).exploit != exploit)
			return false;
		if(!((PhysicalJoin)o).fattrsL.equals(fattrsL))
			return false;
		if(!((PhysicalJoin)o).fattrsL.equals(fattrsL))
			return false;
		if(((PhysicalJoin)o).logging != logging)
			return false;
		
		
		PhysicalJoin join = (PhysicalJoin) o;
		
		return joinPredicate.equals(join.joinPredicate)
				&& equalsNullsAllowed(getLogProp(), join.getLogProp());
	}

	public int hashCode() {
		return joinPredicate.hashCode() ^ hashCodeNullsAllowed(getLogProp()) ^ propagate.hashCode() ^ exploit.hashCode() ^ logging.hashCode() ^ fattrsL.hashCode() ^ fattrsR.hashCode();
	}

	public void constructTupleSchema(TupleSchema[] inputSchemas) {
		inputTupleSchemas = inputSchemas;

		// We can't depend on our logical property's attribute
		// order, since commutes etc. may have changed it
		outputTupleSchema = inputSchemas[0].copy();
		outputTupleSchema.addMappings(inputSchemas[1].getAttrs());

		projecting = inputSchemas[0].getLength() + inputSchemas[1].getLength() > outputTupleSchema
				.getLength();
		if (projecting) {
			leftAttributeMap = inputSchemas[0].mapPositions(outputTupleSchema);
			rightAttributeMap = inputSchemas[1].mapPositions(outputTupleSchema);
		}
	}

	protected void initExtensionJoin(Join join) {
		if (extensionJoin == null)
			extensionJoin = new boolean[2];

		switch (join.getExtensionJoin()) {
		case Join.LEFT:
			extensionJoin[0] = true;
			extensionJoin[1] = false;
			break;
		case Join.RIGHT:
			extensionJoin[0] = false;
			extensionJoin[1] = true;
			break;

		case Join.BOTH:
			extensionJoin[0] = true;
			extensionJoin[1] = true;
			break;

		case Join.NONE:
			extensionJoin[0] = false;
			extensionJoin[1] = false;
			break;
		default:
			throw new PEException("Invalid extension join value");
		}
		return;
	}

	/**
	 * @see niagara.utils.SerializableToXML#dumpChildrenInXML(StringBuffer)
	 */
	public void dumpChildrenInXML(StringBuffer sb) {
		sb.append(">");
		joinPredicate.toXML(sb);
		sb.append("</").append(getName()).append(">");
	}

	void processCtrlMsgFromSink(ArrayList ctrl, int streamId)
	throws java.lang.InterruptedException, ShutdownException {
//		downstream control message is GET_PARTIAL
//		We should not get SYNCH_PARTIAL, END_PARTIAL, EOS or NULLFLAG
//		REQ_BUF_FLUSH is handled inside SinkTupleStream
//		here (SHUTDOWN is handled with exceptions)

		if (ctrl == null)
			return;

		ControlFlag ctrlFlag = (ControlFlag) ctrl.get(0);

		switch (ctrlFlag) {
		case GET_PARTIAL:
			processGetPartialFromSink(streamId);
			break;
		case MESSAGE:
			FeedbackPunctuation fp = (FeedbackPunctuation) ctrl.get(2);
			FeedbackPunctuation fpSend = new FeedbackPunctuation(fp.Type(),fp.Variables(),fp.Comparators(),fp.Values());
	
//			 get attribute positions from tuple to check against guards
			names = new String[fp.Variables().size()];
			names = fp.Variables().toArray(names);

			// get positions
			positions = new int[fp.Variables().size()];
			for (int i = 0; i < names.length; i++) {
				positions[i] = outputTupleSchema.getPosition(names[i]);
			}			
		
			//System.out.println(this.getName() + "***Got message: " + fp.toString() + " with propagate =  " + propagate);
			
			if(exploit)
				outputGuard.add(fp);

			if (propagate) {
				
				/*
				 * We need to split the feedback here.
				 */
						
				FeedbackPunctuation fpSend0 = null; 
				FeedbackPunctuation fpSend1 = null; 
		
								
				fpSend0 = split(fp,fattrsL);
				
				fpSend1 = split(fp,fattrsR);
				
				
				sendFeedbackPunctuation(fpSend0, 0);
				//System.out.println(this.getName() + "Left  -> " + fpSend0.toString());
				sendFeedbackPunctuation(fpSend1, 1);
				//System.out.println(this.getName() + "Right -> " + fpSend1.toString());

			}
			break;
			
		default:
			assert false : "KT unexpected control message from sink "
				+ ctrlFlag.flagName();
		}
	}
	
	FeedbackPunctuation split(FeedbackPunctuation fp,String fattrsList)
	{
		 FeedbackType _type;
		 ArrayList<String> _variables = new ArrayList<String>();
		 ArrayList<Comparator> _comparators = new ArrayList<Comparator>();
		 ArrayList<String> _values = new ArrayList<String>();
					
		Iterator<String> iter = fp.Variables().iterator();
		
		int posPointer = 0;
		
		String fattrNames[] = fattrsList.split(" ");
		
		while(iter.hasNext())
		{
			String var = iter.next();			
			for(String s:fattrNames)
			{
				if(var.equals(s))
				{
					_variables.add(var);
					_comparators.add(fp.Comparators().get(posPointer));
					_values.add(fp.getValue(posPointer));
				}
			}	
			
			posPointer++;			
		}
		
		_variables.trimToSize();
		_comparators.trimToSize();
		_values.trimToSize();

		return new FeedbackPunctuation(fp.Type(),_variables,_comparators,_values);	
						
	}
	
	
	
	// amit: this version will go.
	void split(FeedbackPunctuation fp,String fattrsL,String fattrsR,FeedbackPunctuation fpLeft,FeedbackPunctuation fpRight)
	{
		 FeedbackType _typeL;
		 ArrayList<String> _variablesL = new ArrayList<String>();
		 ArrayList<Comparator> _comparatorsL = new ArrayList<Comparator>();
		 ArrayList<String> _valuesL = new ArrayList<String>();
		
		 FeedbackType _typeR;
		 ArrayList<String> _variablesR = new ArrayList<String>();
		 ArrayList<Comparator> _comparatorsR = new ArrayList<Comparator>();
		 ArrayList<String> _valuesR = new ArrayList<String>();
		
		
		Iterator<String> iter = fp.Variables().iterator();
		
		int posPointer = 0;
		
		while(iter.hasNext())
		{
			String var = iter.next();
			
			
			if(fattrsL.contains(var))
			{
				_variablesL.add(var);
				_comparatorsL.add(fp.Comparators().get(posPointer));
				_valuesL.add(fp.getValue(posPointer));				
			}
			
			if(fattrsR.contains(var))
			{
				_variablesR.add(var);
				_comparatorsR.add(fp.Comparators().get(posPointer));
				_valuesR.add(fp.getValue(posPointer));				
			}
			
			posPointer++;			
		}

		fpLeft = new FeedbackPunctuation(fp.Type(),_variablesL,_comparatorsL,_valuesL);
		
		fpRight = new FeedbackPunctuation(fp.Type(),_variablesR,_comparatorsR,_valuesR);
				
	}
	

}
