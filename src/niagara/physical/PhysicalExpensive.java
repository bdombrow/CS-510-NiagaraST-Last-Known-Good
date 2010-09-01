package niagara.physical;

import java.util.ArrayList;
import java.util.Iterator;

import niagara.logical.Expensive;
import niagara.optimizer.colombia.Cost;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalOp;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;
import niagara.utils.ControlFlag;
import niagara.utils.FeedbackPunctuation;
import niagara.utils.Guard;
import niagara.utils.Log;
import niagara.utils.Punctuation;
import niagara.utils.ShutdownException;
import niagara.utils.SourceTupleStream;
import niagara.utils.Tuple;

/**
 * 
 * @author rfernand
 * @version 1.0
 * 
 *          The <code>PhysicalExpensive</code> operator is essentially the
 *          identity but with a fixed cost per tuple and blocking features. It
 *          holds state and works on the tuples on a separate thread.
 * 
 */

public class PhysicalExpensive extends PhysicalOperator {

	private static final boolean[] blockingSourceStreams = { true };

	private long cost;
	private int outCount;
	private int PunctsOut;
	ArrayList<Tuple> toDo;
	WorkerThread w;
	boolean readyToFinish;
	boolean started;
	
	/* Feedback */
	
	// Guard	
	protected Guard outputGuard;
	
	// Input Guard
	protected Guard inputGuard;
	
	// Propagate
	Boolean propagate = false;

	// Exploit
	Boolean exploit = false;
	int[] positions;
	String[] names;
	
	public PhysicalExpensive() {
		setBlockingSourceStreams(blockingSourceStreams);
		cost = 0;
		toDo = new ArrayList<Tuple>();
		readyToFinish = false;
		started = false;
		
		outputGuard = new Guard();
		inputGuard = new Guard();
	}

	@Override
	public boolean isStateful() {
		return false;
	}

	@Override
	protected void opInitFrom(LogicalOp op) {
		cost = ((Expensive) op).getCost();
		logging = ((Expensive) op).getLogging();
		if (logging) {
			log = new Log(this.getName());
			outCount = 0;
		}
		propagate = ((Expensive) op).getPropagate();
		exploit = ((Expensive) op).getExploit();
	}

	@Override
	public Cost findLocalCost(ICatalog catalog, LogicalProperty[] InputLogProp) {
		float InputCard = InputLogProp[0].getCardinality();
		Cost cost = new Cost(InputCard
				* catalog.getDouble("tuple_reading_cost"));
		return cost;
	}

	@Override
	public boolean equals(Object other) {

		if (other.getClass() != this.getClass())
			return false;
		if (other == null)
			return false;
		if (((PhysicalExpensive) other).cost != this.cost)
			return false;
		if (((PhysicalExpensive) other).log != this.log)
			return false;
		if(((PhysicalExpensive)other).exploit != exploit)
			return false;
		if(((PhysicalExpensive)other).propagate != propagate)
			return false;
		if(((PhysicalExpensive)other).outputGuard != outputGuard)
			return false;
		if(((PhysicalExpensive)other).inputGuard != inputGuard)
			return false;
		

		return true;
	}

	@Override
	public int hashCode() {
		return String.valueOf(cost).hashCode() ^ logging.hashCode();
	}

	@Override
	public Op opCopy() {
		PhysicalExpensive pr = new PhysicalExpensive();
		pr.cost = cost;
		pr.log = log;
		pr.logging = logging;
		pr.w = w;
		pr.readyToFinish = readyToFinish;
		
		pr.propagate = propagate;
		pr.exploit = exploit;
		
		pr.outputGuard = outputGuard.Copy();
		pr.inputGuard = inputGuard.Copy();
		
		return pr;
	}

	protected void blockingProcessTuple(Tuple tuple, int streamId)
			throws ShutdownException {

		// Start the worker thread on first input
		if (!started) {
			w = new WorkerThread(this.getName() + "Expensive thread");
			w.start();
			started = true;
		}

		// Add to the ToDo list.
		synchronized (toDo) {
			toDo.add(tuple);
		}
	}

	protected void processPunctuation(Punctuation tuple, int streamId)
			throws ShutdownException, InterruptedException {

		// Start the worker thread on first input
		if (!started) {
			w = new WorkerThread(this.getName() + "Expensive thread");
			w.start();
			started = true;
		}

		// Add to the ToDo list
		synchronized (toDo) {
			toDo.add(tuple);
		}
	}

	/***
	 * specialized to handle shutdown correctly.
	 * 
	 */

	protected void processCtrlMsgFromSource(ControlFlag ctrlFlag, int streamId)
			throws java.lang.InterruptedException, ShutdownException {
		// upstream control messages are SYNCH_PARTIAL
		// END_PARTIAL and EOS. We should not get GET_PARTIAL,
		// NULLFLAG or SHUTDOWN here (SHUTDOWN handled with exceptions)

		switch (ctrlFlag) {
		case SYNCH_PARTIAL:
			// This stream should no longer be active and should be
			// in sychronizing partial state
			activeSourceStreams.remove(streamId);
			sourceStreams[streamId].setStatus(SourceTupleStream.SynchPartial);
			// Need to handle the creation of partial results
			updatePartialResultCreation();
			return;

		case END_PARTIAL:
			// End of partial result, so stop reading from this stream
			// and set status appropriately
			activeSourceStreams.remove(streamId);
			sourceStreams[streamId].setStatus(SourceTupleStream.EndPartial);
			// Need to handle the creation of partial results
			updatePartialResultCreation();
			return;
		case EOS:
			// flush results first
			flushCurrentResults(false);

			// by now, the worker thread should have stopped, so it is safe to
			// close the streams.
			sourceStreams[streamId].setStatus(SourceTupleStream.Closed);
			activeSourceStreams.remove(streamId);

			streamClosed(streamId);
			return;

			/*
			 * // This is the end of stream, so mark the stream as closed // and
			 * remove it from the list of streams to read from
			 * sourceStreams[streamId].setStatus(SourceTupleStream.Closed);
			 * activeSourceStreams.remove(streamId);
			 * 
			 * // Let the operator know that one of its source stream is closed;
			 * streamClosed(streamId);
			 * 
			 * // If this causes the operator to become non-blocking, then //
			 * put out the current output and clear the current output if
			 * (blockingSourceStreams[streamId] && !isBlocking()) { // Output
			 * the current results (which are not partial anymore) // and
			 * indicate to operator that it should clear the // current results
			 * since we are transition to nonblocking
			 * flushCurrentResults(false);
			 * 
			 * // Update partial result creation if any. This is necessary //
			 * because partial result creation may terminate when all // input
			 * streams are either synchronized or closed.
			 * updatePartialResultCreation(); } return;
			 */
		
		default:
			assert false : "KT unexpected control message from source "
					+ ctrlFlag.flagName();
		}
	}

	@SuppressWarnings("deprecation")
	protected synchronized void flushCurrentResults(boolean partial)
			throws ShutdownException, InterruptedException {
		readyToFinish = true;
		boolean Quit = false;

		while (!Quit) {
			synchronized (toDo) {
				if (toDo.isEmpty())
					Quit = true;
			}
			w.resume();
		}
		w.stop();
	}

	void processCtrlMsgFromSink(ArrayList ctrl, int streamId)
			throws java.lang.InterruptedException, ShutdownException {
		// downstream control message is GET_PARTIAL
		// We should not get SYNCH_PARTIAL, END_PARTIAL, EOS or NULLFLAG
		// REQ_BUF_FLUSH is handled inside SinkTupleStream
		// here (SHUTDOWN is handled with exceptions)

		if (ctrl == null)
			return;

		ControlFlag ctrlFlag = (ControlFlag) ctrl.get(0);

		switch (ctrlFlag) {
		case GET_PARTIAL:
			processGetPartialFromSink(streamId);
			break;
		case MESSAGE:
			FeedbackPunctuation fp = (FeedbackPunctuation) ctrl.get(2);

			if (logging) {
				log.Update(fp.toString(), String.valueOf(outCount));
			}

			FeedbackPunctuation fpSend = new FeedbackPunctuation(fp.Type(), fp
					.Variables(), fp.Comparators(), fp.Values());
			
			// get attribute positions from tuple to check against guards
			names = new String[fpSend.Variables().size()];
			names = fpSend.Variables().toArray(names);

			// get positions
			positions = new int[fpSend.Variables().size()];			
			for (int i = 0; i < names.length; i++) {
				positions[i] = outputTupleSchema.getPosition(names[i]);
			}

			if (exploit) {
				synchronized (inputGuard) {
					//outputGuard.add(fp);
					inputGuard.add(fp);
				}
					// cleanse toDo here
						cleanse(fp);
					
					
				}
			

			if (propagate) {
				sendFeedbackPunctuation(fpSend, streamId);
			}
			break;
		default:
			assert false : "KT unexpected control message from sink "
					+ ctrlFlag.flagName();
		}
}
	
void cleanse(FeedbackPunctuation fp)
{
	ArrayList<Tuple> newToDo = new ArrayList<Tuple>();
	
	synchronized(toDo)
	{
		while(!toDo.isEmpty())
		{
			Tuple t = toDo.remove(0);
			if(t.isPunctuation()) {
				newToDo.add(t);
			} else {
			if(!(fp.match(positions, t.getTuple())))
				newToDo.add(t);
			}
		}
		toDo = newToDo;
		//newToDo.clear();
	}
}
	
	
	
	class WorkerThread extends Thread {
		int out = 0;

		public WorkerThread(String name) {
			super(name);
		}

		public synchronized void run() {
			while (!(readyToFinish && toDo.isEmpty())) {
				synchronized (toDo) {
					if (!toDo.isEmpty()) {
						try {

							Tuple t = toDo.remove(0);

							if (t.isPunctuation()) {
								putTuple(t, 0);
								if (logging) {
									PunctsOut++;
									log.Update("PunctsOut", String
											.valueOf(PunctsOut));
								}

							} else {
								if (exploit && outputGuard != null) {
									// check against guards
									Boolean guardMatch = false;
									
									synchronized(outputGuard)
									{
										for (FeedbackPunctuation fp : outputGuard.elements()) {
											guardMatch = guardMatch
													|| fp
															.match(positions, t
																	.getTuple());
										}
									}
									if (!guardMatch) {
										for (int i = 0; i < cost; i++) {
											// something expensive
										}
										putTuple(t, 0);
										if (logging) {
											outCount++;
											log.Update("TupleOut", String.valueOf(outCount));
										}
										
									}
								} else {
									for (int i = 0; i < cost; i++) {
										// something expensive
									}
									putTuple(t, 0);
									if (logging) {
										outCount++;
										log.Update("TupleOut", String.valueOf(outCount));
										//System.out.println(this.getName() + tupleOut);
									}

								}								
							}

						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (ShutdownException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
			}// end while
		}
	}
}
