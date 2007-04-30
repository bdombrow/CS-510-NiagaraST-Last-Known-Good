/**
 * $Id: TimerThread.java,v 1.7 2007/04/30 19:19:06 vpapad Exp $
 *
 */

package niagara.data_manager;

import org.w3c.dom.*;

import niagara.query_engine.*;
import niagara.utils.*;
import niagara.logical.Timer;
import niagara.logical.Variable;

import java.util.Date;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.Locale;
import java.util.TimerTask;

import niagara.optimizer.colombia.*;

public class TimerThread extends SourceThread {
    protected String relative;

    /** Approximate time between clock ticks, in milliseconds */
    protected int period;

    /** Time reporting is delayed by <code>slack</code> milliseconds */
    protected int slack;

    /** Clock starts ticking after <code>delay</code> */
    private int delay;

    /** Reported time is rounded to the closest multiple of 
     * <code>granularity</code> milliseconds */
    protected int granularity;

    /** Simulated time runs <code>warp</code> times faster than real time*/
    protected int warp;

    /** The timer's name */
    private String name;

    protected SinkTupleStream outputStream;

    public TimerThread() {
	isSendImmediate = true;
    }

    public TimerThread(
        String relative,
        int period,
        int slack,
        int granularity,
        int delay,
        int warp,
        String name) {
        this.relative = relative;
        this.period = period;
        this.slack = slack;
        this.granularity = granularity;
        this.warp = warp;
        this.delay = delay;
        this.name = name;
	isSendImmediate = true;
    }

    public void plugIn(SinkTupleStream outputStream, DataManager dm) {
        this.outputStream = outputStream;
	outputStream.setSendImmediate();
    }

    /**
     * Thread run method
     *
     */
    public void run() {
        TimerThreadTask ttt = new TimerThreadTask(this);
        java.util.Timer t = new java.util.Timer();
        // XXX vpapad TODO: if period or delay is not a multiple
        // of warp, this will be imprecise
        t.scheduleAtFixedRate(ttt, delay / warp, period / warp);
    }

    /**
     * @see niagara.optimizer.colombia.PhysicalOp#initFrom(LogicalOp)
     */
    public void opInitFrom(LogicalOp op) {
        Timer t = (Timer) op;
        this.relative = t.getRelative();
        this.period = t.getPeriod();
        this.slack = t.getSlack();
        this.granularity = t.getGranularity();
        this.warp = t.getWarp();
        this.delay = t.getDelay();
        this.name = t.getName();
    }

    /**
     * @see niagara.optimizer.colombia.Op#copy()
     */
    public Op opCopy() {
        return new TimerThread(
            relative,
            period,
            slack,
            granularity,
            delay,
            warp,
            name);
    }

    public boolean equals(Object o) {
        if (o == null || !(o instanceof TimerThread))
            return false;
        if (o.getClass() != getClass())
            return o.equals(this);
        TimerThread other = (TimerThread) o;

        return name.equals(other.name)
            && relative == other.relative
            && period == other.period
            && slack == other.slack
            && granularity == other.granularity
            && delay == other.delay
            && warp == other.warp;
    }

    /**
     * @see java.lang.Object#hashCode()
     */
    public int hashCode() {
        return name.hashCode()
            ^ period
            ^ slack
            ^ granularity
            ^ delay
            ^ warp
            ^ relative.hashCode();
    }

    /**
     * @see niagara.query_engine.SchemaProducer#getTupleSchema()
     */
    public TupleSchema getTupleSchema() {
        TupleSchema ts = new TupleSchema();
        ts.addMapping(new Variable(name));
        return ts;
    }

    /**
     * @see niagara.utils.SerializableToXML#dumpAttributesInXML(StringBuffer)
     */
    public void dumpAttributesInXML(StringBuffer sb) {
        // XXX vpapad TODO: implement this
    }

    /**
     * @see niagara.utils.SerializableToXML#dumpChildrenInXML(StringBuffer)
     */
    public void dumpChildrenInXML(StringBuffer sb) {
        sb.append("/>");
    }

    /** 
     * @see niagara.optimizer.colombia.PhysicalOp#findLocalCost(niagara.optimizer.colombia.ICatalog, niagara.optimizer.colombia.LogicalProperty[])
     */
    public Cost findLocalCost(
        ICatalog catalog,
        LogicalProperty[] InputLogProp) {
        // XXX vpapad TODO: Totally bogus!
        // What's the cost of something that runs forever?
        // We should do something with rates here.
        return new Cost(10 * catalog.getDouble("tuple_construction_cost"));
    }

    /** 
     * @see niagara.query_engine.SchemaProducer#constructTupleSchema(niagara.query_engine.TupleSchema[])
     */
    public void constructTupleSchema(TupleSchema[] inputSchemas) {
        // Do nothing
    }

    class TimerThreadTask extends TimerTask {
        TimerThread tt;
        Document doc;
        long offset;
        public TimerThreadTask(TimerThread tt) {
            this.tt = tt;
            doc = niagara.ndom.DOMFactory.newDocument();
            if (tt.relative.equalsIgnoreCase("now"))
                offset = System.currentTimeMillis();
            else if (tt.relative.length() == 0)
                offset = 0;
            else {
                    // Check that the string provided is a valid date string
                    DateFormat df =
                        DateFormat.getDateTimeInstance(
                            DateFormat.MEDIUM,
                            DateFormat.MEDIUM,
                            Locale.US);
                    Date d;                            
                    try {
                        d = df.parse(tt.relative);
                    } catch (ParseException pe) {
                            throw new PEException("Invalid date string passed to TimerThread!");
                            
                    }
                    offset = d.getTime();                    
            }
        }
        public void run() {
            // XXX vpapad TODO: we should handle
            // cases where scheduledExecutionTime() is far
            // from the current time, and also cases where
            // the output stream doesn't move fast enough

            // XXX vpapad TODO: it's really stupid that we have
            // to create text nodes here, we really need a LongDomain
            long currentTime =
                (System.currentTimeMillis() - offset) * tt.warp - tt.slack;
            currentTime = currentTime - (currentTime % granularity);
	    LongAttr la = new LongAttr(new Long(currentTime));
	    Tuple t = new Tuple(false, 1);
	    t.appendAttribute(la);
            do {
                try {
                    tt.outputStream.putTuple(t);
                    return;
                } catch (InterruptedException ie) {
                    // Do nothing
                } catch (ShutdownException se) {
                    cancel();
                    return;
                }
            } while (true);
        }
    }
}
