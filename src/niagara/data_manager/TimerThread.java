/**
 * $Id: TimerThread.java,v 1.1 2003/02/25 06:14:21 vpapad Exp $
 *
 */

package niagara.data_manager;

/** Niagara DataManager
  * ConstantOpThread - retrieve data from an embedded document and
  * put it into the stream; based on FirehoseThread
  */

import org.w3c.dom.*;

import niagara.query_engine.*;
import niagara.utils.*;
import niagara.logical.Variable;
import niagara.logical.Timer;
import java.util.TimerTask;
import niagara.optimizer.colombia.*;

public class TimerThread extends SourceThread {
    /** In relative reporting, we start reporting time from 0, 
     * otherwise we report time as milliseconds after midnight,
     * January 1, 1970. */
    protected boolean relative;

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
    }

    public TimerThread(
        boolean relative,
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
    }

    public void plugIn(SinkTupleStream outputStream, DataManager dm) {
        this.outputStream = outputStream;
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
    public void initFrom(LogicalOp op) {
        Timer t = (Timer) op;
        this.relative = t.isRelative();
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
    public Op copy() {
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
            ^ (relative ? 0 : 1);
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
            if (tt.relative)
                offset = System.currentTimeMillis();
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
            System.err.println("XXX vpapad: producing value");
            Node node = doc.createTextNode(String.valueOf(currentTime));
            do {
                try {
                    tt.outputStream.put(node);
                    System.err.println("XXX vpapad: tuple sent");
                    return;
                } catch (InterruptedException ie) {
                    // Do nothing
                } catch (ShutdownException se) {
                        System.err.println("XXX vpapad: cancelling");
                        cancel();
                        return;
                }
            } while (true);
        }
    }
}
