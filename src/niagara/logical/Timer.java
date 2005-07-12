/* $Id: Timer.java,v 1.5 2005/07/12 02:21:12 vpapad Exp $ */
package niagara.logical;

import org.w3c.dom.Element;

import java.util.Locale;
import java.util.StringTokenizer;
import java.text.DateFormat;
import java.text.ParseException;

import niagara.connection_server.Catalog;
import niagara.connection_server.InvalidPlanException;
import niagara.optimizer.colombia.Attrs;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;

/** A <code>Timer</code> produces a stream of tuples with the current time */
public class Timer extends NullaryOperator {
    /** The epoch we're using to report time. Empty string means
     * use the default Java timestamp (milliseconds after midnight,
     * January 1, 1970), "now" means start from when the query plan
     * started executing, anything else is a String representation
     * of a date and time. */
    private String relative;

    /** Clock starts ticking after <code>delay</code> */
    private int delay;

    /** Approximate time between clock ticks, in milliseconds */
    private int period;

    /** Time reporting is delayed by <code>slack</code> milliseconds */
    private int slack;

    /** Reported time is rounded to the closest multiple of 
     * <code>granularity</code> milliseconds */
    private int granularity;

    /** Simulated time runs <code>warp</code> times faster than real time*/
    private int warp;

    /** The timer's name */
    private String name;

    private final static int DEFAULT_DELAY = 0;
    private final static int DEFAULT_SLACK = 0;
    private final static int DEFAULT_GRANULARITY = 1;
    private final static int DEFAULT_WARP = 1;

    private final static int SEC_AS_MILLISECS = 1000;
    private final static int MIN_AS_MILLISECS = 60 * SEC_AS_MILLISECS;
    private final static int HOUR_AS_MILLISECS = 60 * MIN_AS_MILLISECS;
    private final static int DAY_AS_MILLISECS = 24 * HOUR_AS_MILLISECS;

    public void loadFromXML(Element e, LogicalProperty inputs[], Catalog catalog)
        throws InvalidPlanException {
        name = e.getAttribute("id");

        String attrStr = e.getAttribute("relative");

        if (attrStr.length() == 0 || attrStr.equalsIgnoreCase("now"))
            relative = attrStr;
        else {
            // Check that the string provided is a valid date string
            DateFormat df =
                DateFormat.getDateTimeInstance(
                    DateFormat.MEDIUM,
                    DateFormat.MEDIUM,
                    Locale.US);
            try {
                df.parse(attrStr);
            } catch (ParseException pe) {
                throw new InvalidPlanException(
                    "Could not parse '"
                        + attrStr
                        + "' as a valid date description");

            }
            relative = attrStr;
        }
        attrStr = e.getAttribute("period");
        if (attrStr.length() == 0)
            throw new InvalidPlanException("Period is a required attribute for timer");
        period = parseTimeInterval(attrStr);

        // XXX vpapad: is this the right way to handle defaults, or should
        // we just provide defaults in queryplan.dtd ???
        attrStr = e.getAttribute("slack");
        if (attrStr.length() == 0)
            slack = DEFAULT_SLACK;
        else
            slack = parseTimeInterval(attrStr);

        attrStr = e.getAttribute("granularity");
        if (attrStr.length() == 0)
            granularity = DEFAULT_GRANULARITY;
        else
            granularity = parseTimeInterval(attrStr);

        attrStr = e.getAttribute("delay");
        if (attrStr.length() == 0)
            delay = DEFAULT_DELAY;
        else
            delay = parseTimeInterval(attrStr);

        attrStr = e.getAttribute("warp");
        if (attrStr.length() == 0)
            warp = DEFAULT_WARP;
        else {
            try {
                warp = Integer.parseInt(attrStr);
            } catch (NumberFormatException nfe) {
                throw new InvalidPlanException(
                    "Expected integer, found "
                        + attrStr
                        + " while parsing "
                        + name);
            }
        }
    }

    public void dumpAttributesInXML(StringBuffer sb) {
        sb.append(" relative='").append(relative);
        sb.append("' period='");
        formatTimeInterval(period, sb);
        sb.append("' delay='");
        formatTimeInterval(delay, sb);
        sb.append("' slack='");
        formatTimeInterval(slack, sb);
        sb.append("' granularity='");
        formatTimeInterval(granularity, sb);
        sb.append("' warp='");
        sb.append(warp).append("'");
    }

    /** @return the number of milliseconds in the specified time interval
     * (if no unit is provided, assume milliseconds) */
    public static int parseTimeInterval(String intervalStr)
        throws InvalidPlanException {
        StringTokenizer strtok = new StringTokenizer(intervalStr);
        boolean expectNumber = true;
        int currentNumber = 0;
        int total = 0;
        while (strtok.hasMoreTokens()) {
            String tok = strtok.nextToken();
            if (expectNumber) {
                try {
                    currentNumber = Integer.parseInt(tok);
                } catch (NumberFormatException nfe) {
                    throw new InvalidPlanException(
                        "Expected integer, found "
                            + tok
                            + "while parsing "
                            + intervalStr);
                }
            } else {
                tok = tok.toLowerCase();
                if (tok.indexOf("day") >= 0)
                    total += currentNumber * DAY_AS_MILLISECS;
                else if (tok.indexOf("hour") >= 0)
                    total += currentNumber * HOUR_AS_MILLISECS;
                else if (tok.indexOf("minute") >= 0)
                    total += currentNumber * MIN_AS_MILLISECS;
                else if (tok.indexOf("second") >= 0)
                    total += currentNumber * SEC_AS_MILLISECS;
                else if (tok.indexOf("millisecond") >= 0)
                    total += currentNumber;
                else
                    throw new InvalidPlanException(
                        "Expected time term, found "
                            + tok
                            + " while parsing "
                            + intervalStr);
            }
            expectNumber = !expectNumber;
        }
        if (!expectNumber)
            total += currentNumber;
        return total;
    }

    /** A human-readable representation of a time interval */
    public void formatTimeInterval(int millis, StringBuffer sb) {
        format(millis, DAY_AS_MILLISECS, "day", sb);
        millis = millis % DAY_AS_MILLISECS;
        format(millis, HOUR_AS_MILLISECS, "hour", sb);
        millis = millis % HOUR_AS_MILLISECS;
        format(millis, MIN_AS_MILLISECS, "minute", sb);
        millis = millis % MIN_AS_MILLISECS;
        format(millis, SEC_AS_MILLISECS, "second", sb);
        millis = millis % SEC_AS_MILLISECS;
        sb.append(millis).append(" millisecond");
        if (millis != 1)
            sb.append("s");
    }

    private void format(
        int value,
        int inMilliSecs,
        String unit,
        StringBuffer sb) {
        value = value / inMilliSecs;
        if (value == 0)
            return;
        sb.append(value).append(" ");
        sb.append(unit);
        if (value > 1)
            sb.append("s ");
        else
            sb.append(" ");
    }

    public boolean isSourceOp() {
        return true;
    }

    /**
     * @see niagara.optimizer.colombia.LogicalOp#findLogProp(ICatalog, ArrayList)
     */
    public LogicalProperty findLogProp(
        ICatalog catalog,
        LogicalProperty[] input) {
        return new LogicalProperty(1, new Attrs(new Variable(name)), true);
    }

    /**
     * @see niagara.optimizer.colombia.Op#copy()
     */
    public Op opCopy() {
        Timer top = new Timer();
        top.relative = relative;
        top.delay = delay;
        top.period = period;
        top.slack = slack;
        top.granularity = granularity;
        top.warp = warp;
        top.name = name;
        return top;
    }

    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof Timer))
            return false;
        if (obj.getClass() != Timer.class)
            return obj.equals(this);
        Timer other = (Timer) obj;
        return name.equals(other.name)
            && relative.equals(other.relative)
            && period == other.period
            && slack == other.slack
            && granularity == other.granularity
            && warp == other.warp
            && delay == other.delay;
    }

    /**
     * @see java.lang.Object#hashCode()
     */
    public int hashCode() {
        return name.hashCode()
            ^ period
            ^ slack
            ^ granularity
            ^ warp
            ^ delay
            ^ relative.hashCode();
    }

    public int getDelay() {
        return delay;
    }

    public int getGranularity() {
        return granularity;
    }

    public int getPeriod() {
        return period;
    }

    public String getRelative() {
        return relative;
    }

    public int getSlack() {
        return slack;
    }

    public int getWarp() {
        return warp;
    }
}
