/* $Id: Schedulable.java,v 1.1 2003/09/22 00:15:42 vpapad Exp $ */
package niagara.query_engine;

/** Something that can be scheduled to run in a Niagara thread */
public interface Schedulable extends Runnable {
    public String getName();
}
