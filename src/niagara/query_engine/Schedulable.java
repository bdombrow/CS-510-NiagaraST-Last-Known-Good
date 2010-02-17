package niagara.query_engine;

/** Something that can be scheduled to run in a Niagara thread */
public interface Schedulable extends Runnable {
	public String getName();
}
