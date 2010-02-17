package niagara.ndom.saxdom;

/**
 * 
 * A read-only implementation of the DOM Level 2 interface, using an array of
 * SAX events as the underlying data store.
 * 
 * Constants for SAX events
 */
public class SAXEvent {
	public static final byte EMPTY = 0;

	public static final byte START_DOCUMENT = 1;
	public static final byte END_DOCUMENT = 2;
	public static final byte START_ELEMENT = 3;
	public static final byte END_ELEMENT = 4;
	public static final byte ATTR_NAME = 5;
	public static final byte ATTR_VALUE = 6;
	public static final byte TEXT = 7;
	public static final byte NAMESPACE_URI = 8;
}
