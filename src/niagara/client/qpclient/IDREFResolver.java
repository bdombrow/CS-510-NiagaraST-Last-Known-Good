package niagara.client.qpclient;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.ListIterator;

/**
 * <code>IDREFResolver</code> matches objects who want to become identified with
 * a particular IDREF, with client objects who want to be notified when a
 * particular IDREF is resolved.
 * 
 * @author Vassilis Papadimos
 * @version 1.0
 */

@SuppressWarnings("unchecked")
public class IDREFResolver {
	private HashMap idrefs_to_objects;
	private HashMap idrefs_to_clients;

	public IDREFResolver() {
		idrefs_to_objects = new HashMap();
		idrefs_to_clients = new HashMap();
	}

	/**
	 * Associate an IDREF with an object
	 * 
	 * @param idref
	 *            a <code>String</code> value
	 * @param o
	 *            an <code>Object</code> value
	 */
	public void addIDREF(String idref, Object o) {
		if (idrefs_to_clients.containsKey(idref)) {
			ListIterator iter = ((ArrayList) idrefs_to_clients.get(idref))
					.listIterator();
			while (iter.hasNext()) {
				IDREFResolverClient idc = (IDREFResolverClient) iter.next();
				idc.IDREFResolved(idref, o);
			}
			idrefs_to_clients.remove(idref);
		}
		idrefs_to_objects.put(idref, o);
	}

	/**
	 * Register interest on a particular IDREF.
	 * 
	 * @param idref
	 *            a <code>String</code>
	 * @param idc
	 *            the <code>IDREFResolverClient</code> requesting registration
	 */
	public void addClient(String idref, IDREFResolverClient idc) {
		// If the idref is already resolved, notify client
		if (idrefs_to_objects.containsKey(idref))
			idc.IDREFResolved(idref, idrefs_to_objects.get(idref));
		else {
			if (idrefs_to_clients.containsKey(idref)) {
				((ArrayList) idrefs_to_clients.get(idref)).add(idc);
			} else {
				ArrayList al = new ArrayList();
				al.add(idc);
				idrefs_to_clients.put(idref, al);
			}
		}
	}

	public boolean everythingResolved() {
		return idrefs_to_clients.size() == 0;
	}

	public Iterator getUnresolved() {
		return idrefs_to_clients.keySet().iterator();
	}
}
