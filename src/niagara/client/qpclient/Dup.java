package niagara.client.qpclient;

import diva.graph.*;
import diva.graph.model.*;
import diva.util.*;

import java.util.*;

public class Dup extends Operator {
    public Dup() {
	super();
	type = "dup";
    }
    public void defaultSetLines() {
	lines = new LineDescription[] {new LineDescription("replicate", CENTER, null)};
    }
}
