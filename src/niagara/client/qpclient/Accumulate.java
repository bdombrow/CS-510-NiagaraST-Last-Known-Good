package niagara.client.qpclient;

import diva.graph.*;
import diva.graph.model.*;
import diva.util.*;

import java.util.*;
import java.awt.Color;

public class Accumulate extends Operator {
    public Accumulate() {
	super();
	type = "accumulate";
	nodeColor = Color.orange;
    }
}
