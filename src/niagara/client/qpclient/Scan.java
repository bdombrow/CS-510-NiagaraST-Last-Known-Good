package niagara.client.qpclient;

import java.awt.Font;
import java.awt.Graphics2D;

public class Scan extends Operator {

    public Scan() {
	super();
	type = "scan";
    }

    public void paint(QPFigure qpf, Graphics2D g) {
	Font current_font = g.getFont();
	LineDescription toDisplay[] = {
	    new LineDescription(type, CENTER, current_font),
	    new LineDescription("$" + (String) getProperty("id"), CENTER, current_font)
	};
	setLines(toDisplay);
	super.paint(qpf, g);
    }
}
