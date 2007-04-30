function SampleListener() {
  this.measurements = {};
  
  this.setContainer = function(container) {
    this.container = container;
    //this.gradients = [this.makeGradient('orange'), 
    //                  this.makeGradient('pink'),
    //                  this.makeGradient('blue')];
    // maps graph ids to gradients                      
    //this.gradientMap = new Object();           
    // graph windows
    //this.graphXPoints = new Object();           
    //this.graphYPoints = new Object();           
  }
  
  //this.createsvg = function(elt) {
	//  return this.container.ownerDocument.createElementNS("http://www.w3.org/2000/svg", elt);
  //}
  
  this.makeGradient = function(color) {
      var lg = this.createsvg('linearGradient');
      lg.setAttribute("x1", "100%");
      lg.setAttribute("x2", "100%");
      lg.setAttribute("y1", "100%");
      lg.setAttribute("y2", "0%");

      lg.setAttribute("id", "grad" + color);
      var st1 = this.createsvg('stop');
      lg.appendChild(st1);
      st1.setAttribute('offset', '0%');
      st1.setAttribute('stop-color', 'white');
      var st2 = this.createsvg('stop');
      lg.appendChild(st2);
      st2.setAttribute('offset', '100%');
      st2.setAttribute('stop-color', color);
      return lg;
  }

  //this.randomGradient = function() {
  // var grad = this.gradients[Math.floor(Math.random() * this.gradients.length)];
  // return grad.getAttribute("id");
  //}
                      
  this.addSample = function(sample) {
    sample = sample.getElementsByTagName("measurement").item(0);
    var operator = sample.getAttribute("operator");
    var name = sample.getAttribute("name");

    container = this.container;
    
    var document = container.ownerDocument;

    var opdiv = document.evaluate('//div[@id = ' + "'" + operator + "']", 
                                document, null, 0, null).iterateNext();
	if (opdiv == null) {
	  var newDiv = document.createElement('div');
	  newDiv.setAttribute('id', operator);
	  newDiv.setAttribute('class', 'operator');
	  newDiv.appendChild(document.createTextNode(operator  + ":"));
	  container.appendChild(newDiv);
	  opdiv = newDiv;
	}
	
	var mdiv = document.evaluate('./div[@id = ' + "'" + name + "']",
                                opdiv, null, 0, null).iterateNext();

    if (mdiv == null) {
	  var newDiv = document.createElement('div');
	  newDiv.setAttribute('id', name);
	  newDiv.setAttribute('class', 'measurement');
	  newDiv.appendChild(document.createTextNode(name  + ":"));
	  newDiv.appendChild(document.createTextNode("-"));
	  mdiv = newDiv;
	  opdiv.appendChild(mdiv);
    }
	
	mdiv.replaceChild(document.createTextNode(" " + sample.firstChild.data),
	                  mdiv.childNodes.item(1));
  }
}


var sampleListener = new SampleListener();

function init() {
  sampleListener.setContainer(document.getElementById('results'));
  window.opener.setSampleListener(sampleListener);
}

function done() {
  window.opener.setSampleListener(null);
}
