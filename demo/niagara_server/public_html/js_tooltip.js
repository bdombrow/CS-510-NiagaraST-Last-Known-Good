/* First thing is to declare global variables 
	 to contain the X and Y coordinates of the
	 mouse cursor.
*/
var currentX = 0;
var currentY = 0;

/* Use "object sniffing" technique to determine
   if the browser has the captureEvent method.
   This tells us if the client is a Mozilla-type browser.
	 If so, we tell the document object to 
	 capture the MOUSEMOVE event. */
if (document.captureEvent){
	document.captureEvent(Event.MOUSEMOVE);
}

/* The getMousePosition() function will be the
	 event-handling function that sets 
	 currentX and currentY. */
function getMousePosition(evt){
	/* Internet Explorer and Mozilla have differences
		 both in the way they implement events and the
		 way they determine x/y coordinates. */
	if (window.event){
		/* This is the Internet Explorer way.
			 Adds the x and y coordinates relative to the screen
			 to the number of pixels the user has scrolled 
			 horizontally and vertically. */
		currentX = window.event.clientX + document.body.scrollLeft;
		currentY = window.event.clientY + document.body.scrollTop;
		
	}
	else if (evt){
		/* In Mozilla/Netscape browsers, you just need to access
		   the pageX and pageY properties to determine coordinates. */
		currentX = evt.pageX;
		currentY = evt.pageY;
	}
}
// register our event handler for the onmousemove event.
document.onmousemove = getMousePosition;


/* The positionElement function will place an object
   at the coordinates (currentX+10, currentY+10).
	 Parameter: any valid ID attribute of an element. */
function positionElement(id){
	// set elem to the element with specified id.
	elem = document.getElementById(id);
	if (elem){
		/* If the element exists, set the CSS style "left"
			 to currentX + 10, and the CSS style "top" to
			 currentY + 10. */
		elem.style.left = (currentX + 10) + "px";
		elem.style.top = (currentY + 10) + "px";
	}
}

/* hideToolTip will set the CSS "visibility" property to
	 "hidden" for an element whose ID attribute is passed
	 to the function. */
function hideToolTip(id){
	elem = document.getElementById(id);
  if (elem){
		elem.style.visibility="hidden";
	}
}

/* showToolTip will set the CSS "visibility" property to
	 "visible" for an element whose ID attribute is passed
	 to the function. */
function showToolTip(id){
	elem = document.getElementById(id);
	if (elem){
		elem.style.visibility="visible";
	}
}