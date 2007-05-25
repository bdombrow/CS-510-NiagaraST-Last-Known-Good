function on_resize() {
	resize_canvas();
}

function on_load() {
	init_canvas();
	resize_canvas();
}

function restart() {
	reset_canvas();
	submit_query();
}

function set_status(str) {
	var statusDiv = document.getElementById("status");
	statusDiv.removeChild(statusDiv.firstChild);
	statusDiv.appendChild(document.createTextNode("Status: " + str));
}

function set_lag(str) {
	var statusDiv = document.getElementById("lag");
	statusDiv.removeChild(statusDiv.firstChild);
	if (str != "0") {
		statusDiv.setAttribute("class", "biglag");
	} else {
		statusDiv.setAttribute("class", "status");
	}
	statusDiv.appendChild(document.createTextNode("Lag: " + str + " ms"));
}

function set_buffered(str) {
	var statusDiv = document.getElementById("buffered");
	statusDiv.removeChild(statusDiv.firstChild);
	statusDiv.appendChild(document.createTextNode("Archive buffering: " + parseInt(str)/1000 + " seconds of data"));
}

