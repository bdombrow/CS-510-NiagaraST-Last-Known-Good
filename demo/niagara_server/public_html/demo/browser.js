function on_resize() {
	resize_canvas();
}

function on_load() {
	init_canvas();
	resize_canvas();
	submit_query();
}

function set_status(str) {
	var statusDiv = document.getElementById("status");
	statusDiv.removeChild(statusDiv.firstChild);
	statusDiv.appendChild(document.createTextNode("Status: " + str));
}

