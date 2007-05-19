var MIN_WIDTH = 500;
var MIN_HEIGHT = 500;
var MARGIN_WIDTH = 50;
var MARGIN_HEIGHT = 50;

var MILLISECONDS_IN_DAY = 24*60*60*1000;

var canvas_width = MIN_WIDTH;
var canvas_height = MIN_HEIGHT;

var canvasContainer;
var canvas;
var stage;

function init_canvas() {
	canvasContainer = findElement("canvasContainer");
	canvas = new TextCanvas(canvasContainer);	
}

function resize_canvas() {
	canvas_width = max(window.innerWidth - MARGIN_WIDTH, MIN_WIDTH); 
	canvas_height = max(window.innerHeight - MARGIN_HEIGHT, MIN_HEIGHT); 
	canvas.setDimensions(canvas_width, canvas_height);
	redraw_canvas();
}

function redraw_canvas() {
	var ctx = canvas.getContext('2d');
	ctx.clear();
	ctx.drawString(canvas_width/2, canvas_height/2, "FOO!");
}

function DateTime(timestamp) {
	this.timestamp = timestamp;
	var dateTS = new Date(timestamp);
	this.midnight = this.timestamp - dateTS.getMilliseconds();
	this.date_string =  (dateTS.getMonth()+1).toString() 
				        + "/" + (dateTS.getDate()).toString();
	this.same_day = function(other_day) {
		return (this.midnight == other_day.midnight);
	}
	this.next_day = function() {
		return new DateTime(this.timestamp + MILLISECONDS_IN_DAY);
	}
}

function Stage(stageXML) {
	this.startdate = stageXML.@startdate;
	this.enddate = stageXML.@enddate;
	this.starttime = stageXML.@starttime,
	this.endtime = stageXML.@endtime;
	this.days = new Array();
	this.days_hashtable = new Object();
	this.visible_days_hashtable = new Object();
	this.actors = new Array();
	// Find all days
	var ts_today = this.startdate;
	do {
		var today = new DateTime(ts_today);
		this.days.push(today);
		this.days_hashtable[today.date_string] = today;
		ts_today = today.next_day().midnight;
	} while (ts_today <= this.enddate);
	
	// Load actors
	for (var actorXML in stageXML..actor) {
		var actor = new Actor(actorXML, this);
		this.actors.push(actor);
		this.visible_days_hashtable[actor.day.date_string] = true;
	}
	
	this.find_day = function(day) {
		return days_hashtable[day.toString()];
	}
	
	this.draw_visible_days = function(canvas) {
		console.log("Visible days");
		var first_day = this.days[0];
		var last_day = this.days[this.days.length - 1];
		for (var day in this.days) {
			if (day == this.days[0] 
			    || day == this.days[this.days.length-1]
			    || visible_days_hashtable[day.date_string] != null) {
			    console.log(day.date_string);
			}
		}
	}
}

function Actor(actorXML, stage) {
	var start = new DateTime(actorXML.start);
	var end = new DateTime(actorXML.end);
	this.day = stage.find_day(start.date_string);
	this.status = actorXML.@status;
}

function load_stage(stageXML) {
	stage = new Stage(stageXML);
}