var MIN_WIDTH = 500;
var MIN_HEIGHT = 400;
var MARGIN_WIDTH = 150;
var MARGIN_HEIGHT = 150;

var DAY_TOTAL_HEIGHT = 25;
var DAY_MARGIN = 8;
var DAY_HEIGHT = DAY_TOTAL_HEIGHT - DAY_MARGIN;
var SPACER_HEIGHT = 8;
var DAY_PLUS = -3;
var SPACER_PLUS = -3;

var MILLISECONDS_PER_PIXEL = 10000;

var MILLISECONDS_IN_DAY = 24*60*60*1000;

var canvas_width = MIN_WIDTH;
var canvas_height = MIN_HEIGHT;

var canvasContainer;
var canvas;

var canvas_xpos;

var mouseDownX = null;
var mouseUpX = null;
var user_just_scrolled = false;

var buffer_canvas;

var stagelist;

var stagelistXML;

var outline_colors = {
    "buffered" : 'rgb(20,200,20)',
    "purged" : 'rgb(20,200,20)',
    "progress" : 'rgb(255, 200, 50)',
    "progress_late" : 'rgb(255, 200, 50)',
    "future" : 'rgb(192, 192, 192)',
    "future_late" : 'rgb(192, 192, 192)'
}


var fill_colors = {
    "buffered" : 'rgba(20,200, 20, 0.5)',
    "purged" : 'rgba(20, 200, 20, 0)',
    "progress" : 'rgba(255, 200, 50, 0.5)',
    "progress_late" : 'rgba(255, 0, 0, 0.5)',
    "future" : 'rgba(192, 192, 192, 0.4)',
    "future_late" : 'rgba(255, 0, 0, 0.75)'
}

var now_strokestyle = 'rgb(255, 0, 0)';
var now_linewidth = 2;

function recordMouseDown(evt) {
    mouseDownX = evt.clientX;
    mouseUpX = null;
}

function recordMouseUp(evt) {
    mouseUpX = evt.clientX;
    if (mouseDownX != null) {
		var old_pos = canvas_xpos;
		var new_xpos = canvas_xpos - mouseUpX + mouseDownX; 
		new_xpos = max(0, new_xpos);
		new_xpos = min(stagelist.pixel_length - 10, new_xpos);
		canvas_xpos = new_xpos;
		user_just_scrolled = true;
		update_canvas();
		mouseUpX = mouseDownX = null;
    }
}

function init_canvas() {
	canvasContainer = findElement("canvasContainer");
	canvas = new TextCanvas(canvasContainer);	
	buffer_canvas = findElement("bufferCanvas");
	reset_canvas();
	canvasContainer.onmousedown = recordMouseDown;
	canvasContainer.onmouseup = recordMouseUp;
}

function reset_canvas() {
	canvas_xpos = 0;
}

function resize_canvas() {
    canvas_width = max(window.innerWidth - MARGIN_WIDTH, MIN_WIDTH); 
    canvas_height = max(window.innerHeight - MARGIN_HEIGHT, MIN_HEIGHT); 
    canvas.setDimensions(canvas_width, canvas_height);
    redraw_canvas();
}

function redraw_canvas() {
    if (stagelist == undefined) 
	return;
    
    var ctx = buffer_canvas.getContext('2d');
    ctx.clearRect(0, 0, buffer_canvas.width, buffer_canvas.height);
    
    // Resize the buffer canvas
    var xrange = stagelist.end_time.diff(stagelist.start_time);
    var buffer_canvas_width = ceil(xrange / MILLISECONDS_PER_PIXEL);
    
    // Figure out the y positions of things
    var ypos = 0;
    for each (var o in stagelist.day_list) {
	    o.ypos = ypos;
	    ypos += o.height;
	}
    var buffer_canvas_height = ypos;
    
    var day_ypos = new Object();
    for each (var day in stagelist.day_list) {
	    if (day.kind == "day") {
		day_ypos[day.date_string] = day.ypos;
	    }
	}

    buffer_canvas.width = buffer_canvas_width + 20;
    buffer_canvas.height = buffer_canvas_height;

    now_xpos = stagelist.now.diff(stagelist.start_time) / MILLISECONDS_PER_PIXEL;    
    for each (var stage in stagelist.stage_list) {
	    for each (var actor in stage.actor_list) {
		    var x = actor.start_time.diff(stagelist.start_time);
		    var w = actor.end_time.diff(actor.start_time);
		    var xpos = ceil(x / MILLISECONDS_PER_PIXEL);
		    var width = ceil(w / MILLISECONDS_PER_PIXEL);
		    var height = DAY_HEIGHT;
		    for each (var day in stage.day_list) {
			    var ypos = day_ypos[day.date_string];
			    ctx.fillStyle = outline_colors[actor.status];
			    ctx.fillRect(xpos, ypos, width, height);
			    if (width >= 4 && height >= 4) {
				ctx.clearRect(xpos + 1, ypos + 1,
					      width - 2, height - 2);
				ctx.fillStyle = fill_colors[actor.status];
				ctx.fillRect(xpos + 1, ypos + 1,
					      width - 2, height - 2);
			    }
			}
		}
	}
    

    ctx.strokeStyle = now_strokestyle;
    ctx.lineWidth = now_linewidth;
    ctx.beginPath();
    ctx.moveTo(now_xpos, 0);
    ctx.lineTo(now_xpos, buffer_canvas_height - DAY_MARGIN);
    ctx.stroke();

    if (user_just_scrolled) {
		user_just_scrolled = false;
    } else if (now_xpos - canvas_xpos > (4*canvas_width)/5 
    			&& stagelist.ended != "true") {
		canvas_xpos = now_xpos - ceil(canvas_width / 4);
    }

    update_canvas();
}


function update_canvas() {
    var sx = canvas_xpos;
    var sw = min(canvas_width, buffer_canvas.width - canvas_xpos);
    var sh = min(canvas_height, buffer_canvas.height);

    var ctx_actual = canvas.getContext('2d');
    canvas.clear();
    ctx_actual.drawImage(buffer_canvas, sx, 0, sw, sh, 0, 0, sw, sh);

    // Add labels
    if (stagelist.day_list) {
	for each (var o in stagelist.day_list) {
		var ypos = o.ypos;
		if (o.kind == "day") {
		    ypos +=  DAY_PLUS;
		} else {
		    ypos += SPACER_PLUS;
		}
		canvas.addLabel(-70, ypos, o.date_string);
	    }
    }

    var start_time = stagelist.start_time.after(canvas_xpos * MILLISECONDS_PER_PIXEL);
    canvas.addLabel(0, buffer_canvas.height + 20, start_time.time_string);
    var end_time = stagelist.start_time.after((canvas_xpos + canvas_width) * MILLISECONDS_PER_PIXEL);
    canvas.addLabel(canvas_width - 60, buffer_canvas.height + 20, 
		    end_time.time_string);

    var now_xpos = stagelist.now.diff(stagelist.start_time) / MILLISECONDS_PER_PIXEL;    

	var now_label_xpos = now_xpos - canvas_xpos - 40;
	if (now_label_xpos + 100 > canvas_width) {
		now_label_xpos = canvas_width - 100;
	} if ((now_xpos >= canvas_xpos) && (now_xpos - canvas_xpos <= canvas_width)) {
	    canvas.addLabel(now_label_xpos, -30, stagelist.now.second_string);
	}
}

function Stagelist(stagelistXML) {
    this.now = new JustTime(parseInt(stagelistXML.@now) * 1000);
    this.start_time = this.end_time = null;
	this.ended = stagelistXML.@ended;
	
    this.stage_list = new Array();
    for each (var stageXML in stagelistXML..stage) {
		this.stage_list.push(new Stage(stageXML, this.now));
    }

    // Merge, order, and deduplicate visible days from all stages
    var day_list = new Array();
    for each (var stage in this.stage_list) {
		day_list = day_list.concat(stage.day_list);
		if (this.start_time == null) {
			this.start_time = stage.start_time;
			this.end_time = stage.end_time;
		} else {
			this.start_time = this.start_time.min(stage.start_time);
			this.end_time = this.end_time.max(stage.end_time);
		}
    }
    day_list.sort(compare_days);
    var unique_list = new Array();
    for (di in day_list) {
		var ul = unique_list.length;
		if (ul == 0 || day_list[di].gt(unique_list[ul-1])) {
		    unique_list.push(day_list[di]);
		}
    }

    this.day_list = new Array();
    var prev_day = null;
    for each (day in unique_list) {
	    if (prev_day == null || day.is_next_day_of(prev_day)) {
			this.day_list.push(day);
	    } else {
			this.day_list.push(new Spacer(prev_day, day));
			this.day_list.push(day);
	    }
	    prev_day = day;
    }
    
    this.pixel_length = ceil((this.end_time.diff(this.start_time)) / MILLISECONDS_PER_PIXEL);
 }

 function Stage(stageXML, now) {
     var date_str_list = stageXML.@dates;
     this.day_list = new Array();
     for each (var date_str in date_str_list.split(" ")) {
	 if (date_str.length > 0)
	     this.day_list.push(new JustDay(date_str));
     }
     this.actor_list = new Array();
     var start_time = null;
     var end_time = null;
     for each (var actorXML in stageXML..actor) {
     	var actor = new Actor(actorXML, now);
     	if (start_time == null || actor.start_time.before(start_time)) {
     		start_time = actor.start_time;
     	}
     	if (end_time == null || end_time.before(actor.end_time)) {
     		end_time = actor.end_time;
     	}
		this.actor_list.push(actor);
     }
     this.start_time = start_time;
     this.end_time = end_time;
 }

 function Actor(actorXML, now) {
     this.start_time = new JustTime(actorXML.@start);
     this.end_time = new JustTime(actorXML.@end);
     this.status = actorXML.@status;
     if (this.status == "done") {
     	if (this.end_time.before(now)) {
     		this.status = "purged";
     	} else {
     		this.status = "buffered";
     	}
     } else if (this.status == "progress" && this.start_time.before(now)) {
		this.status = "progress_late";
     } else if (this.status == "future" && this.start_time.before(now)) {
		this.status = "future_late";
     }
 }

 function JustTime(str) {
     var time = new Date(parseInt(str));
     time.setFullYear(2007);
     time.setMonth(0);
     time.setDate(1);
     this.time = time;
     this.time_string = this.time.getHours() + ":" + fill(this.time.getMinutes());
     this.second_string = this.time_string + ":" + fill(this.time.getSeconds());
     this.max = function(other_time) {
	 if (other_time.time > this.time) {
	     return other_time;
	 } else {
	     return this;
	 }
     }
     this.min = function(other_time) {
	 if (other_time.time < this.time) {
	     return other_time;
	 } else {
	     return this;
	 }
     }
     this.diff = function(other_time) {
	 return this.time - other_time.time;
     }

	this.before = function(other_time) {
		return this.time < other_time.time;
	}
     this.after = function(interval) {
	 var new_time = this.time.getTime() + parseInt(interval);
	 return new JustTime(new_time);
     }
 }

function fill(num) {
    if (num < 10) {
	return "0" + num;
    } else
	return num.toString();
}

 function JustDay(str) {
     var day = new Date(parseInt(str));
     day.setHours(0); 
     day.setMinutes(0);
     day.setSeconds(0);
     day.setMilliseconds(0);
     this.day = day;

     this.gt = function(other_day) {
	 return this.day > other_day.day;
     }

     this.date_string = (this.day.getMonth()+1) + "/" + this.day.getDate();
     this.is_next_day_of = function(other_day) {
		 return (this.day - other_day.day == MILLISECONDS_IN_DAY);
     }
     this.days_in_between = function (other_day) {
     	return ceil(absdiff(this.day, other_day.day) / MILLISECONDS_IN_DAY);
     }
     this.height = DAY_TOTAL_HEIGHT;
     this.kind = "day";
}

function Spacer(day_1, day_2) {
    this.day_1 = day_1;
    this.day_2 = day_2;
    this.height = day_1.days_in_between(day_2) * SPACER_HEIGHT;
    this.date_string = "";
    this.kind = "spacer";
}

function load_stagelist(stagelistXML) {
	stagelist = new Stagelist(stagelistXML);
	redraw_canvas();
}

function snapshot() {
    var req_width=canvas_width + MARGIN_WIDTH;
    var req_height= canvas_height + MARGIN_HEIGHT;
    var snap = window.open('snapshot.html', "Snapshot-" + Date.now(),
			   "width=" + req_width +",height=" + req_height);
    snap.stagelistXML = stagelistXML;
}

function compare_days(day1, day2) {
    return (day1.day - day2.day); 
}

function snapshot_on_load() {
    on_load();
    stagelist = new Stagelist(stagelistXML);
    redraw_canvas();
}
