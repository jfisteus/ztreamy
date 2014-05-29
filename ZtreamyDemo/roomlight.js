var _SECONDS = 60;
var _MAX = 10
var _COLORS = ["gold", "indigo", "red"];

function xformat(x) {
   return x + "s"
}

function yformat(y) {
   return y + " [mW/m2]"
}


function initLightSensorsGraph() {

  var _measurementsGraph = new Rickshaw.Graph( {
     element: document.querySelector("#editions_chart"), 
     width: 320, 
     height: 240, 
     renderer: 'line',
     // max: 300,
     series: [{
       color: 'steelblue',
       data: [{x: 0, y: 0}],
       name: "",
     }]
  });

  var hoverDetail = new Rickshaw.Graph.HoverDetail( {
    graph: _measurementsGraph,
    xFormatter: xformat,
    yFormatter: yformat,
  });
	
  var xAxis = new Rickshaw.Graph.Axis.X( {
    graph: _measurementsGraph,
    orientation: 'bottom',
    element: document.getElementById('editions_x_axis'),
    tickFormat: xformat
  });

  var yAxis = new Rickshaw.Graph.Axis.Y( {
    graph: _measurementsGraph,
    orientation: 'left',
    element: document.getElementById('editions_y_axis'),
  });
		
  return _measurementsGraph;
}


function plotLightSensorsGraph(name, data, graph) {

  var measurements = [];

  for (idx=0; idx < _SECONDS; idx++) {
    if (idx >= data.length) {
	measurements.push({x: idx, y: 0});
    } else {
	measurements.push({x: idx, y: Number(data[idx])});
    }
    console.log(measurements[idx]);
  }


  if (graph.series.length == 0) {
	graph.series.push({
       	   color: _COLORS[0],
       	   data: measurements,
           name: name
     });
  } else {

    // Look for the entry in the list of graphs
    var flag = false;
    for (idx=0; idx < graph.series.length; idx++) {
	if (graph.series[idx].name == name) {
	   graph.series[idx].data = measurements;
	   flag = true;
	   break;
	}
    }

    // Not found, append a new one
    if (flag == false) {
	graph.series.push({
       	   color: _COLORS[graph.series.length],
       	   data: measurements,
           name: name
        })
    }
  }

  graph.update();
}

