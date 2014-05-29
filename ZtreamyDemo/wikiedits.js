var _MINUTES = 60;
var _MAX = 10

function xformat(x) {
  var minute = Math.floor(x/2);
  var part = x%2;
  if (part == 0) {
     return minute+":00"
  } else {
     return minute+":30"
  } 
}

function findPopular(cache) {
   var items = []
   var values = []
   var minimum = 0
   cache.forEach(function(k, v) {
      if (items.length < _MAX) {
          values.push(v);
	  items.push({name: k, value: v});
          minimum = Math.min.apply(Math, values);
      } else {
          if (v > minimum) {
	      idx = values.indexOf(minimum);
              items.splice(idx, 1);
              values.splice(idx, 1);
	      items.push({name: k, value: v});
	      values.push(v);
	      minimum = Math.min.apply(Math, values);
          }
      }
   }, true);
   items.sort(function(a,b){return b.value-a.value});
   return items;
}


function plotPopulars(data) {

  d3.select("#populars_chart_container")
    .selectAll("div")
    .data(data)
    .enter()
    .append("div").html(
	function(d) { 
		return "<a target=\"_blank\" href=\"http://en.wikipedia.org/wiki/" + d.name + "\">" + d.name + "</a>"; 
	}
    ).append("div").attr("class", "bar")
    .style("width", function(d) { return ((d.value * 2) + 5) + "px"; })
    .text(function(d) { return d.value; });
}


function initEditionsPerEventGraph() {

  var initData = []
  
  for (idx=0; idx < _MINUTES*2; idx++) {
     initData[idx] = {x: idx, y: 0}		
  }

  var _editionsGraph = new Rickshaw.Graph( {
     element: document.querySelector("#editions_chart"), 
     width: 320, 
     height: 240, 
     renderer: 'bar',
     series: [{
       color: 'steelblue',
       data: initData,
       name: "Editions"
     }]
  });

  var hoverDetail = new Rickshaw.Graph.HoverDetail( {
    graph: _editionsGraph,
    xFormatter: xformat
  });
	
  var xAxis = new Rickshaw.Graph.Axis.X( {
    graph: _editionsGraph,
    orientation: 'bottom',
    element: document.getElementById('editions_x_axis'),
    tickFormat: xformat
  });

  var yAxis = new Rickshaw.Graph.Axis.Y( {
    graph: _editionsGraph,
    orientation: 'left',
    element: document.getElementById('editions_y_axis'),
  });
		
  return _editionsGraph;
}


function plotEditionsPerEvent(editionsPerEvent, graph) {
  var editions = [];

  for (idx=0; idx < _MINUTES*2; idx++) {
    if (idx >= editionsPerEvent.length) {
	editions.push({x: idx, y: 0});
    } else {
	editions.push({x: idx, y: editionsPerEvent[idx]});
    }
  }

  graph.series[0].data = editions;
  graph.update();
}

