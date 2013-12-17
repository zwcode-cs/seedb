(function (window) {
  "use strict";

  var io = window.io;
  var _ = window._;
  var Backbone = window.Backbone;
  var crossfilterGlobal = window.crossfilter;
  var d3 = window.d3;
  var CrossfilterGraphs = window.CrossfilterGraphs;

  var QueryProcessor = function () {
    var _this = this;
    _.extend(this, Backbone.Events);
    
    this.socket = io.connect("/");
    
    this.setTable = function(table) {
      _this.socket.emit("call", {methodName: "setTable", args:[table]}, function () {
        _this.getMetadata();
        _this.getAllMeasureAggregatesForAllDimensionCombinations();
      });
    };

    this.setDistanceMeasure = function(distanceMeasure) {
      _this.socket.emit("call", {methodName: "setDistanceMeasure", args:[distanceMeasure]});
    };

    this.getMetadata = function() {
      var _this = this;
      _this.socket.emit("call", {methodName: "getMetadata", args:[]}, function (response) {
        _this.trigger("Metadata", response);
        _this.metadata = response;
        _this.getAllMeasureAggregatesForAllDimensionCombinations();
      });
    };

    this.submitQuery = function(query) {
      _this.socket.emit("call", {methodName: "setQuery", args: [query]}, function () {
          _this.socket.emit("call", {methodName: "Process", args: []}, function (response) {
            _this.trigger("ProcessResult", response);
          });
      });
    };

    this.getAllMeasureAggregatesForAllDimensionCombinations = function () {
      _this.included = {};

      _this.socket.emit("call", {methodName: "getAllMeasureAggregatesForAllDimensionCombinations", args: []}, function (response) {
        var crossfilter = crossfilterGlobal(response);

        var dimensions = {};
        _this.metadata.dimensionAttributes.forEach(function (dimensionAttribute) {
          var dimension = crossfilter.dimension(function (d) {
            return d[dimensionAttribute];
          });

          dimension.name = dimensionAttribute;

          dimensions[dimensionAttribute] = dimension;

          _this.included[dimensionAttribute] = {};
        });

        var charts = [];
        _.values(dimensions).forEach(function (dimension) {
          var group = dimension.group();

          var domain = group.top(20).map(function (obj) {return obj.key;});
          console.log(domain);
          var xScale = d3.scale.ordinal()
            .domain(domain)
            .rangePoints([0, 12*Math.min(group.size(), 20)], 0.5);

          xScale.invert = function(x) {
            return xScale.domain()[d3.bisect(xScale.range(), x+6) - 1];
          };

          var barChart = CrossfilterGraphs.barChart(dimension.name)
            .dimension(dimension)
            .group(group)
            .x(xScale);

          charts.push(barChart);
        });

        var chart = d3.select("#distributionCharts")
          .selectAll(".chart").data(charts)
          .enter()
            .append("div").attr("class", "chart");

        function render (method) {
          d3.select(this).call(method);
        }

        function renderAll () {
          chart.each(render);
        }

        renderAll();
      });
    };
  };

  window.QueryProcessor = new QueryProcessor();
}(this));
