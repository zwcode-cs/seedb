(function (window) {
  "use strict";

  var io = window.io;
  var _ = window._;
  var Backbone = window.Backbone;
  var crossfilterGlobal = window.crossfilter;
  var d3 = window.d3;
  var CrossfilterGraphs = window.CrossfilterGraphs;

  var SUMMARY_DISTRIBUTION_MAX_COUNT = 20;

  var QueryProcessor = function () {
    var _this = this;
    _.extend(this, Backbone.Events);
    
    this.socket = io.connect("/");
    
    this.setTable = function(table) {
      _this.socket.emit("call", {methodName: "setTable", args:[table]}, function () {
        _this.getMetadata();
        _this.getDistributionsForAllDimensions();
        console.log("setTable called");
      });
    };

    this.setDistanceMeasure = function(distanceMeasure) {
      _this.socket.emit("call", {methodName: "setDistanceMeasure", args:[distanceMeasure]}, function () {
        return false; // TODO what if it fails somehow?
      });
    };

    this.getMetadata = function() {
      var _this = this;
      _this.socket.emit("call", {methodName: "getMetadata", args:[]}, function (response) {
        _this.trigger("Metadata", response);
        _this.metadata = response;
      });
    };

    this.submitQuery = function(query) {
      _this.socket.emit("call", {methodName: "setQuery", args: [query]}, function () {
          _this.socket.emit("call", {methodName: "Process", args: []}, function (response) {
            _this.trigger("ProcessResult", response);
          });
      });
    };

    this.getDistributionsForAllDimensions = function () {
      _this.socket.emit("call", {methodName: "getDistributionsForAllDimensions", args: [SUMMARY_DISTRIBUTION_MAX_COUNT]}, function (response) {
        _this.trigger("DistributionsForAllDimensions", response);
        _this.dimensionDistributions = response;
        _this.getAllMeasureAggregatesForAllDimensionCombinations();
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

        var groupOfAll = crossfilter.groupAll();

        function reduceAdd(p, v) {
          p[v.cand_nm] += v.contb_receipt_amt;
          return p;
        }

        function reduceRemove(p, v) {
          p[v.cand_nm] -= v.contb_receipt_amt;
          return p;
        }

        function reduceInitial() {
          var p = {};

          _this.dimensionDistributions.cand_nm.forEach(function (distributionUnit) {
            p[distributionUnit.attributeValue] = 0;
          });

          return p;
        }

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

        groupOfAll.reduce(reduceAdd, reduceRemove, reduceInitial);

        dimensions["contbr_st"].filterExact("AZ");
        console.log(groupOfAll.value());

        dimensions["contbr_st"].filterAll();
        dimensions["cand_nm"].filterExact("McCain, John S");
        console.log(groupOfAll.value());

      });
    };
  };

  window.QueryProcessor = new QueryProcessor();
}(this));
