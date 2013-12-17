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

    // candidates:
    // callJava (methodName)
    // callJava (methodName, arg1, arg2..)
    // callJava (methodName, callback)
    // callJava (methodName, arg1, arg2.., callback)
    function callJava () {
      // check for at least one argument
      if (arguments.length < 1) {
        throw "not enough arguments to callJava";
      }

      // check that first argument is a string and could be valid method name
      if (!_.isString(arguments[0])) {
        throw "first argument to callJava must be method name";
      }

      // allocate variables for arguments
      var methodName = arguments[0];
      var javaArgs = [];
      var callback;

      // loop through arguments, starting with second item since we have already processed methodName
      var i, argument;
      for (i = 1; i < arguments.length; i += 1) {
        argument = arguments[i];

        if (_.isFunction(argument)) {
          // if it is a function, it is the callback and should be the last argument
          callback = argument;
          if (i < arguments.length - 1) {
            console.warn("All arguments after the first function will be ignored in callJava.");
          }
          break;
        } else {
          // if it's not a function, add it to the list of arguments to send to the server
          javaArgs.push(argument);
        }
      }

      // send method call to socket
      _this.socket.emit("call", {methodName: methodName, args: javaArgs}, callback);
    }
    
    this.setTable = function(table) {
      callJava("setTable", table, function () {
        _this.getMetadata();
        _this.getAllMeasureAggregatesForAllDimensionCombinations();
      });
    };

    this.setDistanceMeasure = function(distanceMeasure) {
      callJava("setDistanceMeasure", distanceMeasure);
    };

    this.getMetadata = function() {
      var _this = this;
      callJava("getMetadata", function (response) {
        _this.trigger("Metadata", response);
        _this.metadata = response;
        _this.getAllMeasureAggregatesForAllDimensionCombinations();
      });
    };

    this.submitQuery = function(query) {
      callJava("setQuery", query, function () {
        callJava("Process", function (response) {
          _this.trigger("ProcessResult", response);
        });
      });
    };

    this.getAllMeasureAggregatesForAllDimensionCombinations = function () {
      _this.included = {};

      callJava("getAllMeasureAggregatesForAllDimensionCombinations", function (response) {
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
