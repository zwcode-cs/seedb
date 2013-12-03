(function (window) {
  "use strict";
  
  var Backbone = window.Backbone;
  var io = window.io;
  var $ = window.jQuery;
  var google = window.google;
  var _ = window._;
  var Handlebars = window.Handlebars;
  var document = window.document;

  var GraphModel = Backbone.Model.extend({
      defaults: {
          query: "select * from election_data where cand_nm = 'McCain, John S';",
          width: 500,
          height: 400,
          response: null,
          charts: []
      },

      initialize: function() {
          var _this = this;
          var socket = io.connect("/");
          this.set("socket", socket);
          socket.on("connect", function() {
              _this.fetchData(socket);
          });
          this.on("change:query", $.proxy(_this.fetchData, this));
      },

      fetchData: function() {
          console.log("fetchData");
          var _this = this;
          _this.get("socket").emit("call", {methodName: "setQuery", args: [_this.get("query")]}, function (response) {
              _this.get("socket").emit("call", {methodName: "Process", args: []}, function (response) {
                  _this.set("response", response);
                  response.forEach($.proxy(_this.setChartAttributes, _this));
                  _this.trigger("change:charts");
              });
          });
      },

      setChartAttributes: function(element, index, array) {    
          var chart = {
              chartData: {}, 
              chartType: null,
              aggregate_by: element.aggregateAttribute,
              group_by: element.groupByAttribute,
              utility: element.utility,
              num_rows: element.datasetDistribution.length
          };

          element.datasetDistribution.forEach(function(element, index, array) {
              chart.chartData[element.attributeValue] = element.fraction;
          });

          // determine chart type based on data
          var chartDataIsNumbers = !isNaN(parseFloat(chart.chartData[0]));
          if (chart.chartData.length > 10 && chartDataIsNumbers) {
              chart.chartType = "LineChart";
          } else {
              chart.chartType = "ColumnChart";
          }

          // update charts in the model
          var charts = this.get("charts");
          charts.push(chart);
          this.set("charts", charts);
      }
  });


  // Backbone View
  var GraphView = Backbone.View.extend({
      initialize: function(){
          this.model.on("change:charts", $.proxy(this.render, this));
      },

      events: {
          "keypress input[type=text]"  : "updateOnEnter"
      },

      updateOnEnter: function(e) {
          if (e.type === "keypress" && e.keyCode === 13) {
            this.model.set("query", $("#query_input").val());
          }
      },
      render: function() {
          this.$el.html(Handlebars.templates.visualizations(this.model.attributes));
          var _this = this;

          // draw a chart for every item in charts
          this.model.get("charts").forEach(function(chart, index, array){
              // Create the data table.
              var data = new google.visualization.DataTable();
              data.addColumn('string', chart.group_by);
              data.addColumn('number', chart.aggregate_by);
              data.addRows(_.pairs(chart.chartData));

              // Set chart options
              var options = {
                  'title': chart.aggregate_by,
                  'width': _this.model.get("width"),
                  'height': _this.model.get("height")
              };

              // Instantiate and draw our chart, passing in some options.
              var googleChart = new google.visualization[chart.chartType](document.getElementById('chart' + index));
              googleChart.draw(data, options);
          });
      }
  });

  function createViewAndModel() {
      var model = new GraphModel();
      $(function() { // only create the view when the page is ready
          var view = new GraphView({
              el: $("#visualizations"),
              model: model
          });
      });
  }
  
  // Draw Google Charts 
  google.load('visualization', '1.0', {
      'packages': ['corechart']
  });
  google.setOnLoadCallback(createViewAndModel);

}(this));

