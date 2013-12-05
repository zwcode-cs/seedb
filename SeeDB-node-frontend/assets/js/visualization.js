(function (window) {
  "use strict";
  
  var Backbone = window.Backbone;
  var io = window.io;
  var $ = window.jQuery;
  var google = window.google;
  var _ = window._;
  var Handlebars = window.Handlebars;
  var document = window.document;
  var QueryProcessor = window.QueryProcessor;

  var GraphModel = Backbone.Model.extend({
      defaults: {
          query: "select * from election_data where cand_nm = 'McCain, John S';",
          width: 500,
          height: 400,
          response: null,
          charts: []
      },

      initialize: function() {
        QueryProcessor.on("ProcessResult", $.proxy(this.processData, this));
      },

      processData: function(response) {
          var _this = this;
          _this.set("response", response);
          this.set("charts", response.map($.proxy(_this.setChartAttributes, _this)));
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
          return chart;
      }
  });


  // Backbone View
  var GraphView = Backbone.View.extend({
      initialize: function(){
          this.model.on("change:charts", $.proxy(this.render, this));
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

