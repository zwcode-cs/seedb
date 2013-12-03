// Backbone Model

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
        socket.on("connect", function() {
            _this.fetchData(socket);
        });
        this.on("change:query", this.fetchData);
    },

    fetchData: function(socket) {
        var _this = this;
        socket.emit("call", {methodName: "setQuery", args: [_this.get("query")]}, function (response) {
            socket.emit("call", {methodName: "Process", args: []}, function (response) {
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

        element.datasetDistribution.forEach(
            function(element, index, array) {
                chart.chartData[element.attributeValue] = element.fraction;
            }
        );

        // determine chart type based on data
        if (chart.chartData.length > 10 && !isNaN(parseFloat(chart.chartData[0]))) {
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
    render: function() {
        this.$el.html(Handlebars.templates.visualizations(this.model.attributes));
        var _this = this;

        // draw a chart for every item in charts
        this.model.get("charts").forEach(
            function(chart, index, array){

                // Create the data table.
                var data = new google.visualization.DataTable();
                data.addColumn('string', chart.group_by);
                data.addColumn('number', chart.aggregate_by);
                data.addRows(_.pairs(chart.chartData));

                // Set chart options
                var options = {'title': chart.aggregate_by,
                               'width': _this.model.get("width"),
                               'height': _this.model.get("height")};

                // Instantiate and draw our chart, passing in some options.
                var chart = new google.visualization[chart.chartType](document.getElementById('chart' + index));
                chart.draw(data, options);
            }
        );
    }
})

// Draw Google Charts 
google.load('visualization', '1.0', {'packages':['corechart']});
google.setOnLoadCallback(createViewAndModel);

function createViewAndModel() {
    var model = new GraphModel();
    $(function() { // only create the view when the page is ready
        new GraphView({ el: $("#visualizations"), model: model});
    });
}

