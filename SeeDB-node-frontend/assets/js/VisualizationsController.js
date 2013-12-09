(function(window) {
    "use strict";

    var QueryProcessor = window.QueryProcessor;

    var chartWidth = 500;
    var chartHeight = 400;

    // Draw Google Charts 
    google.load('visualization', '1.0', {
        'packages': ['corechart']
    });

    angular.module("seeDB")
        .controller("VisualizationsController", function($scope) {
            $scope.width = 500;
            $scope.height = 400;
            $scope.charts = [];

            var setChartAttributes = function(element, index, array) {
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
            };

            var processData = function(response) {
                $scope.$apply(function() {
                    $scope.response = response;
                    $scope.charts = response.map(setChartAttributes);
                });
            };

            QueryProcessor.on("ProcessResult", processData);

        })
        .directive("seedbChart", function() {
            return {
                restrict: "E",
                scope: {
                    chart: "=chart"
                },
                link: function(scope, element) {
                    var chart = scope.chart;
                    element = element.get(0);

                    // Create the data table.
                    var data = new google.visualization.DataTable();
                    data.addColumn('string', chart.group_by);
                    data.addColumn('number', chart.aggregate_by);
                    data.addRows(_.pairs(chart.chartData));

                    // Set chart options
                    var options = {
                        'title': chart.aggregate_by,
                        'width': chartWidth,
                        'height': chartHeight
                    };

                    // Instantiate and draw our chart, passing in some options.
                    var googleChart = new google.visualization[chart.chartType](element);
                    googleChart.draw(data, options);
                }
            };
        });
}(this));