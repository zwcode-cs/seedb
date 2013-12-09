(function(window) {
    "use strict";

    var QueryProcessor = window.QueryProcessor;
    var google = window.google;
    var angular = window.angular;
    var _ = window._;

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

            var dataTableFromDiscriminatingView = function (dView) {
                var keys = [];
                var datasetDistribution = {};
                var queryDistribution = {};

                dView.datasetDistribution.forEach(function (dUnit) {
                    keys.push(dUnit.attributeValue);
                    datasetDistribution[dUnit.attributeValue] = dUnit.fraction;
                });

                dView.queryDistribution.forEach(function (dUnit) {
                    keys.push(dUnit.attributeValue);
                    queryDistribution[dUnit.attributeValue] = dUnit.fraction;
                });

                keys = _.uniq(keys);
                keys.sort();

                var rows = [];
                keys.forEach(function (key) {
                    rows.push([key, queryDistribution[key], datasetDistribution[key]]);
                });

                var domainTitle = dView.groupByAttribute;
                var queryRangeTitle = dView.aggregateAttribute + " in query";
                var datasetRangeTitle = dView.aggregateAttribute + " in dataset";

                // Create the data table.
                var data = new google.visualization.DataTable();
                data.addColumn("string", domainTitle);
                data.addColumn("number", queryRangeTitle);
                data.addColumn("number", datasetRangeTitle);
                data.addRows(rows);

                return data;
            };

            var setChartAttributes = function (dView) {
                var dataTable = dataTableFromDiscriminatingView(dView);

                // determine chart type based on data
                var chartDataIsNumbers = dataTable.getColumnType(1) === "numbers";
                
                var chartType = "ColumnChart";
                if (dataTable.getNumberOfRows() > 10 && chartDataIsNumbers) {
                    chartType = "LineChart";
                }

                // Set chart options
                var options = {
                    'title': dView.aggregateAttribute + " vs " + dView.groupByAttribute,
                    'width': chartWidth,
                    'height': chartHeight
                };

                // update charts in the model
                var chart = {
                    dataTable: dataTable,
                    chartType: chartType,
                    utility: dView.utility,
                    numRows: dView.queryDistribution.length,
                    dView: dView,
                    options: options,
                    aggregateBy: dView.aggregateAttribute,
                    groupBy: dView.groupByAttribute
                };

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

                    // Instantiate and draw our chart, passing in some options.
                    var googleChart = new google.visualization[chart.chartType](element);
                    googleChart.draw(chart.dataTable, chart.options);
                }
            };
        });
}(this));