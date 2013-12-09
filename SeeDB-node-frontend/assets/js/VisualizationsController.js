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

            var dataTableFromDiscriminatingView = function (discriminatingView) {
                var keys = [];
                var datasetDistribution = {};
                var queryDistribution = {};

                discriminatingView.datasetDistribution.forEach(function (distributionUnit) {
                    keys.push(distributionUnit.attributeValue);
                    datasetDistribution[distributionUnit.attributeValue] = distributionUnit.fraction;
                });

                discriminatingView.queryDistribution.forEach(function (distributionUnit) {
                    keys.push(distributionUnit.attributeValue);
                    queryDistribution[distributionUnit.attributeValue] = distributionUnit.fraction;
                });

                keys = _.uniq(keys);
                keys.sort();

                var rows = [];
                keys.forEach(function (key) {
                    rows.push([key, queryDistribution[key], datasetDistribution[key]]);
                });

                var domainTitle = discriminatingView.aggregateAttribute;
                var queryRangeTitle = discriminatingView.groupByAttribute + " in query";
                var datasetRangeTitle = discriminatingView.groupByAttribute + " in dataset";

                // Create the data table.
                var data = new google.visualization.DataTable();
                data.addColumn("string", domainTitle);
                data.addColumn("number", queryRangeTitle);
                data.addColumn("number", datasetRangeTitle);
                data.addRows(rows);

                // sorts the data 
                data.sort([{column: 1, desc: true}]);

                if (data.getNumberOfRows() > 10) {  // too many rows, can't deal with it

                    // gets the rows for which the query fraction is less than 1/100th of the max fraction
                    var rowsToIgnore = data.getFilteredRows([{
                        column: 1,
                        maxValue: data.getValue(0, 1)/50 // first item in data
                    }]);

                    //remove those rows
                    data.removeRows(rowsToIgnore[0], rowsToIgnore.length);
                }

                return data;
            };

            var setChartAttributes = function (discriminatingView) {
                var dataTable = dataTableFromDiscriminatingView(discriminatingView);

                // determine chart type based on data
                var chartDataIsNumbers = dataTable.getColumnType(1) === "numbers";
                
                var chartType = "ColumnChart";
                if (dataTable.getNumberOfRows() > 10 && chartDataIsNumbers) {
                    chartType = "LineChart";
                }

                // Set chart options
                var options = {
                    'title': discriminatingView.aggregateAttribute + " vs " + discriminatingView.groupByAttribute,
                    'width': chartWidth,
                    'height': chartHeight
                };

                // update charts in the model
                var chart = {
                    dataTable: dataTable,
                    chartType: chartType,
                    utility: discriminatingView.utility,
                    numRows: discriminatingView.queryDistribution.length,
                    discriminatingView: discriminatingView,
                    options: options,
                    aggregateBy: discriminatingView.aggregateAttribute,
                    groupBy: discriminatingView.groupByAttribute
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