(function(window) {
    "use strict";

    var SeeDB = window.SeeDB;
    var google = window.google;
    var angular = window.angular;

    var chartWidth = 1000;
    var chartHeight = 400;

    // Draw Google Charts 
    google.load('visualization', '1.0', {
        'packages': ['corechart']
    });

    angular.module("seeDB")
        .controller("VisualizationsController", function($scope) {
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
                    queryDistribution[distributionUnit.attributeValue] = distributionUnit.fraction;
                });

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

                if (data.getNumberOfRows() > 20) {  // too many rows, can't deal with it
                    var rowsToLeave = 20;
                    data.removeRows(rowsToLeave, data.getNumberOfRows() - rowsToLeave);
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
                    $scope.views = response;
                });
            };

            SeeDB.on("views", processData);

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
                    // var googleChart = new google.visualization[chart.chartType](element);
                    // googleChart.draw(chart.dataTable, chart.options);
                }
            };
        })

        // Cardinality View
        .directive("cardinalityView", function () {
            return {
                scope: {
                    view: "=view"
                },
                link: function (scope, element) {
                    var data = google.visualization.arrayToDataTable([
                      ['Value', 'Dataset 1', 'Dataset 2'],
                      ['Cardinality',  scope.view.cardinalities[0], scope.view.cardinalities[1]]
                    ]);

                    var options = {
                      title: 'Cardinality',
                      vAxis: {
                        textPosition: "none"
                      },
                      chartArea: {
                        left: 0
                      }
                    };

                    var chart = new google.visualization.BarChart(element.get(0));
                    chart.draw(data, options);
                }
            };
        })

        // Cardinality View
        .directive("dataSampleView", function () {
            return {
                scope: {
                    view: "=view"
                },
                link: function (scope, element) {
                    element.html("helloooo");
                }
            };
        });
}(this));