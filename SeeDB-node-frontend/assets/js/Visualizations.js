(function(window) {
  "use strict";

  var SeeDB = window.SeeDB;
  var google = window.google;
  var angular = window.angular;

  // Draw Google Charts 
  google.load('visualization', '1.0', {
    'packages': ['corechart', 'table']
  });

  angular.module("seeDB")
    .controller("VisualizationsController", function($scope) {
      $scope.charts = [];

      var updateViews = function(response) {
        $scope.$apply(function() {
          $scope.views = response;
        });
      };

      SeeDB.on("views", updateViews);

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

    // Data Sample View
    .directive("sampleView", function () {
      return {
        scope: {
          view: "=view"
        },
        templateUrl: "sampleView.html",
        link: function (scope, element) {
          // iterate over datasets
          scope.charts = _.map(scope.view.rows, function (rowsOfDataset, index) {
            var headerRow = [scope.view.columnNames];
            var dataRows = rowsOfDataset;

            var headerAndData = headerRow.concat(dataRows);

            return {
              data: google.visualization.arrayToDataTable(headerAndData),
              options: {
                title: "Dataset " + (index + 1) + " Sample Rows",
                height: "300px",
                width: "100%"
              },
              type: "Table"
            };
          });
        }
      };
    })

    // Aggregate View
    .directive("aggregateView", function () {
      return {
        scope: {
          view: "=view"
        },
        link: function (scope, element) {
          // we need one graph per aggregate
          function generateCharts() {
            var aggAttr = scope.view.aggregateAttribute;
            var gbAttr = scope.view.groupByAttributes;
            var headerRow = [[gbAttr, 'Dataset 1', 'Dataset 2']];
            var dataRows = [];
            var charts = [];
            var titles = [];
            var diffs = [];
            function sortDist(a,b) {
              if (a[3] > b[3]) return -1;
              if (a[3] < b[3]) return 1;
              return 0;
            }
            var max1 = -1;
            var max2 = -1;

            dataRows[0] = _.map(scope.view.result, function (aggValues, groupByValue) {
              if (max1 < aggValues.datasetValues[0].generic)
                max1 = aggValues.datasetValues[0].generic;
              if (max2 < aggValues.datasetValues[1].generic)
                max2 = aggValues.datasetValues[1].generic;

              return [
                groupByValue, 
                aggValues.datasetValues[0].generic, 
                aggValues.datasetValues[1].generic,
                Math.abs(aggValues.datasetValues[0].genericNormalized - aggValues.datasetValues[1].genericNormalized)
                ];
            });
            dataRows[0].sort(sortDist);
            titles[0] = scope.view.function + "(" + aggAttr + ") vs. " + gbAttr;

            /*dataRows[1] = _.map(scope.view.result, function (aggValues, groupByValue) {
              return [
                groupByValue, 
                aggValues.datasetValues[0].sumNormalized, 
                aggValues.datasetValues[1].sumNormalized,
                Math.abs(aggValues.datasetValues[0].sumNormalized - aggValues.datasetValues[1].sumNormalized)
                ];
            });
            dataRows[1].sort(sortDist);
            titles[1] = "SUM(" + aggAttr + ") vs. " + gbAttr;

            dataRows[2] = _.map(scope.view.result, function (aggValues, groupByValue) {
              return [
                groupByValue, 
                aggValues.datasetValues[0].averageNormalized, 
                aggValues.datasetValues[1].averageNormalized,
                Math.abs(aggValues.datasetValues[0].averageNormalized - aggValues.datasetValues[1].averageNormalized)
                ];
            });
            dataRows[2].sort(sortDist);
            titles[2] = "AVG(" + aggAttr + ") vs. " + gbAttr;*/

            function truncateDist(dist) {
              return dist.slice(0, 3);
            }

            for (var i = 0; i < 1; i++) {
              var headerAndData = headerRow.concat(dataRows[i].slice(0,20).map(truncateDist));
              charts.push({
                data: google.visualization.arrayToDataTable(headerAndData),
                utility: scope.view.utility,
                options: {
                  title: titles[i],
                  chartArea: {
                    //left: -5
                  },
                  vAxes: {
                    0: {logScale: false, maxValue: max1},
                    1: {logScale: false, maxValue: max2}},
                  series: {
                    0:{targetAxisIndex:0},
                    1:{targetAxisIndex:1}}
                },
                type: "ColumnChart"
              });
            }
            return charts;
          }
          scope.charts = generateCharts();
        },
        templateUrl: "aggregateView.html"
      };
    })

    // Aggregate View
    .directive("googleChart", function () {
      return {
        scope: {
          data: "=",
          options: "=",
          type: "="
        },
        link: function (scope, element) {
          var chart = new google.visualization.ColumnChart(element.get(0));
          chart.draw(scope.data, scope.options);
        }
      };
    });
}(this));