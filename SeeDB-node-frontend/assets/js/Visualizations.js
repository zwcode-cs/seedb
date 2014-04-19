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
          var sortedByIndex = _.sortBy(
            _.pairs(scope.view.aggregateAttributeIndex), function (pair) {
              return pair[1];
            });

          var sortedAggregateAttrs = _.map(sortedByIndex, function (pair) {
            return pair[0];
          });

          scope.sortedAggregateAttrs = sortedAggregateAttrs;

          scope.charts = _.map(scope.sortedAggregateAttrs, function (aggAttr) {
            var index = scope.view.aggregateAttributeIndex[aggAttr];

            var headerRow = [[scope.view.groupByAttributes[0], 'Dataset 1', 'Dataset 2']];
            var dataRows = _.map(scope.view.result, function (aggValues, groupByValue) {
              return [groupByValue, aggValues[0][index], aggValues[1][index]];
            });

            var headerAndData = headerRow.concat(dataRows);

            console.log(headerAndData);

            return {
              index: index,
              data: google.visualization.arrayToDataTable(headerAndData),
              options: {
                title: aggAttr,
                chartArea: {
                  left: 0
                }
              },
              type: "ColumnChart"
            };
          });
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
          var chart = new google.visualization[scope.type](element.get(0));
          chart.draw(scope.data, scope.options);
        }
      };
    });
}(this));