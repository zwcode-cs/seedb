(function(window) {
  "use strict";

  var SeeDB = window.SeeDB;
  var google = window.google;
  var angular = window.angular;

  // Draw Google Charts 
  google.load('visualization', '1.0', {
    'packages': ['corechart']
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
    .directive("dataSampleView", function () {
      return {
        scope: {
          view: "=view"
        },
        link: function (scope, element) {
          element.html("helloooo");
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
            return {
              index: scope.view.aggregateAttributeIndex[aggAttr],
              data: google.visualization.arrayToDataTable([
                ['Value', 'Dataset 1', 'Dataset 2'],
                ['Cardinality',  1, 2]
              ]),
              options: {
                title: 'Cardinality',
                vAxis: {
                  textPosition: "none"
                },
                  chartArea: {
                left: 0
                }
              }
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
          options: "="
        },
        link: function (scope, element) {
          var chart = new google.visualization.ColumnChart(element.get(0));
          chart.draw(scope.data, scope.options);
        }
      };
    });
}(this));