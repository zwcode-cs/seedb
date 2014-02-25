(function(window) {
  "use strict";

  var DOMManipulation = window.DOMManipulation;
  // declare variables
  var datasetController = function ($scope, queryBroadcast) {
    $scope.query_num = 1;
    $scope.expanded = false;
    $scope.selectionTypes = ["SQL", "Visual", "Upload"];
    $scope.selectionType = $scope.selectionTypes[0];
    $scope.query = "No data selected";
    $scope.populateDataSelector = function() {
      // remove the select data and replace with pane to select data
      $scope.expanded = true;
      DOMManipulation.addQueryBuilder($scope.query_num /* query_num*/, true/*add*/);
    }; 

    $scope.init = function(query_num) {
      $scope.query_num = query_num;
    }

    $scope.$on('handleQueryBroadcast', function() {
      if ($scope.expanded) {
        if (queryBroadcast.query_num == $scope.query_num){
          $scope.query = queryBroadcast.query;
        }
      }
    });

    $scope.addQuery = function() {
      $(".datasetPanel").each(function() {
        if ($(this).data('query') == 2) {
          $(this).show();
        }
        else {
          $(this).find(".add-dataset").hide();
        }
      })
    };

    $scope.setQueryNum = function(query_num) {
      $scope.query_num = query_num;
    };
  };
  datasetController.$inject = ['$scope', 'queryBroadcastService'];
  angular.module("seeDB").controller("DatasetController", datasetController);
}(this));

