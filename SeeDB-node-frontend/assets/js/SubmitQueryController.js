(function(window) {
  "use strict";

  var DOMManipulation = window.DOMManipulation;
  // declare variables
  var submitQueryController = function ($scope, queryBroadcast) {
    $scope.query1 = "";
    $scope.query2 = "";
    $scope.distanceMeasures = ["EarthMoverDistance", "EuclideanDistance", 
      "CosineDistance" ,"FidelityDistance" , "ChiSquaredDistance", "EntropyDistance"];  //TODO: hardcoded
    $scope.distanceMeasure = $scope.distanceMeasures[0];
    
    $scope.submitQuery = function() {
      $('#visualizations_div').show();
      QueryProcessor.submitQuery($scope.query);
    };

    $scope.setDistanceMeasure = function() {
      QueryProcessor.setDistanceMeasure($scope.distanceMeasure);
    };

    $scope.$on('handleQueryBroadcast', function() {
      if (queryBroadcast.query_num == 1)
        $scope.query1 = queryBroadcast.query;
      else
        $scope.query2 = queryBroadcast.query;
    });
  };
  submitQueryController.$inject = ['$scope', 'queryBroadcastService'];
  angular.module("seeDB").controller("SubmitQueryController", submitQueryController);
}(this));
