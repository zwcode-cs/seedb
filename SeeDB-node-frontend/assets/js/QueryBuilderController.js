(function(window) {
  "use strict";

  var QueryProcessor = window.QueryProcessor;

  angular.module("seeDB")
    .controller("QueryBuilderController", function ($scope) {
      $scope.predicates = [{
        columnName: "cand_nm",  // TODO: this is hardcoded as the first default predicate
        modifier: "=",
        value: "McCain, John S"
      }];

      $scope.tableNames = ["election_data", "election_data_full", "super_store_data"]; // TODO: hardcoded
      $scope.tableName = $scope.tableNames[0];

      $scope.setTable = function() {
        QueryProcessor.setTable($scope.tableName);
      };

      $scope.setMetadata = function(metadata) {
        var dimensionAttributes = metadata.dimensionAttributes.map(function(element) {
          return element;
        });
        var measureAttributes = metadata.measureAttributes.map(function(element) {
          return element;
        });

        var allAttributes = dimensionAttributes.concat(measureAttributes);

        $scope.$apply(function() {
          $scope.columnNames = allAttributes;
        });
      };

      QueryProcessor.on("Metadata", $scope.setMetadata);
      QueryProcessor.getMetadata();

      $scope.addPredicate = function () {
        this.predicates.push({
        });
      };

      $scope.removePredicate = function(predicateToRemove) {
        var index = this.predicates.indexOf(predicateToRemove);
        this.predicates.splice(index, 1);
      };

      $scope.distanceMeasures = ["EarthMoverDistance", "EuclideanDistance"];  //TODO: hardcoded
      $scope.distanceMeasure = $scope.distanceMeasures[0];

      $scope.setDistanceMeasure = function() {
        QueryProcessor.setDistanceMeasure($scope.distanceMeasure);
      };

      $scope.generateQuery = function() {
        var newQuery = "SELECT * FROM " + $scope.tableName;
        var predicateStrings = [];

        $scope.predicates.forEach(function(predicate) {
          var predicateString = predicate.columnName + " " + predicate.modifier + " '" + predicate.value + "'";
          predicateStrings.push("(" + predicateString + ")");
        });

        if(predicateStrings.length > 0) {
          newQuery += " WHERE " + predicateStrings.join(" AND ");
        }
        newQuery += ";";

        $scope.query = newQuery;
      };

      $scope.submitQuery = function() {
        QueryProcessor.submitQuery($scope.query);
      };
      
      $scope.$watch("predicates", $scope.generateQuery, true);
      $scope.$watch("tableName", $scope.generateQuery, true);
      $scope.$watch("tableName", $scope.setTable, true);
      $scope.$watch("distanceMeasure", $scope.setDistanceMeasure, true);
    });
}(this));
