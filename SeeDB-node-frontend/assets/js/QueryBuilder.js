(function(window) {
  "use strict";

  var SeeDB = window.SeeDB;
  var angular = window.angular;

  angular.module("seeDB")
    .directive("queryBuilder", function () {
      return {
        scope: {
          query: "="
        },
        templateUrl: "/queryBuilder.html",
        controller: function ($scope) {
          $scope.predicates = [{
            columnName: "cand_nm",  // TODO: this is hardcoded as the first default predicate
            modifier: "=",
            value: "Obama, Barack"
          }];

          $scope.tableNames = ["election_data_full", "election_data", "super_store_data"]; // TODO: hardcoded
          $scope.tableName = $scope.tableNames[0];

          $scope.setTable = function() {
            SeeDB.setTable($scope.tableName);
          };

          $scope.columnNames = ["cand_nm", "contbr_zip"];

          $scope.addPredicate = function () {
            this.predicates.push({
            });
          };

          $scope.removePredicate = function(predicateToRemove) {
            var index = this.predicates.indexOf(predicateToRemove);
            this.predicates.splice(index, 1);
          };

          $scope.distanceMeasures = ["EarthMoverDistance", "EuclideanDistance", "CosineDistance" ,"FidelityDistance" , "ChiSquaredDistance", "EntropyDistance"];  //TODO: hardcoded
          $scope.distanceMeasure = $scope.distanceMeasures[0];

          $scope.setDistanceMeasure = function() {
            SeeDB.setDistanceMeasure($scope.distanceMeasure);
          };

          $scope.generateQuery = function() {
            var newQuery = "SELECT * FROM " + $scope.tableName;
            var predicateStrings = [];

            $scope.predicates.forEach(function(predicate) {
              var predicateString;

              if (predicate.modifier === "in") {
                predicateString = predicate.columnName + " " + predicate.modifier + " (" + predicate.value + ")";
              } else {
                predicateString = predicate.columnName + " " + predicate.modifier + " '" + predicate.value + "'";
              }

              predicateStrings.push("(" + predicateString + ")");
            });

            if(predicateStrings.length > 0) {
              newQuery += " WHERE " + predicateStrings.join(" AND ");
            }
            newQuery += ";";

            $scope.query = newQuery;
          };

          $scope.submitQuery = function() {
            SeeDB.submitQuery($scope.query);
          };
          
          $scope.$watch("predicates", $scope.generateQuery, true);
          $scope.$watch("tableName", $scope.generateQuery, true);
          $scope.$watch("tableName", $scope.setTable, true);
          $scope.$watch("distanceMeasure", $scope.setDistanceMeasure, true);
        }
      };
    });
}(this));
