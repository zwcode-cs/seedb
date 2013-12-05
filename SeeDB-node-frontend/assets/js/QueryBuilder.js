(function(window) {
  "use strict";

  var QueryProcessor = window.QueryProcessor;

  function QueryBuilderController($scope) {
    $scope.predicates = [{
      columnName: "cand_nm",  // TODO: this is hardcoded as the first default predicate
      modifier: "=",
      value: "McCain, John S"
    }];

    $scope.tableNames = ["election_data", "superstore"]; // TODO: hardcoded
    $scope.tableName = $scope.tableNames[0];

    $scope.columnNames = ["cand_nm", "contbr_st"];  // TODO: hardcoded

    $scope.addPredicate = function () {
      this.predicates.push({
      });
    };

    $scope.removePredicate = function(predicateToRemove) {
      var index = this.predicates.indexOf(predicateToRemove);
      this.predicates.splice(index, 1);
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
  }

  window.QueryBuilderController = QueryBuilderController;
}(this));
