(function(window) {
  "use strict";

  var QueryProcessor = window.QueryProcessor;

  function QueryBuilderController($scope) {
    $scope.predicates = [{
      columnName: "cand_nm",
      modifier: "=",
      value: "McCain, John S"
    }];
    $scope.tableName = "election_data";
    $scope.columnNames = ["cand_nm", "contbr_st"];

    $scope.addPredicate = function () {
      this.predicates.push({
      });
    };

    $scope.removePredicate = function(predicateToRemove) {
      var index = this.predicates.indexOf(predicateToRemove);
      this.predicates.splice(index, 1);
    };

    $scope.generateQuery = function() {
      var newQuery = "select * from " + $scope.tableName;
      var predicateStrings = [];

      $scope.predicates.forEach(function(predicate) {
        var string = predicate.columnName + " " + predicate.modifier + " '" + predicate.value + "'";
        predicateStrings.push("(" + string + ")");
      });

      if(predicateStrings.length > 0) {
        newQuery += " where " + predicateStrings.join(" and ");
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
