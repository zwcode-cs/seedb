(function(window) {
  "use strict";

  var QueryProcessor = window.QueryProcessor;
  var angular = window.angular;
  var DOMManipulation = window.DOMManipulation;

  var queryBuilderController = function ($scope, queryBroadcast) {
      $scope.predicates = [{
        columnName: "cand_nm",  // TODO: this is hardcoded as the first default predicate
        modifier: "=",
        value: "Obama, Barack"
      }];

      $scope.init = function(query_num) {
        $scope.query_num = query_num;
      }

      $scope.tableNames = ["donations_small"];
      //["election_data_full", "election_data", "super_store_data"]; // TODO: hardcoded
      $scope.tableName = $scope.tableNames[0];

      $scope.databaseNames = ["donations"];
      $scope.databaseName = $scope.databaseNames[0];

      $scope.setDatabase = function() {
        // connect to the right database
      }

      $scope.setTable = function() {
        QueryProcessor.setTable($scope.tableName, $scope.query_num);
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
        queryBroadcast.prepForBroadcast(newQuery, $scope.query_num);
      };

      $scope.drilldownPredicates = {};
      
      $scope.generateDrilldownPredicates = function(dimensionDrilldown) {
        $scope.$apply(function () {
          if(dimensionDrilldown.items) {
            $scope.drilldownPredicates[dimensionDrilldown.dimensionName] = dimensionDrilldown.items;
          } else {
            delete $scope.drilldownPredicates[dimensionDrilldown.dimensionName];
          }
        });
      };

      $scope.formatDrilldownPredicate = function(dimensionName, items) {
        var optionsString = items.map(function (item) {
          return "'" + item + "'";
        }).join(", ");
        return dimensionName + " in (" + optionsString + ")";
      };

      $scope.addDrillDownPredicate = function(key) {
        var options = $scope.drilldownPredicates[key];
        var optionsString = options.map(function (item) {
          return "'" + item + "'";
        }).join(", ");
        $scope.predicates.push({
          columnName: key,
          modifier: "in",
          value: optionsString
        });

        console.log($scope.predicates);
      };

      $scope.done = function(query_num) {
        DOMManipulation.addQueryBuilder(query_num, false);
      }

      QueryProcessor.on("filter", $scope.generateDrilldownPredicates);
      
      $scope.$watch("predicates", $scope.generateQuery, true);
      $scope.$watch("tableName", $scope.generateQuery, true);
      $scope.$watch("tableName", $scope.setTable, true);
      $scope.$watch("distanceMeasure", $scope.setDistanceMeasure, true);
  };

  var queryBroadcastService = function($rootScope) {
    var queryBroadcast = {};
    
    queryBroadcast.query = '';

    queryBroadcast.prepForBroadcast = function(msg, query_num) {
        this.query = msg;
        this.query_num = query_num;
        this.broadcastItem();
    };

    queryBroadcast.broadcastItem = function() {
        $rootScope.$broadcast('handleQueryBroadcast');
    };
    return queryBroadcast;
  };

  angular.module("seeDB").factory("queryBroadcastService", queryBroadcastService);
  queryBuilderController.$inject = ['$scope', 'queryBroadcastService']; 
  angular.module("seeDB")
    .controller("QueryBuilderController", queryBuilderController);

}(this));