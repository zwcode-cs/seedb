(function(window) {
    "use strict";

    var SeeDB = window.SeeDB;
    var angular = window.angular;

    angular.module("seeDB")
        .controller("DatasetsController", function($scope) {
            $scope.twoDatasets = true;

            $scope.query1 = "select * from election_data where cand_nm='Cox, John H' and random()<0.1";
            $scope.query2 = "select * from election_data where cand_nm='McCain, John S' and random()<0.1";

            $scope.toggleTwoDatasets = function () {
                $scope.twoDatasets = ! $scope.twoDatasets;
            };

            $scope.submitQueries = function () {
                SeeDB.submitQueries($scope.query1, $scope.query2);
            };
        });
}(this));