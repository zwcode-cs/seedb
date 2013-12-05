(function(window){
  "use strict";

  var angular = window.angular;

  var myApp = angular.module("seeDB", [], function($interpolateProvider) {
    $interpolateProvider.startSymbol("[[");
    $interpolateProvider.endSymbol("]]");
  });
}(this));
