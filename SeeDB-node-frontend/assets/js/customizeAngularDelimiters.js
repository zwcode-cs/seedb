(function(window){
  "use strict";

  var angular = window.angular;

  var myApp = angular.module("myApp", [], function($interpolateProvider) {
    $interpolateProvider.startSymbol("[[");
    $interpolateProvider.endSymbol("[[");
  });
}(this));
