(function (window) {
  "use strict";

  var io = window.io;
  var _ = window._;
  var Backbone = window.Backbone;

  var QueryProcessor = function () {
    this.socket = io.connect("/");

    _.extend(this, Backbone.Events);

    this.setTable = function(table) {
      var _this = this;
      _this.socket.emit("call", {methodName: "setTable", args:[table]}, function (response) {
        _this.getMetadata();
      });
    };

    this.setDistanceMeasure = function(distanceMeasure) {
      var _this = this;
      console.log("set distance measure");

      _this.socket.emit("call", {methodName: "setDistanceMeasure", args:[distanceMeasure]}, function(response) {
        return false;
      });
    };

    this.getMetadata = function() {
      var _this = this;
      _this.socket.emit("call", {methodName: "getMetadata", args:[]}, function (response) {
        _this.trigger("Metadata", response);
      });
    };

    this.submitQuery = function(query) {
      var _this = this;
      _this.socket.emit("call", {methodName: "setQuery", args: [query]}, function (response) {
          _this.socket.emit("call", {methodName: "Process", args: []}, function (response) {
            _this.trigger("ProcessResult", response);
          });
      });
    };
  };

  window.QueryProcessor = new QueryProcessor();
}(this));
