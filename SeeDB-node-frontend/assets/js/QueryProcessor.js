(function (window) {
  "use strict";

  var io = window.io;
  var _ = window._;
  var Backbone = window.Backbone;

  var QueryProcessor = function () {
    this.socket = io.connect("/");

    _.extend(this, Backbone.Events);

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
