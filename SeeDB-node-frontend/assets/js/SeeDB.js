(function (window) {
  "use strict";

  var io = window.io;
  var _ = window._;
  var Backbone = window.Backbone;

  var SeeDB = function () {
    var _this = this;
    _.extend(this, Backbone.Events);
    
    this.socket = io.connect("/");

    // candidates:
    // callJava (methodName)
    // callJava (methodName, arg1, arg2..)
    // callJava (methodName, callback)
    // callJava (methodName, arg1, arg2.., callback)
    function callJava () {
      // check for at least one argument
      if (arguments.length < 1) {
        throw "not enough arguments to callJava";
      }

      // check that first argument is a string and could be valid method name
      if (!_.isString(arguments[0])) {
        throw "first argument to callJava must be method name";
      }

      // allocate variables for arguments
      var methodName = arguments[0];
      var javaArgs = [];
      var callback;

      // loop through arguments, starting with second item since we have already processed methodName
      var i, argument;
      for (i = 1; i < arguments.length; i += 1) {
        argument = arguments[i];

        if (_.isFunction(argument)) {
          // if it is a function, it is the callback and should be the last argument
          callback = argument;
          if (i < arguments.length - 1) {
            console.warn("All arguments after the first function will be ignored in callJava.");
          }
          break;
        } else {
          // if it's not a function, add it to the list of arguments to send to the server
          javaArgs.push(argument);
        }
      }

      // send method call to socket
      _this.socket.emit("call", {methodName: methodName, args: javaArgs}, callback);
    }

    this.submitQueries = function(query1, query2) {
      callJava("initialize", query1, query2, function () {
        callJava("computeDifference", function (response) {
          _this.trigger("views", response);
          console.log(response);
        });
      });
    };
  };

  window.SeeDB = new SeeDB();
}(this));
