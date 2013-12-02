var socket = io.connect("/");
    socket.on("connect", function() {
      socket.emit("call", {methodName: "setQuery", args: ["select * from election_data ;"]}, function (response) {
        socket.emit("call", {methodName: "Process", args: []}, function (response) {
          console.log(jQuery.parseJSON(response));
        });
      });
    });