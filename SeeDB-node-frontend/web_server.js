var java = require("java");
var io = require("socket.io");
var static = require("node-static");

java.classpath.push("../SeeDB-java-backend/lib/postgresql-9.2-1000.jdbc4.jar");
java.classpath.push("../SeeDB-java-backend/lib/py4j0.8.jar");
java.classpath.push("server.jar");

var file = new static.Server("./public");

var server = require("http").createServer(function(request, response) {
  request.addListener("end", function() {
    file.serve(request, response);
  }).resume();
}).listen(8000);

io = io.listen(server);

io.sockets.on("connection", function(socket) {
  var query_processor = java.newInstanceSync("core.QueryProcessor");

  // set up runtime settings
  var runtime_settings = java.newInstanceSync("utils.RuntimeSettings");
  query_processor.setRuntimeSettingsSync(runtime_settings);

  socket.on("call", function(options, callback) {
    response = query_processor[options.methodName + "Sync"].apply(query_processor, options.args);
    if(response) {
      var viewArray = [];
      var size = response.sizeSync();
      console.log(size);
      for(var i=0; i<size; i++) {
        viewArray.push(response.getSync(i));
      }

      // at this point, you have an array of DiscriminatingView objects
      console.log(viewArray);
    }

    callback(response);
  });
});
