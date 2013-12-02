var java = require("java");
var io = require("socket.io");
var express = require("express");
var hbs = require("express-hbs");
var connectAssets = require("connect-assets");

require("./hbs_helpers.js");

java.classpath.push("../SeeDB-java-backend/lib/postgresql-9.2-1000.jdbc4.jar");
java.classpath.push("../SeeDB-java-backend/lib/py4j0.8.jar");
java.classpath.push("../SeeDB-java-backend/lib/jackson-annotations-2.3.0.jar");
java.classpath.push("../SeeDB-java-backend/lib/jackson-core-2.3.0.jar");
java.classpath.push("../SeeDB-java-backend/lib/jackson-databind-2.3.0.jar");
java.classpath.push("server.jar");

var app = express();
app.engine("hbs", hbs.express3({
  partialsDir: __dirname + "/views/partials",
  contentHelperName: "content"
}));
app.set("view engine", "hbs");
app.set("views", __dirname + "/views");

app.use(connectAssets());

app.get("/", function (req, res) {
  res.render("index");
});

var server = require("http").createServer(app);

io = io.listen(server);

io.sockets.on("connection", function(socket) {
  var query_processor = java.newInstanceSync("core.QueryProcessor");

  // set up runtime settings
  var runtime_settings = java.newInstanceSync("utils.RuntimeSettings");
  query_processor.setRuntimeSettingsSync(runtime_settings);

  socket.on("call", function(options, callback) {
    response = query_processor[options.methodName + "Sync"].apply(query_processor, options.args);
    objectMapper = java.newInstanceSync("com.fasterxml.jackson.databind.ObjectMapper");
    jsonObject = objectMapper.writeValueAsStringSync(response);

    callback(jsonObject);
  });
});

server.listen(8000);
