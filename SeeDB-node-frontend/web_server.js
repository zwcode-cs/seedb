(function() {
  "use strict";

  var java = require("java");
  var io = require("socket.io");
  var express = require("express");
  var hbs = require("express-hbs");
  var connectAssets = require("connect-assets");

  var commandLineArgs = require('minimist')(process.argv.slice(2));

  if (commandLineArgs._[0] === "help") {
    console.info("help: display help");
    console.info("--port n: run on port n");
    console.info("--debug n: run java in debug mode, connect with Eclipse on port n");
    process.exit(0);
  }

  java.classpath.push("../SeeDB-java-backend/lib/postgresql-9.2-1000.jdbc4.jar");
  java.classpath.push("../SeeDB-java-backend/lib/py4j0.8.jar");
  java.classpath.push("../SeeDB-java-backend/lib/jackson-annotations-2.3.0.jar");
  java.classpath.push("../SeeDB-java-backend/lib/jackson-core-2.3.0.jar");
  java.classpath.push("../SeeDB-java-backend/lib/jackson-databind-2.3.0.jar");
  java.classpath.push("../SeeDB-java-backend/lib/guava-15.0.jar");
  java.classpath.push("../SeeDB-java-backend/bin/");

  java.options.push("-Xmx4g");

  // read debug command line option, and if it's present enable debug mode
  if (commandLineArgs.debug) {
    var debugPort = commandLineArgs.debug;

    java.options.push("-Xdebug");
    java.options.push("-agentlib:jdwp=transport=dt_socket,address=" +
      debugPort + ",server=y,suspend=n");
  }

  var app = express();
  app.engine("html", hbs.express3({
    partialsDir: __dirname + "/views/partials",
    contentHelperName: "content"
  }));
  app.set("view engine", "html");
  app.set("views", __dirname + "/views");

  app.use(connectAssets());

  // serve the HTML templates
  app.use(express.static(__dirname + "/views/partials"));

  app.get("/", function (req, res) {
    res.render("index");
  });

  hbs.registerHelper("js", function(path) {
    return js(path);
  });

  hbs.registerHelper("css", function(path) {
    return css(path);
  });

  var server = require("http").createServer(app);

  io = io.listen(server, {log: false});

  io.sockets.on("connection", function(socket) {
    var seeDB = java.newInstanceSync("api.SeeDB");

    seeDB.connectToDatabaseSync(0);
    seeDB.connectToDatabaseSync(1);

    socket.on("call", function(options, callback) {
      console.log("Calling method:", options.methodName, options.args);

      var javaArgs = options.args.concat([function (err, result) {
        var objectMapper = java.newInstanceSync("com.fasterxml.jackson.databind.ObjectMapper");
        var jsonObject = JSON.parse(objectMapper.writeValueAsStringSync(result));

        console.log("Returning result for: ", options.methodName);

        if (callback) {
          callback(jsonObject);
        }
      }]);

      seeDB[options.methodName].apply(seeDB, javaArgs);
    });
  });

  var port = 8000;

  // read port command line argument, if it's set change the default port
  if (commandLineArgs.port) {
    port = commandLineArgs.port;
  }

  server.listen(port);
  console.info("Call this program with the argument 'help' to list options.");
  console.info("Started SeeDB web server on port " + port + ".");
  console.info("Go to http://localhost:" + port + "/");

  if (commandLineArgs.debug) {
    console.info(" --- ");
    console.info("Running with debug mode. Please visit the app URL to start the Java server.");
  }
}());
