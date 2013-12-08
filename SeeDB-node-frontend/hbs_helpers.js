(function(){
  "use strict";

  var hbs = require("express-hbs");
  var js = js;
  var css = css;

  hbs.registerHelper("js", function(path) {
    return js(path);
  });

  hbs.registerHelper("css", function(path) {
    return css(path);
  });
}());
