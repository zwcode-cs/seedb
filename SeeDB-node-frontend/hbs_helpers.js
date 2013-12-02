var hbs = require("express-hbs");

hbs.registerHelper("js", function(path) {
  return js(path);
});

hbs.registerHelper("css", function(path) {
  return css(path);
});
