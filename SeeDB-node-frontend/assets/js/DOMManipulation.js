(function(window) {
  "use strict";

  var angular = window.angular;

  var DOMManipulation = function () {
    this.addQueryBuilder = function(query_num, add) {
      if (add) { 
        var beforeElement;
        $('.select_data_button_row').each(function(idx) {
          if ($(this).data('query') == query_num) {
            beforeElement = this;
            $(this).hide();
          }
        });

        $('.queryBuilder').each(function(idx) {
          if ($(this).data('query') == query_num) {
            $(beforeElement).after($(this));
            $(this).show();
          }
        });
      }
      else {
        $('.select_data_button_row').each(function(idx) {
          if ($(this).data('query') == query_num) {
            $(this).find('.btn').html("Edit Data");
            $(this).show();
          }
        });
        $('.queryBuilder').each(function(idx) {
          if ($(this).data('query') == query_num) {
            $(this).hide();
          }
        });
      }
    };

    this.addDatasetPanel = function() {
      var firstPanel = $('.datasetPanel');
      var secondPanel = $('.datasetPanel').clone();
      angular.element($(secondPanel)).scope().setQueryNum(2);
      $(firstPanel).after(secondPanel);
      $('.add-dataset').hide();
    }
  };
  window.DOMManipulation = new DOMManipulation();
}(this));