(function ($, global) {
  "use strict";
  $(function () {
    global.topology_filter = $('#topology-list .filter');

    topology_filter.selected = "";

    topology_filter.add = function(name) {
      if(!topology_filter.include(name)) {
        var elem = $("<li>" + name + "</li>");
        elem.data("filter-value", name);
        topology_filter.append(elem);
      }
    };

    topology_filter.setFilter = function(filter) {
      topology_list.find(".hidden").removeClass('hidden');
      if(filter != "*") {
        topology_list.find("tbody tr").not("."+filter).addClass('hidden');
      }
    };

    topology_filter.find("li").on("click", function() {
      var filter = $(this).data("filter-value");
      topology_filter.setFilter(filter);
    });

    topology_filter.include = function (filter) {
      var elems = topology_filter.find("li");
      var r = false
      $.each(elems, function(k,v) {
        if(!r) {
          r = $(v).data("filter-value") == filter;
        }
      });
      return r;
    };
  });
}(jQuery, this))
