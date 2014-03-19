(function ($, global) {
  "use strict";
  $(function () {
    global.topology_filter = $('#topology-list .filter');

    topology_filter.selected = "*";

    topology_filter.add = function(name) {
      if(!topology_filter.include(name)) {
        var elem = $("<li>" + name + "</li>");
        elem.data("filter-value", name);
        topology_filter.append(elem);
      }
    };

    topology_filter.setFilter = function(filter) {
      topology_list.find("tbody tr:hidden").show();
      if(filter != "*") {
        topology_list.find("tbody tr").not("."+filter).hide();
      }
    };

    topology_filter.on("click", "[data-filter-value]", function() {
      var filter = $(this).data("filter-value");
      topology_filter.setFilter(filter);
      topology_filter.selected = filter;
    });

    topology_filter.on("click", ".toggleLogging", function(e) {
      global.topology_list.toggleLogging();
      $(this).find("span").toggleClass("glyphicon-play");
      $(this).find("span").toggleClass("glyphicon-pause");
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

    topology_filter.tableStyle = function(filter) {
      if(topology_filter.selected == filter) {
        return "";
      } else if(topology_filter.selected == "*") {
        return "";
      } else {
        return "style='display: none'";
      }
    };
  });
}(jQuery, this))
