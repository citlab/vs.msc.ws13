(function ($, global) {
  "use strict";
  $(function () {
    global.topology_list = $('#topology-list');

    topology_list.add = function(time, bolt, msg) {
      var list = topology_list.find("tbody");
      var elem = $("<tr><td>" + time + "</td><td>" + bolt + "</td><td>" + msg + "</td></tr>");
      elem.data("filter-value", bolt);
      list.prepend(elem)
    };
  });
}(jQuery, this))
