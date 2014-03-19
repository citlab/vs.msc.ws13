(function ($, global) {
  "use strict";
  $(function () {
    global.topology_list = $('#topology-list');

    global.topology_list.lastId = 0;

    topology_list.add = function(time, bolt, msg, id, hasError) {
      var list = topology_list.find("tbody");
      var style = topology_filter.tableStyle();
      var cssClass = bolt;
      if(hasError) cssClass += " danger";
      var elem = $("<tr " + style + " class='" + cssClass + "'><td>" + time + "</td><td>" + bolt + "</td><td>" + msg + "</td></tr>");
      elem.data("filter-value", bolt);
      list.prepend(elem)

      global.topology_filter.add(bolt);
      global.topology_list.lastId = id;
    };

    topology_list.fetchLatest = function() {
      $.ajax( {
        url: 'log/' + global.topology_list.lastId,
        dataType: 'json',
        type: 'POST',
        success: global.topology_list.addJson,
        error: function() {
          alert('Test');
        }
      });
    }

    topology_list.addJson = function( data ) {
      var obj = data;

      $.each(data, function(i, v) {
        global.topology_list.add(v.time, v.bolt, v.message, v.id, v.hasError);
      });

      window.setTimeout(function() {global.topology_list.fetchLatest();}, 1000);
    }

    if(!$('#topology-list').length == 0) {
      topology_list.fetchLatest();
    }
  });
}(jQuery, this))
