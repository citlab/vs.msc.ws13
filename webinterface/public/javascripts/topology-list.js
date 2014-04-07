(function ($, global) {
  "use strict";
  $(function () {
    global.topology_list = $('#topology-list');

    global.topology_list.lastId = 0;

    global.topology_list.logging = true;

    topology_list.add = function(time, bolt, msg, id, hasError) {
      var list = topology_list.find("tbody");
      var style = topology_filter.tableStyle();
      var cssClass = "normal";
      if(hasError) cssClass = "danger";
      var elem = $("<tr class="+ cssClass +" data-toggle='modal' data-target='#myModal' " + style + " data-value='" + bolt + "'><td>" + id + "</td><td>" + time + "</td><td>" + bolt + "</td><td>" + msg + "</td></tr>");
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
          console.log('Topology List fetch latest throws an error');
        }
      });
    }

    topology_list.addJson = function( data ) {
      var obj = data;

      $.each(data, function(i, v) {
        global.topology_list.add(v.time, v.bolt, v.message, v.id, v.hasError);
      });

      if(topology_list.logging) {
        window.setTimeout(function() {global.topology_list.fetchLatest();}, 1000);
      }
    }

    topology_list.toggleLogging = function() {
      topology_list.logging = !topology_list.logging;
      if(topology_list.logging) {
        topology_list.fetchLatest();
      }
    }

    topology_list.truncate = function() {
      topology_list.find('tbody').html('');
    };

    topology_list.on("click", "tr", function() {
      var id = $($(this).find("td")[0]).html();
      $.ajax({
        url: 'log/single/' + id,
        dataType: 'json',
        type: 'POST',
        success: function(data) {
          global.modal.setTitle("Weitere Log Details");
          global.modal.setContent(data);
        }
      });
    });

    if(!$('#topology-list').length == 0) {
      global.topology_list.fetchLatest();
    }
  });
}(jQuery, this))
