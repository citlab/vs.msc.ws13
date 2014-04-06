(function ($, global) {
  "use strict";
  $(function () {
    global.server_control = $("#server-control");

    global.server_control.panel = {};
    global.server_control.panel.nimbus = $("#nimbus");
    global.server_control.panel.cassandra = $("#cassandra");
    global.server_control.panel.supervisor = $("#supervisor");

    global.server_control.panel.cassandra.started = function() {
      return $(global.server_control).find("[value=start_cassandra]").is(':disabled');
    }

    global.server_control.panel.cassandra.updateIp = function(value) {
      $("#server-cassandra-ip").html(value);
    }

    global.server_control.panel.cassandra.updateStatus = function(value) {
      $("#server-cassandra-status").html(value);
    }

    global.server_control.update = function() {
      $.ajax({
        url: 'server/update',
        dataType: 'json',
        type: 'POST',
        success: global.server_control.updateData,
        error: function() {
          console.log('Error in update Server Data');
        }
      });
    };

    global.server_control.updateData = function(data) {
      global.server_control.panel.nimbus.updateIp(data.Nimbus.ip);
      global.server_control.panel.nimbus.updateStatus(data.Nimbus.status);

      global.server_control.panel.cassandra.updateIp(data.Cassandra.ip);
      global.server_control.panel.cassandra.updateStatus(data.Cassandra.status);

      global.server_control.panel.supervisor.updateIp(data.Supervisor.ip);
      global.server_control.panel.supervisor.updateStatus(data.Supervisor.status);

      window.setTimeout(function() {global.server_control.update();}, 5000);
    }

    if(!$("#server-control").length == 0) {
      global.server_control.update();
    }
    
  });
}(jQuery, this))
