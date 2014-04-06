(function ($, global) {
  "use strict";
  $(function () {
    global.server_control.panel.cassandra.started = function() {
      return $(global.server_control).find("[value=start_cassandra]").is(':disabled');
    }

    global.server_control.panel.cassandra.enableStart = function() {
      $(global.server_control).find("[value=start_cassandra]").attr('disabled', false);
      $(global.server_control).find("[value=stop_cassandra]").attr('disabled', true);
      $(global.server_control).find("[value=reboot_cassandra]").attr('disabled', true);
    }

    global.server_control.panel.cassandra.disableStart = function() {
      $(global.server_control).find("[value=start_cassandra]").attr('disabled', true);
      $(global.server_control).find("[value=stop_cassandra]").attr('disabled', false);
      $(global.server_control).find("[value=reboot_cassandra]").attr('disabled', false);
    }

    global.server_control.panel.cassandra.updateIp = function(value) {
      if(value == "null"){
        value = "Cassandra nicht gestartet!";
        global.server_control.panel.cassandra.enableStart();
      } else {
        global.server_control.panel.cassandra.disableStart();
      }

      $("#server-cassandra-ip").html(value);
    }

    global.server_control.panel.cassandra.updateStatus = function(value) {
      $("#server-cassandra-status").html(value);
    }

    global.server_control.panel.cassandra.find("[value=start_cassandra]").on('click', function() {
      global.server_request("start","cassandra");
    });

    global.server_control.panel.cassandra.find("[value=stop_cassandra]").on('click', function() {
      global.server_request("stop","cassandra");
    });

    global.server_control.panel.cassandra.find("[value=reboot_cassandra]").on('click', function() {
      global.server_request("reboot","cassandra");
    });
  });
}(jQuery, this))
