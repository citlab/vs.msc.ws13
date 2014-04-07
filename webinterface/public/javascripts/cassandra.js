(function ($, global) {
  "use strict";
  $(function () {
    global.server_control.panel.cassandra.started = function() {
      return $(global.server_control).find("[value=start_cassandra]").is(':disabled');
    }

    global.server_control.panel.cassandra.status = "stopped";

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
        if(global.server_control.panel.cassandra.status == "stopping") {
          global.server_control.panel.cassandra.status = "stopped";
          $(global.server_control).find("[value=stop_cassandra] span").removeClass('rotating');
        }

        $("#server-cassandra-ip").attr('href', '#');
      } else {
        global.server_control.panel.cassandra.disableStart();
        if(global.server_control.panel.cassandra.status == 'stopped') {
          global.server_control.panel.cassandra.status = "running";
        } else if(global.server_control.panel.cassandra.status == 'starting') {
          global.server_control.panel.cassandra.status = "running";
          $(global.server_control).find("[value=start_cassandra] span").removeClass('rotating');
        }

        $("#server-cassandra-ip").attr('href', 'http://' + value + ':8080');
      }

      $("#server-cassandra-ip").html(value);
    }

    global.server_control.panel.cassandra.updateStatus = function(value) {
      $("#server-cassandra-status").html(global.server_control.panel.cassandra.status);
    }

    global.server_control.panel.cassandra.find("[value=start_cassandra]").on('click', function() {
      $(global.server_control).find("[value=start_cassandra] span").addClass('rotating');
      $(global.server_control).find("[value=start_cassandra]").attr('disabled', true);
      global.server_control.panel.cassandra.status = "starting";
      global.server_control.panel.cassandra.updateStatus();

      global.server_request("start","cassandra", global.server_control.panel.cassandra);
    });

    global.server_control.panel.cassandra.find("[value=stop_cassandra]").on('click', function() {
      $(global.server_control).find("[value=stop_cassandra] span").addClass('rotating');
      $(global.server_control).find("[value=stop_cassandra]").attr('disabled', true);
      global.server_control.panel.cassandra.status = "stopping";

      global.server_request("stop","cassandra", global.server_control.panel.cassandra);
    });
  });
}(jQuery, this))
