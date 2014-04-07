(function ($, global) {
  "use strict";
  $(function () {
    global.server_control.panel.supervisor.started = function() {
      return $(global.server_control).find("[value=start_supervisor]").is(':disabled');
    }

    global.server_control.panel.supervisor.status = "stopped";

    global.server_control.panel.supervisor.enableStart = function() {
      $(global.server_control).find("[value=start_supervisor]").attr('disabled', false);
      $(global.server_control).find("[value=stop_supervisor]").attr('disabled', true);
      $(global.server_control).find("[value=reboot_supervisor]").attr('disabled', true);
    }

    global.server_control.panel.supervisor.disableStart = function() {
      $(global.server_control).find("[value=start_supervisor]").attr('disabled', true);
      $(global.server_control).find("[value=stop_supervisor]").attr('disabled', false);
      $(global.server_control).find("[value=reboot_supervisor]").attr('disabled', false);
    }

    global.server_control.panel.supervisor.updateIp = function(value) {
      if(value == "null"){
        value = "Supervisor nicht gestartet!";
        global.server_control.panel.supervisor.enableStart();
        if(global.server_control.panel.supervisor.status == "stopping") {
          global.server_control.panel.supervisor.status = "stopped";
          $(global.server_control).find("[value=stop_supervisor] span").removeClass('rotating');
        }

        $("#server-supervisor-ip").attr('href', '#');
      } else {
        global.server_control.panel.supervisor.disableStart();
        if(global.server_control.panel.supervisor.status == 'stopped') {
          global.server_control.panel.supervisor.status = "running";
        } else if(global.server_control.panel.supervisor.status == 'starting') {
          global.server_control.panel.supervisor.status = "running";
          $(global.server_control).find("[value=start_supervisor] span").removeClass('rotating');
        }

        $("#server-supervisor-ip").attr('href', 'http://' + value + ':8080');
      }

      $("#server-supervisor-ip").html(value);
    }

    global.server_control.panel.supervisor.updateStatus = function(value) {
      $("#server-supervisor-status").html(global.server_control.panel.supervisor.status);
    }

    global.server_control.panel.supervisor.find("[value=start_supervisor]").on('click', function() {
      $(global.server_control).find("[value=start_supervisor] span").addClass('rotating');
      $(global.server_control).find("[value=start_supervisor]").attr('disabled', true);
      global.server_control.panel.supervisor.status = "starting";
      global.server_control.panel.supervisor.updateStatus();

      var count = $('#n_sv').val();

      global.server_request("start","supervisor", global.server_control.panel.supervisor, count);
    });

    global.server_control.panel.supervisor.find("[value=stop_supervisor]").on('click', function() {
      $(global.server_control).find("[value=stop_supervisor] span").addClass('rotating');
      $(global.server_control).find("[value=stop_supervisor]").attr('disabled', true);
      global.server_control.panel.supervisor.status = "stopping";

      global.server_request("stop","supervisor", global.server_control.panel.supervisor);
    });
  });
}(jQuery, this))
