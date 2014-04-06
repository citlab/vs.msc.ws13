(function ($, global) {
  "use strict";
  $(function () {
    global.server_control.panel.supervisor.started = function() {
      return $(global.server_control).find("[value=start_supervisor]").is(':disabled');
    }

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
        value = "supervisor nicht gestartet!";
        global.server_control.panel.supervisor.enableStart();
        $("#server-supervisor-ip").attr('href', 'not_set');
      } else {
        global.server_control.panel.supervisor.disableStart();
        $("#server-supervisor-ip").attr('href', 'http://' + value + ':8080');
      }

      $("#server-supervisor-ip").html(value);
    }

    global.server_control.panel.supervisor.updateStatus = function(value) {
      $("#server-supervisor-status").html(value);
    }

    global.server_control.panel.supervisor.find("[value=start_supervisor]").on('click', function() {
      var count = $("#n_sv").val();
      global.server_request("start","supervisor", "", count);
    });

    global.server_control.panel.supervisor.find("[value=stop_supervisor]").on('click', function() {
      global.server_request("stop","supervisor");
    });

    global.server_control.panel.supervisor.find("[value=reboot_supervisor]").on('click', function() {
      global.server_request("reboot","supervisor");
    });
  });
}(jQuery, this))
