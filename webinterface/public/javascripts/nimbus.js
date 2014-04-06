(function ($, global) {
  "use strict";
  $(function () {
    global.server_control.panel.nimbus.started = function() {
      return $(global.server_control).find("[value=start_nimbus]").is(':disabled');
    }

    global.server_control.panel.nimbus.enableStart = function() {
      $(global.server_control).find("[value=start_nimbus]").attr('disabled', false);
      $(global.server_control).find("[value=stop_nimbus]").attr('disabled', true);
      $(global.server_control).find("[value=stop_nimbus] span").removeClass('rotating');
    }

    global.server_control.panel.nimbus.disableStart = function() {
      $(global.server_control).find("[value=start_nimbus]").attr('disabled', true);
      $(global.server_control).find("[value=stop_nimbus]").attr('disabled', false);
      $(global.server_control).find("[value=start_nimbus] span").removeClass('rotating');
    }

    global.server_control.panel.nimbus.updateIp = function(value) {
      if(value == "null"){
        value = "Nimbus nicht gestartet!";
        global.server_control.panel.nimbus.enableStart();
        $("#server-nimbus-ip").attr('href', '#');
      } else {
        global.server_control.panel.nimbus.disableStart();
        $("#server-nimbus-ip").attr('href', 'http://' + value + ':8080');
      }

      $("#server-nimbus-ip").html(value);
    }

    global.server_control.panel.nimbus.updateStatus = function(value) {
      $("#server-nimbus-status").html(value);
    }

    global.server_control.panel.nimbus.find("[value=start_nimbus]").on('click', function() {
      console.log($(global.server_control).find("[value=start_nimbus] span"));
      $(global.server_control).find("[value=start_nimbus] span").addClass('rotating');
      $(global.server_control).find("[value=start_nimbus]").attr('disabled', true);

      global.server_request("start","nimbus");
    });

    global.server_control.panel.nimbus.find("[value=stop_nimbus]").on('click', function() {
      $(global.server_control).find("[value=stop_nimbus] span").addClass('rotating');
      $(global.server_control).find("[value=stop_nimbus]").attr('disabled', true);

      global.server_request("stop","nimbus");
    });
  });
}(jQuery, this))
