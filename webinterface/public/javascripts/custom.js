(function($, global) {
  $(document).ready(function() {
    $(".start-deployment").not('disabled').on('click', function() {
      self = $(this);
      $(".start-deployment").not(self).addClass('disabled');
      self.find('.glyphicon').addClass('glyphicon-refresh');
      self.find('.glyphicon').addClass('rotating');
      self.find('span').removeClass('glyphicon-play');
    })

    $(".ajax-form").ajaxForm({
      dataType: 'json',
      beforeSubmit: function(arr, $form, options) {
        console.log($form.find('button span'));
        $form.find('button span').addClass('glyphicon-refresh');
        $form.find('button span').addClass('rotating');
        $form.find('button span').removeClass('glyphicon-cloud-upload');
        $form.find('button').attr('disabled', true);
      },
      success: function(response, status, xhr, form) {
        location.reload();
      },
      error: function() {
        location.reload();
      }
    });

    $(".fancybox").fancybox();

    $(".start-form").ajaxForm({
      dataType: 'json',
      success: function(response, status, xhr, form) {
        
      },
      error: function() {
        
      }
    });

    global.server_request = function(action, server, obj, count) {
      var url = "server/" + server + "/" + action;
      if(count != undefined)
         url += "/" + count;

      $.ajax({
        url: url,
        data: {count: count},
        dataType: 'text',
        type: 'POST',
        error: function() {

          if(obj.status == "stopping")
            obj.status = "started";
          else
            obj.status = "stopped";

          global.server_control.panel.nimbus.updateStatus();
          global.server_control.panel.cassandra.updateStatus();
          global.server_control.panel.supervisor.updateStatus();

          $('.rotating').removeClass("rotating");
          console.log('Error at server action ' + server + ' - ' + action);
        }
      });
    }
  });
}(jQuery, this))