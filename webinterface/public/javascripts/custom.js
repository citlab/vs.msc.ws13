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

    global.server_request = function(action, server, success, count) {
      var url = "server/" + server + "/" + action;
      if(count != undefined)
         url += "/" + count;

      $.ajax({
        url: url,
        data: {count: count},
        dataType: 'text',
        type: 'POST',
        success: success,
        error: function() {
          alert('Error at server action ' + server + ' - ' + action);
        }
      });
    }
  });
}(jQuery, this))