(function($) {
  $(document).ready(function() {
    $(".start-deployment").not('disabled').on('click', function() {
      self = $(this);
      $(".start-deployment").not(self).addClass('disabled');
      self.find('.glyphicon').addClass('glyphicon-refresh');
      self.find('.glyphicon').addClass('rotating');
      self.find('span').removeClass('glyphicon-play');
    })

    $("form").ajaxForm({
      dataType: 'json',
      error: function() {
        error_message("Etwas ist schief gelaufen");
      },
      success: function(response, status, xhr, form) {
        success_message(jQuery.parseJSON(response));
      }
    });

    function error_message(msg) {
      $('#main_message_field').addClass('alert-danger').html(msg);
    }

    function success_message(msg) {
      $('#main_message_field').addClass('alert-success').html(msg);
    }
  })
}(jQuery))