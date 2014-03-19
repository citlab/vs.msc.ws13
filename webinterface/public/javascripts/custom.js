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

    $(".start-form").ajaxForm({
      dataType: 'json',
      success: function(response, status, xhr, form) {
        
      },
      error: function() {
        
      }
    });
  });
}(jQuery, this))