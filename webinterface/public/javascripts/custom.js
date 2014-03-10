(function($) {
  $(document).ready(function() {
    $(".start-deployment").not('disabled').on('click', function() {
      self = $(this);
      $(".start-deployment").not(self).addClass('disabled');
      self.find('.glyphicon').addClass('glyphicon-refresh');
      self.find('.glyphicon').addClass('rotating');
      self.find('span').removeClass('glyphicon-play');
    })
  })
}(jQuery))