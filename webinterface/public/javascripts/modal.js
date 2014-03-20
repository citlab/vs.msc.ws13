(function ($, global) {
  "use strict";
  $(function () {
    global.modal = $("#myModal");
    global.modal.content = global.modal.find(".modal-body");
    global.modal.title = global.modal.find(".modal-title");

    global.modal.setTitle = function(title) {
      global.modal.title.html(title);
    }

    global.modal.setContent = function(data) {
      var text = "";
      $.each(data, function(key, value) {
        if(key == "exception") {
          text += "<div class='panel panel-default'><div class='panel-heading'>"+ key +"</div><div class='panel-body'><pre>"+ value +"</pre></div></div>";
        } else {
          text += "<div class='panel panel-default'><div class='panel-heading'>"+ key +"</div><div class='panel-body'>"+ value +"</div></div>";
        }
      });

      global.modal.content.html(text);
    }
  });
}(jQuery, this))
