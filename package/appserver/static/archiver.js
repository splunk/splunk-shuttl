$(document).ready(function() {

  // Handler for events
  bindHandlers();

  // Datetime-picker
  $('.datetime-input').datepicker({
    dateFormat:"yy-mm-dd",
    showOn: "button",
    buttonImage: Splunk.util.make_url('static/app/', getAppName() ,'/images/calendar_blue.png'),
    buttonImageOnly: true
  });

  // Validator
  $('#search-thaw-buckets-form').validate({
    errorPlacement: function(error, element) {
      error.insertAfter( element.next() );
    }
  });
  jQuery.validator.addClassRules({
    'datetime-input': {
      required: true,
      dateISO: true
    }
  });
});

function bindHandlers() {
    $('#search-thaw-button').bind('click', function(event){ searchOrThawBuckets(event); } );
    $('input').bind('keyup', function(event) { 
      if (event.keyCode == 13) {
        listBucketsPOST();
      }
    });
    $('#search-thaw-buckets-form').bind('change', function(event) { 
      if (isFormValid()) {
        $('#search-thaw-button').enable();
        listBucketsPOST();
      } else {
        $('#search-thaw-button').disable();
      }
    });
}

function getPostArguments(form) {
  //remove attributes with no data
  var serializedData = form.serializeArray();
  return $.map(serializedData, function(obj, i){
    if ( obj['value']=="" ) {
      return null;
    } else if( obj['name']=="index" && obj['value']=="*" ) {
      return null;
    } else {
      return obj;
    }
  });
}
function listBucketsGET() {
  $.ajax({
    url: 'list_buckets',

    type: 'GET',
    success: function(html) {
      $('#bucket-table').html(html);
    }
  });
}
function listBucketsPOST() {
  var data = getPostArguments($('form'));
  console.log(data);
  $.ajax({
    url: 'list_buckets',
    type: 'POST',
    data: data,
    success: function(html) {
      $('#bucket-table').html(html);
    }
  });
}
function thawBucketsGET() {
  var formData = $('form').serialize();
  $.ajax({
    url: 'thaw',
    type: 'GET',
    data: formData,
    success: function(html) {
      window.open(); 
      document.write(html);
    }
  });
}

function getAppName() {
  return $(top.document.body).attr("s:app") || 'UNKNOWN_APP_REALLY';
}

function isFormValid() {
  return $('#search-thaw-buckets-form').valid();
}


$.fn.enable = function() {
  $(this).removeAttr('disabled');
}
$.fn.disable = function() {
  $(this).attr('disabled', 'disabled');
}
$.fn.isEnabled = function() {
  return $(this).is(':enabled');
}

function searchOrThawBuckets(event) {
  var target = $(event.target);
  if (target.isEnabled()) {
    if (target.hasClass('search')) {
      listBucketsPOST();
    } else if (target.hadClass('thaw')) {
      console.log("thaw something!?");
      thawBucketsGET();
    }
  }
}
