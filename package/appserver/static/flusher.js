// Debug
var logger = Splunk.Logger.getLogger("Splunk.Shuttl.Flusher");

$(document).ready(function() {

  logger.debug("Loaded Flusher");
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
  $('#flush-buckets-form').validate({
    errorPlacement: function(error, element) {
      error.insertAfter( element.next() );
    }
  });
  jQuery.validator.addMethod(
    "archiverInput", 
    function(value, element) {
      var check = false;
      var re = /^\d{4}-\d{1,2}-\d{1,2}$/;
      if (re.test(value)) {
        var adate = value.split('-');
        var yyyy = parseInt(adate[0], 10);
        var mm = parseInt(adate[1], 10);
        var dd = parseInt(adate[2], 10);
        bdate = new Date(yyyy, mm-1, dd);
        check = ( (bdate.getFullYear() == yyyy) && 
                  (bdate.getMonth() == mm-1) && 
                  (bdate.getDate() == dd) );
      } else if (value == "") {
        check = true;
      }
      return check; // || this.optional(element)
    }, 
    "Please enter a date in the format yyyy-mm-dd"
  );
  jQuery.validator.addClassRules({
    'datetime-input': {
      "archiverInput": true
      // dateISO: true
    }
  });

  // Load UI
  setSearchOrFlushButtonToSearch();
  $('.loadingBig').hide();
  $('#thaw-list').hide();
  $('form input:visible:enabled:first').focus();

  // Search for indexes and list all buckets
  listIndexesGET();
  listThawedPOST();


});

function bindHandlers() {
  $('#search-flush-button').bind('click', function(event){ searchOrFlushBuckets(event); } );
  $('input').bind('keyup', function(event) { 
    if (event.keyCode == 13) {
      setSearchOrFlushButtonToSearch();
      listThawedPOST();
    }
  });
  $('#flush-buckets-form').bind('change', function(event) { 
    $('#search-flush-button').enable();
    setSearchOrFlushButtonToSearch();
   });
}

function getPostArguments(form) {
  //remove attributes with no data
  var serializedData = form.serializeArray();
  return $.map(serializedData, function(obj, i){
    if ( obj['value']=="" ) {
      // return null;
    } else if( obj['name']=="index" && obj['value']=="*" ) {
      // return null;
    } else {
      return obj;
    }
  });
}

function listThawedPOST() {
    if (!isFormValid()) return;

    var data = getPostArguments($('form'));
    logger.debug('list buckets with post data: ' + data);

    loading();
    $.ajax({
        url: 'list_thawed',
        type: 'POST',
        data: data,
        success: function(html) {
            $('#thaw-list').html(html);
            setSearchOrFlushButtonToFlush();
        },
        complete: function() {
            loadingDone();
            $('#data-size').html( $('#buckets-table-total-size').html() );
            resizePage();
        }
    });
}

function listIndexesGET() {
  $.ajax({
    url: 'list_indexes',
    type: 'GET',
    success: function(html) {
      // Hack: create a div with the content, then extract what you need with a selector.
      var optionElements = $("<div>").append(html).find('select.indexes');
      $('select.indexes').html( optionElements.html() );
      logger.debug('got indexes: ' + optionElements.html());
    }
  });
}

function flushBucketsPOST() {

  if (!isFormValid()) return;

  var data = getPostArguments($('form'));
  logger.debug('flush buckets with post data: ' + data)
  
  $('#flushPage').show();
  resizePage();
  loading();
  $.ajax({
    url: 'flush',
    type: 'POST',
    data: data,
    success: function(html) {
      $('#flush-list').html(html);
      resizePage();
    },
    complete: function() {
      loadingDone();
      $('#flush-list').show();
      resizePage();
      listThawedPOST();
    },
    error: function(x, t, m) {
      if(t == "timeout") {
        $('#flush-list').html("Request submitted!");
      } else {
        $('#flush-list').html("Got unexpected error");
      }
    }
  });
}

function getAppName() {
  return $(top.document.body).attr("s:app") || 'UNKNOWN_APP_REALLY';
}

function isFormValid() {
  if($('#flush-buckets-form').valid()) {
    return true;
  } else {
    return false;
  }
}

function resizePage() {
  top.$(".IFrameInclude").trigger("resizeBody");
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

function loading() {
  var button = $('#flush-button');

  button.disable();
  if(button.hasClass('search')) { 
    $('#thaw-list').hide();
    $('#thaw-list').prev('.loadingBig').show();
  } else if (button.hasClass('flush')) {
    $('#flush-list').hide();
    $('#flush-list').prev('.loadingBig').show();
  } 
  resizePage();
}
function loadingDone() {
  var button = $('#flush-button');
  
  button.enable();
  $('.loadingBig').hide();
  $('#thaw-list').show();
}

function setSearchOrFlushButtonToFlush() {
  var button = $('#search-flush-button');
  button.addClass('flush');
  button.removeClass('search');
  button.val("Flush buckets!");
}
function setSearchOrFlushButtonToSearch() {
  var button = $('#search-flush-button');
  button.addClass('search');
  button.removeClass('flush');
  button.val("Search for buckets in the range");
  $('#flushPage').hide();
  resizePage();
}
function searchOrFlushBuckets(event) {
  var target = $(event.target);
  if (target.isEnabled()) {
    if (target.hasClass('search')) {
      listThawedPOST();
    } else if (target.hasClass('flush')) {
      flushBucketsPOST();
    }
  }
}
