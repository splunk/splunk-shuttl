
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

  if (!$('#search-thaw-buckets-form').valid()) {
    return;
  }

  var formData = $('form').serializeArray();
  var data = formData;
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

function getStaticContextURL() {
  var url = Splunk.util.make_url('static/images/calendar_blue.png'); //initialize string
  // var scripts = $('script').last().attr('src');
  // url = scripts.slice(7, scripts.lastIndexOf("/"));
  console.log(url);
  return url;
}

// Splunk.util.getCurrentApp() only checks document.body not top.document
function getAppName() {
  return $(top.document.body).attr("s:app") || 'UNKNOWN_APP_REALLY';
}

$(document).ready(function() {
    bindHandler();

    $('.datetime-input').datepicker({
      dateFormat:"yy-mm-dd",
      showOn: "button",
      buttonImage: Splunk.util.make_url('static/app/', getAppName() ,'/images/calendar_blue.png'),
      buttonImageOnly: true
     });

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
    //$('.datetime-input').change( function() {
    //  if ($('#search-thaw-buckets-form').valid()) {
    //    enableSearchThawButton();
    //  } else {
    //    disableSearchThawButton();
    //  }
    //});
});

function bindHandler() {
    //$('#enable-button').bind('click', function() { toggleSearchThawButton(); });
    $('#search-thaw-button').bind('click', function(event) { thawBuckets(event); });
}

function enableSearchThawButton() {
  $('#search-thaw-button').removeClass('disabled');
}

function disableSearchThawButton() {
  $('#search-thaw-button').addClass('disabled');
}

function toggleSearchThawButton() {
  $('#search-thaw-button').toggleClass('disabled');
}

function thawBuckets(event) {
  var target = $(event.target);

  if (!$('#search-thaw-buckets-form').valid()) {
    alert('Please fill out form first');
    return;
  }

  if (!target.hasClass('disabled')) {
    thawBucketsGET();
  }
}
