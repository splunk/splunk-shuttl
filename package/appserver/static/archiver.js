function loadXMLDoc()
{
  var xmlhttp;
  if (window.XMLHttpRequest)
  {// code for IE7+, Firefox, Chrome, Opera, Safari
    xmlhttp=new XMLHttpRequest();
  }
  else
  {// code for IE6, IE5
    xmlhttp=new ActiveXObject("Microsoft.XMLHTTP");
  }
  xmlhttp.onreadystatechange=function()
  {
    if (xmlhttp.readyState==4 && xmlhttp.status==200)
    {
      document.getElementById("bucket-table").innerHTML=xmlhttp.responseText;
    }
  }
  xmlhttp.open("GET","/custom/shep/Archiving/list_buckets",true);
  xmlhttp.send();
}


function listBucketsGET() {
  $.ajax({
    url: 'list_buckets',
    type: 'GET',
    success: function(html) {
      alert('loaded buckets');
      $('#bucket-table').html(html);
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
  
  $.ajax({
    url: 'list_buckets',
    type: 'POST',
    success: function(html) {
      alert('loaded buckets');
      $('#bucket-table').html(html);
    }
  });
}

$(document).ready(function() {
    bindHandler();
});

function bindHandler() {
    //$('.refresh').bind('click',function(event){alert('qwert!');});
    $('#enable-button').bind('click', function() { toggleSearchThawButton(); });
    $('#search-thaw-button').bind('click', function(event) { searchOrThawBuckets(event); });
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

function searchOrThawBuckets(event) {
  var target = $(event.target);
  if (target.hasClass('disabled')) {
    alert('do nothing...');
  } else {
    listBucketsGET();
  }
}
