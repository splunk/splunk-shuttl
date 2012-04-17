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
      document.getElementById("bucket_table").innerHTML=xmlhttp.responseText;
    }
  }
  xmlhttp.open("POST","/custom/shep/Archiving/list_buckets",true);
  xmlhttp.send();
}


$(document).ready(function() {
    alert('ALERT A LE Rt');
    bindHandler();
});

function bindHandler() {
    $('.refresh').bind('click',function(event){alert('qwert!')});
}
