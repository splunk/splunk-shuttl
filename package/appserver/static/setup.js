var logger = Splunk.Logger.getLogger("Splunk.shuttl.setup");

$(document).ready(function() {
    bindHandler();

    //Show only app indexes by default.
    $('#option-show-internal-indexes').trigger('showHideInternalIndexes');
});
function bindHandler() {
    $('form').bind('submit', {index_id: 'conf-indexes', id: '<index-name>'}, formatFieldsHandler);

    $('.add-index').bind('click',function(event){addIndexHandler(event);});
    $('.remove-text').bind('click',function(event){removeTextHandler(event);});
    
    // Need to separate this from click to hide internal indexes on page load.
    $('#option-show-internal-indexes').bind('showHideInternalIndexes', {
            index_id: 'conf-indexes', 
            app: 'shuttl', 
            hide_indexes: ['conf-indexes.shuttl']
        }, showIndexesHandler);
    $('#option-show-internal-indexes').click(function(event){
        $(this).trigger('showHideInternalIndexes');
    });

    //Add foldout handlers
    $('.foldout').bind('click', function(event){foldoutHandler(event);});
}

function resize() {
    return top.$("body").trigger("resizeIFrames");
}

// Magic function that escapes stuff. I.e. when doing jQuery selects of id:s with dots in there names!
// TODO: Is there a better way to name id:s?
RegExp.quote = function(str) {
    return (str+'').replace(/([.?*+^$[\]\\(){}|-])/g, "\\$1");
}

function foldoutHandler(event) {
    var me = $(event.target)
    var target_id = me.attr('foldout_target');
    var target = $('body').find( '#'+RegExp.quote(target_id) );

    if (target.css('display') == "none") {
        me.html(me.attr('fold_text'));
    } else {
        me.html(me.attr('expand_text'));
    }

    target.toggle(300, function() {
        resize();
    });
}
function formatFieldsHandler(event) {
    var index_id = event.data.index_id;
    var id = event.data.id;

    // Find unnamed index name stanzas. Ex. 'conf-indexes.<index-name>'.
    var indexes = $('#'+index_id + ' .index.stanza[template]');
    logger.log(indexes);

    $.each(indexes, function(i, index) {

        var index_name = $(index).find('input.rename-field.name').attr('value');
        logger.debug('index_name='+index_name);
        
        // TODO: validate index name.
        if (index_name != '' || index_name != null) {
            index_name = '_new.' + index_name;
        } else {

            return false;
        }
        logger.debug('new index_name='+index_name);
        
        var inputs = $(index).find('input');
        logger.debug('inputs' + inputs);

        inputs.each( function(i) {
            var re = new RegExp(RegExp.quote(id), '');
            var new_name = $(this).attr('name').replace(re, index_name);
            $(this).attr('name', new_name);
        });

    });
    return true;
}

function showIndexesHandler(event) {
    // Uses 'event.data.index_id' to determine which indexes to hide
    // and 'event.data.app' to determine which to show.

    // Show all indexes.
    if ( $(event.target).attr('checked')!=null ) {
        $('#'+event.data.index_id + ' .index.stanza').show();
    // Show shuttl indexes.
    } else {
        $('#'+event.data.index_id + ' .index.stanza').hide();
        $('#'+event.data.index_id + ' .index.stanza[app='+event.data.app+']').show();
    }

    // Always hide some indexes!?
    $.each(event.data.hide_indexes, function(i, id) {
        $( '#'+RegExp.quote(id) ).hide();
    });

    resize();
}
function addIndexHandler(event) {
    // Find and clone shuttl index template (should be first node found).
    var index = $('.index.stanza[app=shuttl]').first().clone();
    
    // Show node and fields.
    index.show();
    index.find('.field.name').hide();
    index.find('.rename-field.name').show();

    // Update handlers.
    removeTextButton = index.find('.remove-text');
    removeTextButton.unbind().bind('click', removeTextHandler);
    
    // Expand and leave that way.
    index.find('.foldout').unbind().hide()
    
    // Add remove button
    b = document.createElement('button');
    $(b).attr('class', 'splButton-tertiary remove button')
     .html('Delete')
     .bind('click', {objToDelete: index}, removeHandler)
     .insertAfter(removeTextButton.first());
    
    // Add to .index.stanzas and resize.
    index.appendTo('.index.stanzas');
    resize();

    return false;
}
function removeTextHandler(event) {
    var field = $(event.target).prev('input[old_value]');
    var old_value = field.attr('old_value');
    field.attr('value', old_value);
    return false;
}
function removeHandler(event) {
    if (event.data.objToDelete) {
        event.data.objToDelete.remove();
    } else {
        var par  = $(event.target).parent();
        par.remove();
    }
    resize();
    return false;
}