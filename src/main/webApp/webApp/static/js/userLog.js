/**
 * Created by youzhenghong on 30/03/2017.
 */
/**
 * Created by youzhenghong on 30/03/2017.
 */
var log = $('#log');
var userLogPageCount = 1;

log.click(showUserLog);

function bindLogPaginationEvent() {
    $('#preLogPage').click(function () {
        if(userLogPageCount > 1) {
            $.getJSON('/userlog/data', {page:userLogPageCount - 1}).done(function (data) {
                if(!jQuery.isEmptyObject(data)) {
                    userLogPageCount -= 1;
                    buildLogTable();
                    $('#table').bootstrapTable({data:data});
                }
                if(userLogPageCount === 1) {
                    $('#preLogPage').addClass('disabled');
                }
                else {
                    $('#preLogPage').removeClass('disabled');
                }
            });
        }
});

    $('#nextLogPage').click(function () {

        $.getJSON('/userlog/data', {page:userLogPageCount + 1}).done(function (data) {
                if(!jQuery.isEmptyObject(data)) {
                    userLogPageCount += 1;
                    buildLogTable();
                    $('#table').bootstrapTable({data:data});
                    $('#preLogPage').removeClass('disabled');
                    if(data.length<10) {
                        $('#nextLogPage').addClass('disabled');
                    }
                }

        });
        $.getJSON('/userlog/data', {page:userLogPageCount + 1}).done(function (data) {
                if(jQuery.isEmptyObject(data)) {
                    $('#nextLogPage').addClass('disabled');
                }
        });
    });
}



function showUserLog() {
    if(userLogPageCount === 1) {
        $('#nextpage').removeClass('disabled');
        $('#prepage').addClass('disabled');
    }
    else {
        $('#prepage').removeClass('disabled');
    }


    buildLogTable();
    $.getJSON('/userlog/data',{page:userLogPageCount}).done(function (data) {
            //alert(JSON.stringify(data, null, ' '));
            $('#table').bootstrapTable({data:data});
    });
    $.getJSON('/userlog/data', {page:userLogPageCount + 1}).done(function (data) {
            if(jQuery.isEmptyObject(data)) {
                $('#nextpage').addClass('disabled');
            }
    });
}

function buildLogTable() {
    $('ul.nav.nav-pills.nav-stacked li.active').removeClass('active');
    log.addClass('active');
    var panel = $('#panel');
    panel.empty()
    panel.append('<table id="table"><thead><tr id="col"></tr></thead></table>');

    var tr = $('#col');
    tr.empty();
    tr.append('<th data-field="user_id">user_id</th>');
    tr.append('<th data-field="item_id">item_id</th>');
    tr.append('<th data-field="cat_id">cat_id</th>');
    tr.append('<th data-field="merchant_id">seller_id</th>');
    tr.append('<th data-field="brand_id">brand_id</th>');
    tr.append('<th data-field="time_tamp">time_stamp</th>');
    tr.append('<th data-field="action_type">action_type</th>');

    $('#pagination').empty();
    $('#pagination').append('<li class="previous disabled" id="preLogPage"><a href="#">&larr; Older</a></li>');
    $('#pagination').append('<li class="next" id="nextLogPage"><a href="#">Newer &rarr;</a></li>')

    bindLogPaginationEvent();
}