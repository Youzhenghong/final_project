/**
 * Created by youzhenghong on 30/03/2017.
 */
/**
 * Created by youzhenghong on 30/03/2017.
 */
var profile = $('#profile');
profile.click(showProfile);

var userProfilePageCount = 1;
function bindProfilePaginationEvent() {
    $('#preProfilePage').click(function () {
    if(userProfilePageCount > 1) {
        $.getJSON('/userprofile/data', {page:userProfilePageCount - 1}).done(function (data) {
            if(!jQuery.isEmptyObject(data)) {
                userProfilePageCount -= 1;
                buildProfileTable();
                $('#table').bootstrapTable({data:data});
            }
            if(userProfilePageCount===1) {
                $('#preProfilePage').addClass('disabled');
            }
            else{
                $('#preProfilePage').removeClass('disabled');
            }

        });
    }
    });


    $('#nextProfilePage').click(function () {


        $.getJSON('/userprofile/data', {page:userProfilePageCount + 1}).done(function (data) {
                if(!jQuery.isEmptyObject(data)) {
                    userProfilePageCount += 1;
                    buildProfileTable();
                    $('#table').bootstrapTable({data:data});
                    $('#preProfilePage').removeClass('disabled');
                    if(data.length < 10) {
                       $('#nextProfilePage').addClass('disabled');
                    }
                }

        });
        $.getJSON('/userprofile/data', {page:userProfilePageCount + 1}).done(function (data) {
                if(jQuery.isEmptyObject(data) || data.length < 10) {
                    $('#nextProfilePage').addClass('disabled');
                }
        });
    });
}


function showProfile() {
    if(userProfilePageCount===1) {
        $('#nextProfilePage').removeClass('disabled');
        $('#preProfilePage').addClass('disabled');
    }
    else{
        $('#preProfilePage').removeClass('disabled');
    }

    buildProfileTable();
    $.getJSON('/userprofile/data', {page:userProfilePageCount}).done(function (data) {
            $('#table').bootstrapTable({data:data});
    });
    $.getJSON('/userprofile/data', {page:userProfilePageCount + 1}).done(function (data) {
            if(jQuery.isEmptyObject(data)) {
                $('#nextProfilePage').addClass('disabled');
            }
    });
}


function buildProfileTable() {

    $('ul.nav.nav-pills.nav-stacked li.active').removeClass('active');
    profile.addClass('active');
    var panel=$('#panel');
    panel.empty();
    $('#panel').append('<table id="table"><thead><tr id="col"></tr></thead></table>');

    var tr = $('#col');
    tr.empty();
    tr.append('<th data-field="user_id">user_id</th>');
    tr.append('<th data-field="age_range">age_range</th>');
    tr.append('<th data-field="gender">gender</th>');

    $('#pagination').empty();
    $('#pagination').append('<li class="previous disabled" id="preProfilePage"><a href="#">&larr; Older</a></li>');
    $('#pagination').append('<li class="next" id="nextProfilePage"><a href="#">Newer &rarr;</a></li>')
    bindProfilePaginationEvent();
}