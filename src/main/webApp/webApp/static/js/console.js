/**
 * Created by youzhenghong on 15/04/2017.
 */

$(function() {
    $('#term_demo').terminal(function(command, term) {
        term.pause();

        $.ajax({
                type: 'POST',
                url: '/sparktask',
                success: function(data, status, request) {
                    var status_url = request.getResponseHeader('Location');
                    var task_id = request.getResponseHeader('task_id');
                    alert(status_url);
                    update_progress(status_url, term);
                },
                error: function() {
                    alert('Unexpected error');
                }
            });


    },{
        greetings: 'Machine Learning Console',
        prompt: 'my-shell >>',
        height: 200
    });
});

function update_progress(status_url, term) {
    $.getJSON(status_url, function (data) {
        if ('info' in data) {
            if (data['info'].length != 0) {
                var info = data['info'].split('\n');
                info.forEach(function (i) {
                    if(i.length!=0){
                        term.echo(i);
                    }
                })
            }
        }

        if(data['task_state'] == 'SUCCESS'){
            term.resume();
        }
        else {
            setTimeout(function () {
                update_progress(status_url, term);
            }, 200);
        }
    });
}

