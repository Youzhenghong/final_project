/**
 * Created by youzhenghong on 30/03/2017.
 */
/**
 * Created by youzhenghong on 30/03/2017.
 */
var trainingSet = $('#training');
trainingSet.click(showTrainingSet);

function showTrainingSet() {
    $('ul.nav.nav-pills.nav-stacked li.active').removeClass('active');
    trainingSet.addClass('active');
}