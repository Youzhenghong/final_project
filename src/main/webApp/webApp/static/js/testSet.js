/**
 * Created by youzhenghong on 30/03/2017.
 */
/**
 * Created by youzhenghong on 30/03/2017.
 */
var testSets = $('#test');
testSets.click(showTestSet);

function showTestSet() {
    $('ul.nav.nav-pills.nav-stacked li.active').removeClass('active');
    testSets.addClass('active');
}