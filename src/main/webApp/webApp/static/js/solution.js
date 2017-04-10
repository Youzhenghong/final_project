/**
 * Created by youzhenghong on 30/03/2017.
 */
var solutions = $('#solution');
solutions.click(showSolutionPage);

function showSolutionPage() {
    $('ul.nav.nav-pills.nav-stacked li.active').removeClass('active');
    solutions.addClass('active');
}