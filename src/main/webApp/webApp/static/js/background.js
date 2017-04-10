/**
 * Created by youzhenghong on 30/03/2017.
 */
/**
 * Created by youzhenghong on 30/03/2017.
 */
var backgrounds = $('#background');
backgrounds.click(showBackgroundPage);

function showBackgroundPage() {
    $('ul.nav.nav-pills.nav-stacked li.active').removeClass('active');
    backgrounds.addClass('active');
}