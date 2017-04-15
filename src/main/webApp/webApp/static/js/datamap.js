/*
var width  = 1000;
	var height = 1000;

	var svg = d3.select("#chinamap").append("svg")
	    .attr("width", width)
	    .attr("height", height)
	    .append("g")
	    .attr("transform", "translate(0,0)");

	var projection = d3.geo.mercator()
						.center([107, 31])
						.scale(850)
    					.translate([width/2, height/2]);

	var path = d3.geo.path()
					.projection(projection);


	var color = d3.scale.category20();

	d3.json("/map/chinageojson", function(error, root) {

		if (error)
			return console.error(error);
		console.log(root.features);

		svg.selectAll("path")
			.data( root.features )
			.enter()
			.append("path")
			.attr("stroke","#000")
			.attr("stroke-width",1)
			.attr("fill", function(d,i){
				return color(i);
			})
			.attr("d", path )
			.on("mouseover",function(d,i){
                d3.select(this)
                    .attr("fill","yellow");
            })
            .on("mouseout",function(d,i){
                d3.select(this)
                    .attr("fill",color(i));
            });

	});


*/
/*
var mapType = google.maps.MapTypeId.ROADMAP;
var lat = 39.915168, lng = 116.403875, zoom = 10;
var mapOptions = {
    center: new google.maps.LatLng(lat, lng),  //地图的中心点
    zoom: zoom,               　　　　　　　　　　//地图缩放比例
    mapTypeId: mapType,       　　　　　　　　　　//指定地图展示类型：卫星图像、普通道路
    scrollwheel: true          　　　　　　　　　 //是否允许滚轮滑动进行缩放
};
var map = new google.maps.Map(document.getElementById("chinamap"), mapOptions); //创建谷歌地图
*/

var map = new AMap.Map('chinamap',{
	resizeEnable: true,
	zoom: 5,
	center: [108.9398, 34.3416]
});


var marker = new AMap.Marker({
    position: [116.480983, 39.989628]
});
marker.setMap(map);