function route_names (feature, layer) {
    layer.bindPopup(feature.properties.group);
}

function routeFilter (feature) {
    if (feature.properties.group == selected_shape ) return true
}

var selected_routes = L.geoJson(all_route_lines,
    {filter: routeFilter}).addTo(mymap);

mymap.fitBounds(selected_routes.getBounds(), {
        //padding: [50,50]
});


function stopFilter (feature) {
    if (feature.properties.stop_id == selected_stop_shape ) return true
}

var selected_stop = L.geoJson(KCM_stop_shapes,
    {filter: stopFilter}).addTo(mymap);

mymap.fitBounds(selected_stop.getBounds(), {
        padding: [10,10]
});
