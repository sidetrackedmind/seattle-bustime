var mymap = L.map('mapid').setView([47.60, -122.32], 13);

L.tileLayer('https://api.tiles.mapbox.com/v4/{id}/{z}/{x}/{y}.png?access_token={accessToken}', {
attribution: 'Map data &copy; <a href="http://openstreetmap.org">OpenStreetMap</a> contributors, <a href="http://creativecommons.org/licenses/by-sa/2.0/">CC-BY-SA</a>, Imagery © <a href="http://mapbox.com">Mapbox</a>',
maxZoom: 18,
id: 'mapbox.streets',
accessToken: 'pk.eyJ1IjoiYm1hbG5vciIsImEiOiJjamQ0Mmd4NmwwZHNyMzNyenZ5eGZ2YjFuIn0.OdTGjMcYBOji2YGX3hvmjA'
}).addTo(mymap);
