let map = L.map('map').setView([37.7749, -122.4194], 6);  // Set initial view
L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    attribution: 'Map data © OpenStreetMap contributors'
}).addTo(map);

let drawnPolygon = null;
let drawnLayer = new L.FeatureGroup().addTo(map);

let drawControl = new L.Control.Draw({
    draw: {
        polygon: true,
        polyline: false,
        rectangle: false,
        circle: false,
        marker: false,
    },
    edit: {
        featureGroup: drawnLayer,
        remove: true
    }
}).addTo(map);

map.on(L.Draw.Event.CREATED, function (e) {
    if (drawnPolygon) {
        drawnLayer.clearLayers();  // Clear previous polygons
    }
    drawnPolygon = e.layer;
    drawnLayer.addLayer(drawnPolygon);
});

document.getElementById('clearPolygon').addEventListener('click', function () {
    drawnLayer.clearLayers();
    drawnPolygon = null;
});

document.getElementById('confirmPolygon').addEventListener('click', function () {
    if (drawnPolygon) {
        let polygonCoords = drawnPolygon.toGeoJSON().geometry.coordinates;
        // Print polygon coordinates to the browser console
        console.log("Polygon Coordinates: ", polygonCoords);
        fetch('/draw_polygon', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ polygon_coords: polygonCoords })
        }).then(response => response.json())
          .then(data => window.location.href = '/result');
    } else {
        alert('Please draw a polygon first!');
    }
});
