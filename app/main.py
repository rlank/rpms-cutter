import os
import uuid
import tempfile

from datetime import datetime
from dotenv import load_dotenv
from pyproj import CRS, Transformer
from shapely.geometry import Polygon

from flask import Flask, request, jsonify, render_template, redirect, url_for
from werkzeug.utils import secure_filename


from app.clipper import clip_raster_to_bounds, get_raster_bounds_crs
from app.gcs_utils import get_max_rpms_year_gcs, upload_to_gcs, generate_signed_url

load_dotenv()

# Initialize the Flask app
app = Flask(__name__)

# Load configuration from environment variables or use default values
app.config["UPLOAD_FOLDER"] = os.getenv("UPLOAD_FOLDER", "rpms-cutter")
app.config["GCS_BUCKET"] = os.getenv("GCS_BUCKET", "fuelcast-data")
app.config["MAX_CONTENT_LENGTH"] = int(
    os.getenv("MAX_CONTENT_LENGTH", 16 * 1024 * 1024)
)  # 16MB limit
app.config["PORT"] = int(os.getenv("PORT", 5000))
app.config["DEBUG"] = os.getenv("FLASK_DEBUG", "false").lower() == "true"

# Allowed file extensions for raster uploads
ALLOWED_EXTENSIONS = {"tif", "tiff"}


def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


@app.route('/')
def index():
    return render_template('index.html')


@app.route("/upload", methods=["GET", "POST"])
def upload_file():
    if request.method == "POST":
        # Check if the post request has the file part
        if "file" not in request.files:
            return jsonify({"error": "No file part"}), 400
        file = request.files["file"]
        if file.filename == "":
            return jsonify({"error": "No selected file"}), 400
        if file and allowed_file(file.filename):
            # Use tempfile to store file in a temporary location
            with tempfile.NamedTemporaryFile(suffix='.tif') as temp_file:
                file.save(temp_file.name)

                # Call function to get bounding box, CRS, and transform
                bounds, crs, transform = get_raster_bounds_crs(temp_file.name)

                return jsonify({
                    'message': 'File uploaded successfully',
                    'bounds': bounds,
                    'crs': crs,
                    'transform': transform
                })
        else:
            return (
                jsonify(
                    {
                        "error": "Invalid file format. Only TIF, TIFF allowed."
                    }
                ),
                400,
            )

    # GET request, return the file upload form (optional)
    return render_template("upload.html")


@app.route("/draw_polygon", methods=["GET", "POST"])
def draw_polygon():
    if request.method == "POST":
        # Get the polygon coordinates from the request
        polygon_coords = request.json.get("polygon_coords", None)
        if not polygon_coords:
            return jsonify({"error": "No polygon coordinates provided"}), 400

        # Process the polygon (clip the raster, etc.)

        # Assuming the Leaflet coordinates are in EPSG:4326 (WGS 84)
        leaflet_crs = 'EPSG:4326'  # Update this if Leaflet uses a different CRS like EPSG:3857

        # Create a polygon from the coordinates
        polygon = Polygon(polygon_coords[0])  # Assuming polygon_coords is a list of [ [x1, y1], [x2, y2], ... ]
        
        # Calculate the bounding box of the polygon
        bounds = polygon.bounds  # Returns (min_x, min_y, max_x, max_y)

        # CRS information (assuming EPSG:4326 for Leaflet, adjust if needed)
        crs = CRS.from_string(leaflet_crs).to_string()

        # For polygon, we return a null or dummy transform as polygons don’t have an affine transform
        transform = None

        return jsonify({
            'message': 'Polygon processed successfully',
            'bounds': {
                'min_x': bounds[0],
                'min_y': bounds[1],
                'max_x': bounds[2],
                'max_y': bounds[3]
            },
            'crs': crs,
            'transform': transform
        })

    # GET request, render a page with the map to draw polygons
    return render_template("draw_polygon.html")


@app.route('/result')
def result():
    # Retrieve the signed URLs from a session or pass it from the previous route
    download_urls = []  # List of signed URLs from clipping
    return render_template('result.html', download_urls=download_urls)


@app.route('/get_max_rpms_year', methods=['GET'])
def get_max_rpms_year():
    # Get the maximum year available in the RPMS data files
    max_year = get_max_rpms_year_gcs('fuelcast-public', 'rpms')
    
    if max_year:
        return jsonify({'max_year': max_year})
    else:
        return jsonify({'error': 'Could not find any RPMS files in the bucket'}), 404


@app.route('/clip_raster', methods=['POST'])
def clip_raster():
    # Get the metadata and year range from the request
    data = request.json
    bounds = data.get('bounds')
    crs = data.get('crs')
    transform = data.get('transform')
    start_year = data.get('startYear')
    end_year = data.get('endYear')

    # Generate a unique directory name for the upload (using UUID)
    unique_prefix = os.getenv('UPLOAD_FOLDER', 'rpms-cutter') + '/' + str(uuid.uuid4()) + '/'

    # Bucket name from environment variables
    bucket_name = os.getenv('GCS_BUCKET', 'fuelcast-data')

    # Prepare execution log
    execution_log = {
        'date_time': datetime.utcnow().isoformat(),
        'years_selected': list(range(start_year, end_year + 1)),
        'bounds': bounds,
    }

    # Loop over the selected years and generate clipped rasters
    uploaded_files = []
    for year in range(start_year, end_year + 1):
        with tempfile.NamedTemporaryFile(suffix=f'_rpms_{year}.tif') as temp_file:
            # Clip the raster for the given year (your clipping function here)
            clip_raster_to_bounds(bounds, crs, transform, year, temp_file.name)

            # Upload the clipped raster to GCS
            blob_name = unique_prefix + f'rpms_{year}.tif'
            upload_to_gcs(bucket_name, temp_file.name, blob_name)
            uploaded_files.append(blob_name)

    # Create a JSON log file for the execution
    log_blob_name = unique_prefix + 'execution_log.json'
    log_temp_file = tempfile.NamedTemporaryFile(suffix='.json', delete=False)
    with open(log_temp_file.name, 'w') as log_file:
        json.dump(execution_log, log_file)
    
    # Upload the log file to GCS
    upload_to_gcs(bucket_name, log_temp_file.name, log_blob_name)
    
    return jsonify({
        'message': 'Clipping complete',
        'download_url': [generate_signed_url(bucket_name, blob_name) for blob_name in uploaded_files],
        'log_url': generate_signed_url(bucket_name, log_blob_name)
    })

# Error handlers
@app.errorhandler(404)
def not_found_error(error):
    return jsonify({"error": "Resource not found"}), 404


@app.errorhandler(500)
def internal_error(error):
    return jsonify({"error": "An internal error occurred"}), 500


# Run the Flask app
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=app.config["PORT"], debug=app.config["DEBUG"])
