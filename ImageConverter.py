from pyspark.sql import SparkSession
import os
import io
from PIL import Image
from hdfs import InsecureClient

# Create a Spark session
spark = SparkSession.builder \
    .appName("Image Conversion to JPEG2000 (Cluster)") \
    .getOrCreate()

# Input HDFS path (read all files under RAW)
input_path = "hdfs://master:9000/RAW/*"
# Output HDFS directory (make sure this directory exists in HDFS or your user can create it)
output_dir = "/converted_images"

def convert_to_jpeg2000(image_data):
    """
    Convert an image (from bytes) to JPEG2000 format.
    Returns a tuple of (basename, new image bytes) or None on failure.
    """
    image_name, image_bytes = image_data
    try:
        image = Image.open(io.BytesIO(image_bytes))
        output_buffer = io.BytesIO()
        image.save(output_buffer, format="JPEG2000")
        return os.path.basename(image_name), output_buffer.getvalue()
    except Exception as e:
        print(f"Error processing {image_name}: {e}", flush=True)
        return None

# Read images from HDFS as binary files
images_rdd = spark.sparkContext.binaryFiles(input_path)

# Convert images to JPEG2000 and filter out any failures
converted_images_rdd = images_rdd.map(convert_to_jpeg2000).filter(lambda x: x is not None)

def save_to_hdfs(image_data):
    """
    Save the converted image (in JPEG2000 format) to HDFS.
    Each executor creates its own HDFS client.
    """
    # Adjust the URL and port as needed.
    # For newer Hadoop versions, the HTTP port is often 9870.
    client = InsecureClient('http://master:9870', user='hduser')
    
    image_name, image_bytes = image_data
    base_name, _ = os.path.splitext(image_name)
    hdfs_path = os.path.join(output_dir, f"{base_name}.jp2")
    
    try:
        # Write the file to HDFS (overwrite if exists)
        with client.write(hdfs_path, overwrite=True) as writer:
            writer.write(image_bytes)
        print(f"Saved: {hdfs_path}", flush=True)
    except Exception as e:
        print(f"Error saving {image_name} to HDFS: {e}", flush=True)

# Save each converted image to HDFS
converted_images_rdd.foreach(save_to_hdfs)

spark.stop()

