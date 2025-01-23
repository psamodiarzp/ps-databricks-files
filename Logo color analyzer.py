# Databricks notebook source
import numpy as np
from PIL import Image
import scipy.cluster
import matplotlib.pyplot as plt
from collections import Counter
from sklearn.cluster import DBSCAN

# COMMAND ----------



# COMMAND ----------

!pip install Pillow colorthief

# COMMAND ----------

from colorthief import ColorThief

# COMMAND ----------

# MAGIC %md
# MAGIC #Helper Functions

# COMMAND ----------

def rgb_to_hex(r, g, b):
  """Converts RGB values to a hex color code.

  Args:
    r: Red value (0-255)
    g: Green value (0-255)
    b: Blue value (0-255)

  Returns:
    Hex color code as a string.
  """

  return f"#{r:02x}{g:02x}{b:02x}"

# COMMAND ----------

def display_colors(hex_colors):
  """Displays given hex colors as color patches.

  Args:
    hex_colors: A list of hex color codes.
  """

  # Convert hex colors to RGB for Matplotlib
  rgb_colors = [(int(hex_color[1:3], 16), int(hex_color[3:5], 16), int(hex_color[5:7], 16)) for hex_color in hex_colors]

  # Create color patches
  fig, ax = plt.subplots()
  for i, color in enumerate(rgb_colors):
      ax.add_patch(plt.Rectangle((i, 0), 1, 1, color=color))

  # Remove axes and ticks
  ax.axis('off')
  plt.show()

# COMMAND ----------

def print_image_size(image_path):
  """Prints the width and height of an image.

  Args:
    image_path: Path to the image file.
  """

  img = Image.open(image_path)
  width, height = img.size
  print("Width:", width)
  print("Height:", height)

# COMMAND ----------

# MAGIC %md
# MAGIC # Using K-means Clustering: 
# MAGIC ##In consideraton but not perfect

# COMMAND ----------



def get_dominant_colors(image_path, num_clusters=5):
  """
  Extracts dominant colors from an image using K-Means clustering.

  Args:
    image_path: Path to the image file.
    num_clusters: Number of dominant colors to find.

  Returns:
    A list of dominant colors in RGB format.
  """

  # Load image and resize for efficiency
  img = Image.open(image_path).resize((150, 150))
  ar = np.asarray(img)
  shape = ar.shape
  ar = ar.reshape(scipy.product(shape[:2]), shape[2]).astype(float)

  # Perform K-Means clustering
  codes, dist = scipy.cluster.vq.kmeans(ar, num_clusters)

  # Get the dominant colors
  vecs, dist = scipy.cluster.vq.vq(ar, codes)
  counts, bins = np.histogram(vecs, len(codes))
  index_max = np.argmax(counts)  # Index of the dominant color
  index_second_max = np.argsort(counts)[-2]  # Index of the second dominant color
  index_third_max = np.argsort(counts)[-3]  # Index of the third dominant color in case one is a background

  primary_color = codes[index_max]
  secondary_color = codes[index_second_max]
  tertiary_color = codes[index_third_max]

  primary_color = rgb_to_hex(int(codes[index_max][0]), int(codes[index_max][1]), int(codes[index_max][2]))
  secondary_color = rgb_to_hex(int(codes[index_second_max][0]), int(codes[index_second_max][1]), int(codes[index_second_max][2]))
  tertiary_color = rgb_to_hex(int(codes[index_third_max][0]), int(codes[index_third_max][1]), int(codes[index_third_max][2]))



  return primary_color, secondary_color, tertiary_color



# COMMAND ----------

# Example usage:
image_path = '/Workspace/Users/pallavi.samodia@razorpay.com/FIRE-BOLTT-logo.jpg'
primary, secondary, tertiary_color = get_dominant_colors(image_path, num_clusters=3)
print("Primary color:", primary)
print("Secondary color:", secondary)
print("tertiary_color color:", tertiary_color)
#print("Third color:", tertiary_color)

# COMMAND ----------

# MAGIC %md
# MAGIC #Using just counting mechanism : 
# MAGIC ##Rejected

# COMMAND ----------

def count_colors(image_path):
  """Counts the occurrences of each color in an image.

  Args:
    image_path: Path to the image file.

  Returns:
    A list of tuples, where each tuple contains a color (RGB) and its count.
  """

  img = Image.open(image_path).convert('RGB')
  img_array = np.array(img)
  color_counts = Counter(tuple(pixel) for row in img_array for pixel in row)

  top_three_rgb = [color for color, _ in color_counts.most_common(3)]
  top_three_hex = [rgb_to_hex(*color) for color in top_three_rgb]
  return top_three_hex



# COMMAND ----------

image_path = '/Workspace/Users/pallavi.samodia@razorpay.com/FIRE-BOLTT-logo.jpg'
top_three_colors = count_colors(image_path)
print("Top three dominant colors:", top_three_colors)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #Using DBSCAN

# COMMAND ----------

def get_dominant_colors_dbscan_hex(image_path, eps=0.2, min_samples=2):
  """Extracts top 5 dominant colors from an image using DBSCAN and converts to hex.

  Args:
    image_path: Path to the image file.
    eps: The maximum distance between two samples for one to be considered as in the neighborhood of the other.
    min_samples: The number of samples (or total weight) in a neighborhood for a point to be considered as a core point.   


  Returns:
    A list of the top 5 dominant colors in hex format.
  """

  img = Image.open(image_path).convert('RGB')
  img = img.resize((150,150))
  img_array = np.array(img)
  X_raw = img_array.reshape(-1, 3)
  print("Image shape:", X_raw.shape)
  X = X_raw/255

  # Apply DBSCAN
  db = DBSCAN(eps=eps, min_samples=min_samples).fit(X)
  labels = db.labels_

  # Get the dominant colors
  unique_labels = set(labels)
  dominant_colors_rgb = []
  for label in unique_labels:
    if label != -1:  # Ignore noise points
      cluster_colors = X[labels == label]
      mean_color = np.mean(cluster_colors, axis=0)
      dominant_colors_rgb.append(mean_color)
  print(unique_labels)

  # Sort by cluster size
  dominant_colors_rgb = sorted(dominant_colors_rgb, key=lambda x: np.sum(labels == db.labels_[np.argmin(np.linalg.norm(X - x, axis=1))]), reverse=True)

  # Convert to hex
  #dominant_colors_hex = [rgb_to_hex(*color) for color in dominant_colors_rgb[:5]]

  return dominant_colors_rgb

# COMMAND ----------

image_path = '/Workspace/Users/pallavi.samodia@razorpay.com/FIRE-BOLTT-logo.jpg'
dominant_colors_hex = get_dominant_colors_dbscan_hex(image_path)
print("Top 5 dominant colors in hex:", dominant_colors_hex)

# COMMAND ----------

[0.95626475*255, 0.95496627*255, 0.95413351*255]

# COMMAND ----------

image_path = '/Workspace/Users/pallavi.samodia@razorpay.com/FIRE-BOLTT-logo.jpg'
print_image_size(image_path)

# COMMAND ----------

image_path = '/Workspace/Users/pallavi.samodia@razorpay.com/FIRE-BOLTT-logo.jpg'
img = Image.open(image_path).convert('RGB')
img_array = np.array(img)
X_raw = img_array.reshape(-1, 3)
print("Image shape:", X_raw.shape)
X = X_raw/255.0


# COMMAND ----------

# MAGIC %md
# MAGIC # Using Colorthief
# MAGIC ## Most successful one so far
# MAGIC

# COMMAND ----------

def get_color_palette(img):
  color_thief = ColorThief(img)
  colors=color_thief.get_palette(color_count=5)
  hex_colors = [rgb_to_hex(*color) for color in colors]
  return hex_colors

# COMMAND ----------

#Fireboltt
image_path = '/Workspace/Users/pallavi.samodia@razorpay.com/FIRE-BOLTT-logo.jpg'
promenient_colors = get_color_palette(image_path)
print(promenient_colors)


# COMMAND ----------


#Sleepyhead
image_path = '/Workspace/Users/pallavi.samodia@razorpay.com/sleepyheadlogo.png'
promenient_colors = get_color_palette(image_path)
print(promenient_colors)

# COMMAND ----------

# MAGIC %md
# MAGIC #Web Scraping

# COMMAND ----------

import requests
from bs4 import BeautifulSoup
import colorthief
import re

def extract_dominant_colors(url):
    """Extracts the five most dominant colors from a webpage."""

    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for error HTTP statuses
        soup = BeautifulSoup(response.content, 'html.parser')

        # Extract colors from style attributes
        colors = []
        for element in soup(['style', 'span', 'div', 'p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6']):
            style_attr = element.get('style')
            if style_attr:
                for match in re.findall(r'(?:color|background-color|.*color.*):.*?;', style_attr, re.IGNORECASE):
                    color_value = match.strip().split(':')[1].strip()
                    colors.append(color_value)
        print(colors)
        # Apply ColorThief to find dominant colors
        dominant_colors = colorthief.ColorThief(f"{url}.png").get_palette(color_count=5)  # Placeholder for image-based extraction

        return dominant_colors

    except requests.exceptions.RequestException as e:
        print(f"Error fetching webpage: {e}")
        return []
    except Exception as e:
        print(f"Error processing colors: {e}")
        return []

if __name__ == "__main__":
    url = "https://www.fireboltt.com/"  # Replace with your target URL
    dominant_colors = extract_dominant_colors(url)
    print("Five dominant colors:", dominant_colors)

# COMMAND ----------


