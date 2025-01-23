# Databricks notebook source
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D

# COMMAND ----------

import numpy as np 

# COMMAND ----------

from plotly.graph_objects  import Figure, Scatter3d

# COMMAND ----------

base_db = sqlContext.sql(
    """
    select brand_color, count(*) as count from realtime_hudi_api.merchants
    where brand_color is not null
group by 1
    """
)
base_table = base_db.toPandas()
base_table.head()

# COMMAND ----------

base_table.sort_values(by='count',ascending=False)

# COMMAND ----------

base_calc= base_table.groupby(by='count').agg(['count']).reset_index()
base_calc


# COMMAND ----------

base_table['brand_color']=='RK'

# COMMAND ----------

# Replace with your actual data
color_codes = base_table['brand_color'] 
counts = base_table['count']

# COMMAND ----------

# Sample data (replace with your table)
colors = base_table['brand_color']  # Replace with actual color codes
counts = base_table['count']
base_table = base_table[base_table['count']==1]
# Convert color codes to RGB format for Matplotlib
rgb_colors = []
red=[]
green=[]
blue=[]
for color in colors:
    if len(color) == 6:  # Ensure color code has 6 characters
        try:
            hex_color = '#' + color
            rgb_colors.append([int(hex_color[1:3], 16)/255.0, int(hex_color[3:5], 16)/255.0, int(hex_color[5:7], 16)/255.0])
            red.append(int(hex_color[1:3], 16)/255.0)
            green.append(int(hex_color[3:5], 16)/255.0)
            blue.append(int(hex_color[5:7], 16)/255.0)
        except ValueError:
            continue

# Verify if the lengths of 'counts' and 'rgb_colors' are equal
if len(counts) != len(rgb_colors):
    # Handle the inconsistency by either truncating 'counts' or 'rgb_colors'
    min_length = min(len(counts), len(rgb_colors))
    counts = counts[:min_length]
    rgb_colors = rgb_colors[:min_length]



# COMMAND ----------

len(rgb_colors)

# COMMAND ----------

fig = plt.figure()
ax = fig.add_subplot(111, projection='3d')

# Plot the scatter points
ax.scatter3D(red, green, blue)

# Set axis labels and title
ax.set_xlabel('Red')
ax.set_ylabel('Green')
ax.set_zlabel('Blue')
ax.set_title('3D Scatter Plot')

plt.show()

# COMMAND ----------

# Create a scatter plot object
scatter_plot = Scatter3d(x=red, y=green, z=blue)

# Set plot title 
#scatter_plot.update_layout(title='3D Scatter Plot')
# Create a Figure object
fig = Figure()

# Add the Scatter3d trace to the figure
fig.add_trace(scatter_plot)

# Display the plot using the Figure object
fig.show()

# COMMAND ----------

plt.hist(base_table['count'])

# Optional customizations (refer to Matplotlib documentation for more)
plt.xlabel("X-axis Label")
plt.ylabel("Frequency")
plt.title("Histogram of Data")
plt.grid(True)  # Add grid lines

# Display the plot
plt.show()

# COMMAND ----------


