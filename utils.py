import os
import shutil

# Define the source and destination directories
source_dir = 'all_csv2'
destination_dir = 'data'

# Create the destination directory if it doesn't exist
if not os.path.exists(destination_dir):
    os.makedirs(destination_dir)

# Iterate over the files in the source directory
for filename in os.listdir(source_dir):
    # Check if the file is a CSV and does not contain '_info'
    if filename.endswith('.csv') and '_info' not in filename:
        # Construct full file path
        source_file = os.path.join(source_dir, filename)
        destination_file = os.path.join(destination_dir, filename)
        # Move the file
        shutil.move(source_file, destination_file)
        print(f'Moved: {filename}')