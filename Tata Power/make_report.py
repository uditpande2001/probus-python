import os
import csv

# Path to the folder containing JSON files
folder_path = "D:\python\Tata Power\midnight_data"

# Create a list to store the file names
file_names = []

# Iterate over the files in the folder
for filename in os.listdir(folder_path):
    if filename.endswith(".json"):  # Check if the file is a JSON file
        file_names.append(filename)

# Path to save the CSV file
csv_file_path = "D:\python\Tata Power\midnight_result.csv"

# Write the file names to the CSV file
with open(csv_file_path, "w", newline="") as csv_file:
    writer = csv.writer(csv_file)
    writer.writerow(["File Name"])  # Write the header
    writer.writerows([[file_name] for file_name in file_names])  # Write the file names
