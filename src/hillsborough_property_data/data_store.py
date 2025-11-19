import csv

def save_to_csv(data, filename="property_data.csv"):
    """
    Saves a list of dictionaries to a CSV file.
    """
    if not data:
        print("No data to save.")
        return

    keys = data[0].keys()
    with open(filename, "w", newline="") as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(data)
    print(f"Data saved to {filename}")
