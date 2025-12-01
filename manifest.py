import os
import json
import csv

output_csv = './data/output_dict.csv'
fieldnames = None 
rows = []

data_folder = "./data"
for file in os.listdir(data_folder):
    if file in ('.DS_Store', 'output_dict.csv'):
        continue
    channel_folder = os.path.join(data_folder, file) # each youtube channel e.g mkhbd,ksi
    raw_dir = os.path.join(channel_folder, "raw")

    # skip channels without a raw folder yet
    if not os.path.isdir(raw_dir):
        continue

    file_list = [
                f for f in os.listdir(raw_dir)
                if os.path.isfile(os.path.join(raw_dir, f))
                    and f.endswith(".json")
                    and f != ".DS_Store"
            ]
    

    for i in file_list:
        with open(os.path.join(raw_dir, i),"r", encoding="utf-8") as f:
            data = json.load(f)
        
        if fieldnames is None:
            fieldnames = list(data.keys())
            # drop description since i don't want it in the CSV
            if "description" in fieldnames:
                fieldnames.remove("description")

        

        row = {k: v for k, v in data.items() if k in fieldnames}
        rows.append(row)

# Actually write the CSV
if fieldnames is None:
    print("No JSON files found. Nothing to write.")
else:
    with open(output_csv, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote {len(rows)} rows to {output_csv}")


    




