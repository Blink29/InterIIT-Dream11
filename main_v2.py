import pandas as pd
import math
import requests
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from espncricinfo.match import Match

logging.basicConfig(filename='error_log_v4.txt', level=logging.ERROR, format='%(asctime)s - %(message)s')

def get_match_data(match_id):
    m = Match(str(match_id)) 
    url = m.espn_api_url
    print(url)
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        error_message = f"Error getting match data for {match_id}.csv: {response.status_code} - {response.text}"
        logging.error(error_message)
        return None 


def search_run_outs(data, keyword="run out"):
    results = []

    def traverse(node, current_outDetails=None):
        if isinstance(node, dict):
            if "outDetails" in node:
                current_outDetails = node["outDetails"]

            for key, value in node.items():
                if key == "shortText" and keyword in value and current_outDetails:
                    details = current_outDetails.get("details", {})
                    fielders = current_outDetails.get("fielders", [])
                    over = details.get("over", {}).get("overs", None)
                    fielder_names = [
                        fielder.get("athlete", {}).get("displayName", "Unknown")
                        for fielder in fielders
                    ]
                    bowler_batsman = details.get("shortText", "")
                    results.append({
                        "shortText": value,
                        "fielders": fielder_names,  # Store all fielders' names
                        "over": over,
                        "bowler_batsman": bowler_batsman
                    })
                traverse(value, current_outDetails)
        elif isinstance(node, list):
            for item in node:
                traverse(item, current_outDetails)

    traverse(data)
    return results


def process_run_out_rows(df, run_out_data):
    # For each run out entry in the data
    for run_out in run_out_data:
        # Extract the whole number part of the over
        over_number = int(float(run_out["over"]))
        
        # Find all rows with this ball number
        ball_rows = df[df["ball"].astype(str).str.startswith(f"{over_number}.")]
        
        # Filter for run out rows among these ball rows
        run_out_rows = ball_rows[ball_rows["wicket_type"] == "run out"]
        
        # If exactly one run out found for this ball number
        if len(run_out_rows) == 1:
            # Get the index of the single run out row
            index = run_out_rows.index[0]
            
            # Get fielders from run out data
            fielders = run_out["fielders"]
            
            # Add fielder names to player columns
            for i, fielder_name in enumerate(fielders):
                player_column = f"player{i+1}"
                df.at[index, player_column] = fielder_name
                
            print(f"Updated Row Index {index} for over {over_number}: {run_out_rows.iloc[0].to_dict()} with fielders {fielders}")
        else:
            print(f"Skipping over {over_number}: Found {len(run_out_rows)} run outs")
            
    return df


def process_single_csv(csv_file):
    try:
        match_id = csv_file.split('\\')[-1].replace(".csv", "")
        print(f"Processing {csv_file}...")
        
        # Read the CSV
        df = pd.read_csv(csv_file)

        # Check for "run out" rows
        run_out_rows = df[df["wicket_type"] == "run out"]
        if run_out_rows.empty:
            print(f"No 'run out' entries found in {csv_file}")
            return
            
        # Fetch match data and search for run outs
        match_data = get_match_data(match_id)
        if match_data:
            run_out_data = search_run_outs(match_data)
            
            # Process the DataFrame and update player columns
            updated_df = process_run_out_rows(df, run_out_data)
            
            # Update the original CSV file
            updated_df.to_csv(csv_file, index=False)
            print(f"Processed and updated: {csv_file}")
    except Exception as e:
        logging.error(f"Error processing {csv_file}: {str(e)}")

if __name__ == "__main__":
    data_folder = 'problems_v2_probs'
    csv_files = [os.path.join(data_folder, f) for f in os.listdir(data_folder) 
                 if f.endswith('.csv')]
    
    with ThreadPoolExecutor(max_workers=8) as executor:
        executor.map(process_single_csv, csv_files)