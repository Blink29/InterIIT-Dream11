import pandas as pd
import math
import requests
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from espncricinfo.match import Match

logging.basicConfig(filename='error_log.txt', level=logging.ERROR, format='%(asctime)s - %(message)s')

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
    # Collect rows with "run out" wickets
    run_out_rows = df[df["wicket_type"] == "run out"]

    # Process rows with distinct overs
    for index, row in run_out_rows.iterrows():
        ball_value = (row["ball"])

        print(ball_value)
        # Check if the ball ends with .6 (e.g., 3.6)
        if abs(ball_value % 1 - 0.6) < 1e-6:
            # Get the original over (before .6), and the next over (rounded up)
            print(f"Processing Row Index {index}: {row.to_dict()}")
            over_value = math.floor(ball_value)  # Example: 3.6 -> 3
            next_over_value = over_value + 1  # Next over (4 for 3.6)
            possible_over_matches = {ball_value, next_over_value, float(next_over_value)}
            print(f"Possible Over Matches for {ball_value}: {possible_over_matches}")
        else:
            possible_over_matches = {ball_value}

        # Compare with run_out_data for distinct overs
        matching_run_outs = [
            ro for ro in run_out_data
            if (ro["over"]) in possible_over_matches
        ]

        print(matching_run_outs)

        if matching_run_outs:
            # If there's a match, assign the fielder(s) to player columns
            fielders = matching_run_outs[0]["fielders"]

            # Add new columns dynamically for the number of fielders
            for i, fielder_name in enumerate(fielders):
                player_column = f"player{i+1}"  # Create column names like player1, player2, etc.
                df.at[index, player_column] = fielder_name

            print(f"Updated Row Index {index}: {row.to_dict()} with fielders {fielders}")

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
    data_folder = 'data'
    csv_files = [os.path.join(data_folder, f) for f in os.listdir(data_folder) 
                 if f.endswith('.csv')]
    
    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(process_single_csv, csv_files)