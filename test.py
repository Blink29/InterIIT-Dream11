import pandas as pd
import os
import shutil

def check_and_move_csv():
    # Create problems folder if it doesn't exist
    if not os.path.exists('problems_v3_probs'):
        os.makedirs('problems_v3_probs')
    
    run_out_count = 0
    
    # Process each CSV in data folder
    for filename in os.listdir('data'):
        if filename.endswith('.csv'):
            file_path = os.path.join('data', filename)
            df = pd.read_csv(file_path)
            
            # Check for run out rows
            run_out_rows = df[df['wicket_type'] == 'run out']
            
            if not run_out_rows.empty:
                run_out_count += 1
                # Check if player1 column exists and has values
                if ('player1' not in df.columns) or (run_out_rows['player1'].isna().any()):
                    # Move file to problems folder
                    shutil.move(file_path, os.path.join('problems_v3_probs', filename))
                    print(f"Moved {filename} to problems folder - missing or empty player1 data")
    
    print(f"\nTotal files with run out dismissals: {run_out_count}")

if __name__ == "__main__":
    check_and_move_csv()