
import csv
import requests
import time
from collections import defaultdict

# Configuration
BATCH_SIZE = 2000
DELAY_BETWEEN_BATCHES = 300
DELAY_BETWEEN_REQUESTS = 1

def fetch_fuzzy_score(project_name, city, retries=3, delay=2):
    """Fetch fuzzy match score from API."""
    url = "http://sanity7.infoedge.com/99ongroundapi/suggest-project"
    headers = {"Content-Type": "application/json"}
    payload = {"term": f"{project_name.strip()},{city.strip()}"}

    for attempt in range(retries):
        try:
            response = requests.post(url, json=payload, headers=headers)
            response.raise_for_status()
            data = response.json()
            
            if isinstance(data, dict) and "data" in data:
                suggestions = data["data"].get("suggest", [])
                if suggestions:
                    full_matched_name = suggestions[0].get("NAME", "N/A")
                    score = suggestions[0].get("SCORE", "Not Matched")
                    print(f"  → API Response: {full_matched_name} with score {score}")
                    return score, full_matched_name.split(",")[0].strip(), full_matched_name
        except requests.exceptions.RequestException:
            print(f"   API Request Failed. Retrying ({attempt+1}/{retries})...")
            time.sleep(delay)
    return "Not Matched", "N/A", "N/A"

def calculate_match_percentage(original_words, matched_words):
    """Calculate match percentage based on word overlap."""
    if not original_words:
        return 0.0
    matched_count = len(original_words & matched_words)
    return round((matched_count / len(original_words)) * 100, 2)

def advanced_word_matching(original_name, original_city, full_matched_name):
    """Perform partial matching and calculate match percentage."""
    matched_parts = full_matched_name.split(',')
    matched_project_name = matched_parts[0].strip()
    matched_city = matched_parts[-1].strip()

    def clean_words(name):
        ignore_words = {'THE', 'A', 'AN', 'B', ',', '`', ''}
        return set(word.upper() for word in name.split() if word.upper() not in ignore_words)

    original_words = clean_words(original_name)
    
    matched_words = clean_words(matched_project_name)
    
    city_match = original_city.upper() == matched_city.upper()
    match_percentage = calculate_match_percentage(original_words, matched_words)

    print(f"  → Word Matching Debug: {original_words} vs {matched_words} -> {match_percentage}% match")
    return match_percentage, city_match

def process_csv(input_file, output_file, not_matched_file, sus_file):
    with open(input_file, mode='r', encoding='utf-8') as infile, \
         open(output_file, mode='w', newline='', encoding='utf-8') as outfile, \
         open(not_matched_file, mode='w', newline='', encoding='utf-8') as not_matched_outfile, \
         open(sus_file, mode='w', newline='', encoding='utf-8') as sus_outfile:

        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        not_matched_writer = csv.writer(not_matched_outfile)
        sus_writer = csv.writer(sus_outfile)

        # Write headers
        header = next(reader, None)
        if not header or len(header) < 3:
            print("Error: Input file is empty or missing required columns!")
            return

        writer.writerow(["City", "Project Name", "Bank Name", "Score", "Matched Project Name", "Matched Score"])
        not_matched_writer.writerow(["City", "Project Name", "Bank Name", "Score", "Matched Project Name", "Matched Score"])
        sus_writer.writerow(["City", "Project Name", "Bank Name", "Score", "Matched Project Name", "Matched Score"])

        city_idx, project_name_idx, bank_name_idx = header.index("City"), header.index("Project Name"), header.index("Bank Name")
        batch_count = 0

        for i, row in enumerate(reader, start=1):
            if not row or len(row) < 3:
                continue

            city, project_name, bank_name = row[city_idx].strip().upper(), row[project_name_idx].strip().upper(), row[bank_name_idx].strip()
            
            
            if "UNIQUE ID" in project_name:
                project_name = project_name.split("UNIQUE ID")[0].strip()

            print(f"\n  Processing: {project_name} ({city}) - {bank_name}")

            score, core_matched_name, full_matched_name = fetch_fuzzy_score(project_name, city)
            score_numeric = float(score) if score.replace('.', '', 1).isdigit() else 0
            
            match_percentage, city_match = advanced_word_matching(project_name, city, full_matched_name)
            print(f"  → Final Match Percentage: {match_percentage}% | City Match: {city_match}")

            row_data = [city, project_name, bank_name, score_numeric, full_matched_name, match_percentage]

           
            if score_numeric > 70:
                writer.writerow(row_data)
                outfile.flush()
            elif 21 <= score_numeric <= 70:  # Suspicious range
                sus_writer.writerow(row_data)
                sus_outfile.flush()
            else:  # Score <= 20
                not_matched_writer.writerow(row_data)
                not_matched_outfile.flush()

            time.sleep(DELAY_BETWEEN_REQUESTS)

            batch_count += 1
            if batch_count >= BATCH_SIZE:
                print(f"\n Processed {i} projects. Pausing for {DELAY_BETWEEN_BATCHES} seconds...\n")
                time.sleep(DELAY_BETWEEN_BATCHES)
                batch_count = 0

    print(" Processing complete. Check output files for results.")

def main():
    input_csv = "input.csv"
    output_csv = "output_scores.csv"
    not_matched_csv = "not_matched.csv"
    sus_csv = "sus.csv"
    process_csv(input_csv, output_csv, not_matched_csv, sus_csv)





if __name__ == "__main__":
    main()
























