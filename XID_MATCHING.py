
import pandas as pd
import re

def clean_project_name(matched_project_name):
    parts = [part.strip() for part in matched_project_name.split(',')]
    project_name = parts[0]
    city = parts[-1] if len(parts) > 1 else ""
    subcity = ", ".join(parts[1:-1]) if len(parts) > 2 else ""
    return project_name, subcity, city

def normalize_text(text):
    if not isinstance(text, str):
        return ""
    text = re.sub(r'[^\w\s]', ' ', text.lower())
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def exact_match_location(location_to_match, location_fields):
    if not location_to_match:
        return False
    norm_location = normalize_text(location_to_match)
    cleaned_locations = [normalize_text(loc) for loc in location_fields if loc]
    for loc in cleaned_locations:
        if norm_location == loc:
            print(f"   Exact Location match found: '{location_to_match}' matched with '{loc}'")
            return True
    print(f"   No exact location match for '{location_to_match}'")
    return False

def match_projects_exact(output_scores_path, sus_path, list1_path):
    print("Loading data files...")
    output_scores_df = pd.read_csv(output_scores_path)
    sus_df = pd.read_csv(sus_path)
    list1_df = pd.read_csv(list1_path)

    for df in [output_scores_df, sus_df, list1_df]:
        df.columns = df.columns.str.strip()
        df.fillna("", inplace=True)

    sus_df = sus_df[sus_df['Matched Score'] >= 50]

    mapped_data = []
    not_mapped_data_output = []
    not_mapped_data_sus = []

    list1_project_names = list1_df['Project Name'].tolist()
    list1_project_dict = {name.lower(): name for name in list1_project_names}

    print(f"Loaded {len(list1_project_names)} projects from List1.csv")

    def find_matching_project(row, source):
        matched_project_name = row['Matched Project Name']
        project_name, subcity, city = clean_project_name(matched_project_name)

        print(f"\n-----------------------------------------------------")
        print(f"Processing: '{matched_project_name}' from {source}")
        print(f"Extracted: Project='{project_name}', Subcity='{subcity}', City='{city}'")

        exact_matches = list1_df[list1_df['Project Name'].str.lower() == project_name.lower()]

        if exact_matches.empty:
            print(f" No exact project name match found for '{project_name}'")
            return pd.DataFrame(), matched_project_name, row

        print(f" Exact project name match found!")
        print(f"Found {len(exact_matches)} potential project matches. Checking locations...")

        matched_rows = []

        for _, match in exact_matches.iterrows():
            print(f"\nChecking location match for '{match['Project Name']}':")

            location_fields = [
                match.get('Location Locality', ""),
                match.get('Location City', ""),
                match.get('Location State', "")
            ]

            city_match = exact_match_location(city, location_fields)
            subcity_match = False

            if subcity and not city_match:
                subcity_match = exact_match_location(subcity, location_fields)

            if city_match or subcity_match:
                print(f" MATCH CONFIRMED for '{match['Project Name']}'")
                matched_rows.append(match)

        return pd.DataFrame(matched_rows), matched_project_name, row

    print("\n\n==== Processing output_scores.csv ====")
    for _, row in output_scores_df.iterrows():
        matches, matched_project_name, orig_row = find_matching_project(row, 'output_scores')

        if not matches.empty:
            for _, match in matches.iterrows():
                mapped_data.append({
                    'Source': 'output_scores',
                    'bank_project name': row['Matched Project Name'],
                    'bank_location': row['City'],
                    'Matched Project Name': matched_project_name,
                    'Score': row['Score'],
                    'XID': match['XID'],
                    'Project Name': match['Project Name'],
                    'City': match.get('Location City', ""),  # <- updated field
                    'Location Locality': match['Location Locality'],
                    'Location City': match['Location City'],
                    'Location State': match['Location State'],
                    'Bank Name': row['Bank Name'],
                })
        else:
            not_mapped_data_output.append(orig_row.to_dict())

    print("\n\n==== Processing sus.csv ====")
    for _, row in sus_df.iterrows():
        matches, matched_project_name, orig_row = find_matching_project(row, 'sus')

        if not matches.empty:
            for _, match in matches.iterrows():
                mapped_data.append({
                    'Source': 'sus',
                    'bank_project name': row['Project Name'],
                    'bank_location': row['City'],
                    'Matched Project Name': matched_project_name,
                    'Score': row['Score'],
                    'XID': match['XID'],
                    'Project Name': match['Project Name'],
                    'City': match.get('Location City', ""),  # <- updated field
                    'Location Locality': match['Location Locality'],
                    'Location City': match['Location City'],
                    'Location State': match['Location State'],
                    'Bank Name': row['Bank Name'],
                })
        else:
            not_mapped_data_sus.append(orig_row.to_dict())

    print("\n\n==== Saving Results ====")
    df = pd.DataFrame(mapped_data)
    df.to_csv('mapped.csv', index=False)
    pd.DataFrame(not_mapped_data_output).to_csv('not_mapped_output_scores.csv', index=False)
    pd.DataFrame(not_mapped_data_sus).to_csv('not_mapped_sus.csv', index=False)

    print(f"\nSummary:")
    print(f" Mapped records: {len(mapped_data)}")
    print(f" Unmapped records from output_scores: {len(not_mapped_data_output)}")
    print(f" Unmapped records from sus: {len(not_mapped_data_sus)}")
    print(f"Total records processed: {len(output_scores_df) + len(sus_df)}")

if __name__ == "__main__":
    match_projects_exact('output_scores.csv', 'sus.csv', 'List1.csv')
