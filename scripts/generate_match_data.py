import pandas as pd
import random
import os
import sys

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

def generate_match_data(num_clusters=15):
    # Setup paths
    base_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(base_dir, ".."))
    output_dir = os.path.join(project_root, "data", "output")
    output_path = os.path.join(output_dir, "gold_export.csv")
    
    os.makedirs(output_dir, exist_ok=True)
    
    # Base profiles to generate clusters from
    base_people = [
        {"first": "Jennifer", "last": "Martin", "city": "Los Angeles", "state": "CA", "zip": "25354", "spec": "Oncology"},
        {"first": "Sarah", "last": "Wilson", "city": "San Antonio", "state": "TX", "zip": "74966", "spec": "Internal Medicine"},
        {"first": "Nirmal", "last": "Pukazhen", "city": "Houston", "state": "TX", "zip": "77001", "spec": "General Practice"},
        {"first": "Robert", "last": "Chen", "city": "Seattle", "state": "WA", "zip": "98101", "spec": "Cardiology"},
        {"first": "Michael", "last": "Rodriguez", "city": "Miami", "state": "FL", "zip": "33101", "spec": "Pediatrics"},
        {"first": "Samantha", "last": "Smith", "city": "Chicago", "state": "IL", "zip": "60601", "spec": "Family Medicine"},
        {"first": "James", "last": "Johnson", "city": "New York", "state": "NY", "zip": "10001", "spec": "Neurology"},
        {"first": "Emily", "last": "Davis", "city": "Boston", "state": "MA", "zip": "02108", "spec": "Dermatology"},
        {"first": "William", "last": "Brown", "city": "Denver", "state": "CO", "zip": "80202", "spec": "Orthopedics"},
        {"first": "Elizabeth", "last": "Miller", "city": "Phoenix", "state": "AZ", "zip": "85001", "spec": "Psychiatry"},
        {"first": "David", "last": "Garcia", "city": "San Diego", "state": "CA", "zip": "92101", "spec": "Surgery"},
        {"first": "Maria", "last": "Martinez", "city": "Dallas", "state": "TX", "zip": "75201", "spec": "Anesthesiology"},
        {"first": "Richard", "last": "Hernandez", "city": "Austin", "state": "TX", "zip": "73301", "spec": "Radiology"},
        {"first": "Linda", "last": "Lopez", "city": "San Jose", "state": "CA", "zip": "95101", "spec": "Emergency Medicine"},
        {"first": "Joseph", "last": "Gonzalez", "city": "San Francisco", "state": "CA", "zip": "94102", "spec": "Pathology"}
    ]
    
    sources = ["EMR", "NPI_Registry", "Claims_DB", "Snowflake", "Delta Lake", "SAP"]
    
    records = []
    
    # Generate clusters
    for i in range(num_clusters):
        cluster_id = f"CL{i+1:03d}"
        person = base_people[i % len(base_people)]
        
        # Decide how many records for this person (2 to 5)
        num_records_for_person = random.randint(2, 5)
        
        # Base NPI (sometimes varying slightly to simulate error, but mostly constant)
        base_npi = str(random.randint(1000000000, 9999999999))
        
        # Base Phone
        base_phone = f"{random.randint(200, 999)}-555-{random.randint(1000, 9999)}"
        
        used_sources = random.sample(sources, num_records_for_person)
        
        for src in used_sources:
            # Introduce variations
            
            # Name variation
            first_name = person["first"]
            if random.random() < 0.2:
                 # Nicknames or typos
                 if first_name == "Robert": first_name = "Rob"
                 elif first_name == "Michael": first_name = "Mike"
                 elif first_name == "Elizabeth": first_name = "Liz"
                 elif first_name == "William": first_name = "Bill"
                 elif first_name == "Jennifer": first_name = "Jen"
            
            last_name = person["last"]
            if random.random() < 0.1:
                 # Typos
                 last_name += "s" if random.random() < 0.5 else ""
            
            # Address variation
            addr = f"{random.randint(100, 9999)} {random.choice(['Main', 'Oak', 'Pine', 'Cedar', 'Maple'])} {random.choice(['St', 'Ave', 'Rd', 'Blvd', 'Lane'])}"
            if random.random() < 0.3:
                 # Same street, different format maybe? (Simplified here as just random addr to show conflict)
                 # Or keep it same for match
                 pass 
            
            # Values with potential None
            phone = base_phone if random.random() > 0.1 else None
            
            # Confidence score
            conf = round(random.uniform(0.70, 0.99), 2)
            
            rec = {
                "unique_id": f"{src[:3].upper()}{random.randint(100, 999)}",
                "npi": base_npi if random.random() > 0.1 else str(int(base_npi) + 1), # Occasional typo
                "first_name": first_name,
                "last_name": last_name,
                "address_line_1": addr,
                "city": person["city"],
                "state": person["state"],
                "zip_code": person["zip"],
                "phone": phone,
                "specialty": person["spec"] if random.random() > 0.1 else "General Practice", # Occasional mismatch
                "_source_system": src,
                "cluster_id": cluster_id,
                "confidence_score": conf
            }
            records.append(rec)
            
    # Create DataFrame
    df = pd.DataFrame(records)
    
    # Save
    df.to_csv(output_path, index=False)
    print(f"Generated {len(df)} records in {output_path}")

if __name__ == "__main__":
    generate_match_data(num_clusters=15) # Should generate around 40-60 records
