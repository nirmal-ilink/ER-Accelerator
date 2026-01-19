import csv
import random
import os
import uuid

# Configuration
# Using absolute path to ensure clarity on location
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../"))
OUTPUT_DIR = os.path.join(BASE_DIR, "data", "landing")
EMR_FILE = os.path.join(OUTPUT_DIR, "emr_data.csv")
REGISTRY_FILE = os.path.join(OUTPUT_DIR, "npi_registry.csv")

# Ensure output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

# --- Data Banks ---
FIRST_NAMES = ["James", "John", "Robert", "Michael", "William", "David", "Richard", "Joseph", "Thomas", "Charles", "Mary", "Patricia", "Jennifer", "Linda", "Elizabeth", "Barbara", "Susan", "Jessica", "Sarah", "Karen"]
LAST_NAMES = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin"]
CITIES = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose"]
STREETS = ["Main St", "Broadway", "Park Ave", "Oak Ln", "Maple Dr", "Cedar Ct", "Elm St", "Washington Blvd", "Lakeview Dr", "Hillside Ave"]
SPECIALTIES = ["Cardiology", "Dermatology", "Neurology", "Pediatrics", "Internal Medicine", "Family Practice", "Oncology", "Radiology", "Anesthesiology", "Psychiatry"]

def generate_npi():
    return str(random.randint(1000000000, 9999999999))

def generate_record(base_record=None, scenario="exact"):
    """
    Generates a record. If base_record is provided, modifies it based on the scenario.
    """
    if not base_record:
        # Create a brand new "Truth" record (Registry Style)
        return {
            "npi": generate_npi(),
            "first_name": random.choice(FIRST_NAMES),
            "last_name": random.choice(LAST_NAMES),
            "address_line_1": f"{random.randint(100, 9999)} {random.choice(STREETS)}",
            "city": random.choice(CITIES),
            "state": "US", 
            "zip_code": f"{random.randint(10000, 99999)}",
            "phone": f"{random.randint(200, 999)}-{random.randint(200, 999)}-{random.randint(1000, 9999)}",
            "specialty": random.choice(SPECIALTIES)
        }
    
    # Modify base record for EMR scenarios
    rec = base_record.copy()
    
    if scenario == "exact":
        # 1. Exact Match: 100% Identical
        return rec
        
    elif scenario == "fuzzy_name":
        # 2. Typo in Name (e.g., "Smyth" instead of "Smith")
        if len(rec["last_name"]) > 3:
            rec["last_name"] = rec["last_name"][:-1] + "x" # Simple mutation
        return rec
        
    elif scenario == "nickname":
        # 3. Nickname (William -> Bill, just simulating by forcing a change)
        if rec["first_name"] == "William": rec["first_name"] = "Bill"
        elif rec["first_name"] == "Robert": rec["first_name"] = "Bob"
        else: rec["first_name"] = rec["first_name"] + "y" # Generic nickname logic
        return rec
        
    elif scenario == "address_variation":
        # 4. Address Formatting (St vs Street)
        rec["address_line_1"] = rec["address_line_1"].replace("St", "Street").replace("Dr", "Drive").replace("Ave", "Avenue")
        return rec
        
    elif scenario == "missing_npi":
        # 5. Missing Identifier
        rec["npi"] = ""
        return rec
        
    elif scenario == "outdated_info":
        # 6. Moved to a new city
        rec["city"] = "MovedCity"
        rec["zip_code"] = "00000"
        return rec
        
    return rec

def generate_datasets(num_records=1000):
    registry_data = []
    emr_data = []
    
    print(f"Generating {num_records} records with specific ER scenarios...")
    
    for _ in range(num_records):
        # 1. Create the 'Gold' Registry Record
        truth = generate_record()
        registry_data.append(truth)
        
        # 2. Determine if this record exists in EMR (Overlap Rate)
        if random.random() < 0.8: # 80% overlap
            scenario = random.choices(
                ["exact", "fuzzy_name", "nickname", "address_variation", "missing_npi", "outdated_info"],
                weights=[0.4, 0.15, 0.10, 0.15, 0.10, 0.10]
            )[0]
            
            emr_rec = generate_record(truth, scenario)
            emr_data.append(emr_rec)
            
    # Add some noise to EMR (records not in Registry)
    for _ in range(int(num_records * 0.2)):
        noise = generate_record()
        # Ensure NPI is distinct or missing to simulate messy new intake
        if random.random() < 0.5: noise["npi"] = "" 
        emr_data.append(noise)
        
    # Write files
    fieldnames = ["npi", "first_name", "last_name", "address_line_1", "city", "state", "zip_code", "phone", "specialty"]
    
    with open(REGISTRY_FILE, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(registry_data)
        
    with open(EMR_FILE, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(emr_data)
        
    print(f"Values generated at: {OUTPUT_DIR}")
    print(f"Registry: {REGISTRY_FILE}")
    print(f"EMR Data: {EMR_FILE}")

if __name__ == "__main__":
    # Generate 100 records for quick testing/upload
    generate_datasets(100)
