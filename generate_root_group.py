import csv
import json
import re
import sys
import os


def extract_postal_code(address):
    """Extract 5-digit postal code from address"""
    if not address:
        return None

    postal_code_match = re.search(r'\b(\d{5})\b', address)
    return postal_code_match.group(1) if postal_code_match else None


def format_address(address):
    """Format address for unnormalized_address schema"""
    if not address:
        return None

    clean_address = re.sub(r'\b\d{5}(-\d{4})?\b', '', address).strip()
    clean_address = clean_address.rstrip(',').strip()

    parts = [part.strip() for part in clean_address.split(',') if part.strip()]

    if len(parts) >= 2:
        last_part = parts[-1]
        state_match = re.search(r'\b([A-Z]{2})\b', last_part)

        if state_match:
            state = state_match.group(1)
            city = last_part.replace(state_match.group(0), '').strip()

            if city:
                street_part = ', '.join(parts[:-1])
                return f"{street_part}, {city}, {state}"
            else:
                if len(parts) >= 2:
                    city = parts[-2]
                    street_part = ', '.join(parts[:-2]) if len(parts) > 2 else parts[0]
                    return f"{street_part}, {city}, {state}"

        city = parts[-1]
        street_part = ', '.join(parts[:-1])
        return f"{street_part}, {city}, FL"

    return f"{clean_address}, FL"


def ensure_directory(file_path):
    """Ensure the directory for the file exists"""
    directory = os.path.dirname(file_path)
    if directory and not os.path.exists(directory):
        os.makedirs(directory)
        print(f"Created directory: {directory}")


def is_empty_value(value):
    """Check if value is empty or None"""
    if value is None:
        return True
    if isinstance(value, str) and value.strip() == '':
        return True
    return False


def safe_int(value):
    """Safely convert value to int, return as string or None"""
    if is_empty_value(value):
        return None
    try:
        return str(int(float(value)))
    except (ValueError, TypeError):
        return str(value) if value else None


def create_parcel_folder(parcel_id, row):
    # Create folder name based on parcel_id
    folder_name = f"output/{parcel_id}"
    ensure_directory(folder_name + "/")

    # Extract data from row
    address = row.get('Address')
    http_request = row.get('http_request')
    county = row.get('County')
    source_identifier = row.get('source_identifier')

    # Create unnormalized_address.json
    unnormalized_address_data = {
        "full_address": format_address(address) if not is_empty_value(address) else None,
        "postal_code": extract_postal_code(address) if not is_empty_value(address) else None,
        "source_http_request": http_request if not is_empty_value(http_request) else None,
        "request_identifier": source_identifier if not is_empty_value(source_identifier) else None,
        "county_jurisdiction": county if not is_empty_value(county) else None
    }

    # Create property_root.json
    property_root_data = {
        "parcel_id": safe_int(parcel_id),
        "source_http_request": http_request if not is_empty_value(http_request) else None,
        "request_identifier": source_identifier if not is_empty_value(source_identifier) else None,
    }

    # Create relationship_property_to_address.json
    relationship_data = {
        "from": {
            "/": "./property_root.json"
        },
        "to": {
            "/": "./unnormalized_address.json"
        }
    }

    root_schema = {
        "label": "Root",
        "relationships": {
            "property_root": {
                "/": "./relationship_property_to_address.json"
            }
        }
    }

    # Write all JSON files
    files_to_create = [
        (f"{folder_name}/unnormalized_address.json", unnormalized_address_data),
        (f"{folder_name}/property_root.json", property_root_data),
        (f"{folder_name}/relationship_property_to_address.json", relationship_data),
        (f"{folder_name}/bafkreigpfi4pqur43wj3x2dwm43hnbtrxabgwsi3hobzbtqrs3iytohevu.json", root_schema)
    ]

    for filename, data_obj in files_to_create:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data_obj, f, indent=2, ensure_ascii=False)

    return folder_name, unnormalized_address_data, property_root_data


def process_csv(input_file):
    try:
        data = []
        with open(input_file, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                data.append(row)

        print(f"Found {len(data)} rows")

        created_folders = []
        sample_data = []

        for i, row in enumerate(data):
            parcel_id = row.get('parcel_id')

            if is_empty_value(parcel_id):
                print(f"Warning: Row {i + 1} has empty parcel_id, skipping...")
                continue

            clean_parcel_id = re.sub(r'[^\w\-_]', '_', str(parcel_id))

            folder_name, address_data, property_data = create_parcel_folder(clean_parcel_id, row)
            created_folders.append(folder_name)

            if len(sample_data) < 3:
                sample_data.append({
                    'parcel_id': parcel_id,
                    'folder': folder_name,
                    'address_data': address_data,
                    'property_data': property_data
                })

            if (i + 1) % 100 == 0:
                print(f"Processed {i + 1} parcels...")

        print(f"\n✅ Successfully created {len(created_folders)} parcel folders!")

        # Display sample output for verification
        print('\n' + '=' * 60)
        print('SAMPLE DATA VERIFICATION')
        print('=' * 60)

        for i, sample in enumerate(sample_data):
            print(f"\n--- Sample {i + 1}: Parcel ID {sample['parcel_id']} ---")
            print(f"Folder: {sample['folder']}")
            print(f"\nunnormalized_address.json:")
            print(json.dumps(sample['address_data'], indent=2))
            print(f"\nproperty_root.json:")
            print(json.dumps(sample['property_data'], indent=2))

        # Show address formatting examples
        print('\n' + '=' * 60)
        print('ADDRESS FORMATTING EXAMPLES')
        print('=' * 60)

        for i, row in enumerate(data[:5]):  # Show first 5 examples
            original = row.get('Address', '')
            formatted = format_address(original)
            postal = extract_postal_code(original)
            parcel_id = row.get('parcel_id', '')

            print(f"\n{i + 1}. Parcel ID: \"{parcel_id}\"")
            print(f"   Original Address: \"{original}\"")
            print(f"   Formatted Address: \"{formatted}\"")
            print(f"   Postal Code: \"{postal}\"")

        print(f"\n📁 All files created in individual parcel folders under 'output/' directory")
        print(f"📄 Each folder contains:")
        print(f"   - unnormalized_address.json")
        print(f"   - property_root.json")
        print(f"   - relationship_property_to_address.json")
        print(f"   - bafkreihiojomkt7q4cyuw6aupxbhjtfxjl3ohgxow7ihdc5eyqa4asipmq.json")

    except Exception as e:
        print(f'Error processing CSV: {e}')
        import traceback
        traceback.print_exc()


def main():
    """Main function"""
    input_file = sys.argv[1] if len(sys.argv) > 1 else 'seeding.csv'

    if not os.path.exists(input_file):
        print(f'Error: File {input_file} not found.')
        print(f'Usage: python {sys.argv[0]} [csv_file]')
        sys.exit(1)

    process_csv(input_file)


if __name__ == '__main__':
    main()