#!/usr/bin/env python3
import os
import json
import re
import shutil
import pandas as pd
from bs4 import BeautifulSoup
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm
from urllib.parse import urljoin
import multiprocessing
import argparse
import logging
import traceback
from functools import lru_cache

# Configuration
DEFAULT_INPUT_FOLDER = 'test_770'
DEFAULT_OUTPUT_FOLDER = 'test_770/'
DEFAULT_CSV_PATH = '../data/test.csv'
FAILED_FOLDER = "failed_data"

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("data_extraction.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def extract_property_details(soup):
    data = {}
    box = soup.find("div", id="PropertyDetailsCurrent")
    if not box:
        return data

    section = box.find("div", class_="innerBox")
    if not section:
        return data

    # Find all tables in this section
    tables = section.find_all("table", class_="appraisalAttributes")

    for table in tables:
        rows = table.find_all("tr")
        if not rows:
            continue

        # Identify the section from the first row's single <th> with colspan
        first_row_ths = rows[0].find_all("th")
        if len(first_row_ths) == 1:
            header_text = first_row_ths[0].get_text(strip=True).lower()

            if "land tracts" in header_text and len(rows) >= 3:
                headers = [th.get_text(strip=True) for th in rows[1].find_all("th")]
                values = [td.get_text(strip=True) for td in rows[2].find_all("td")]
                data["Land Tracts"] = dict(zip(headers, values))

            elif "land features" in header_text and len(rows) > 2:
                features = []
                for row in rows[2:]:
                    tds = row.find_all("td")
                    if len(tds) == 3:
                        features.append({
                            "Description": tds[0].get_text(strip=True),
                            "Year Added": tds[1].get_text(strip=True),
                            "Units": tds[2].get_text(strip=True)
                        })
                if features:
                    data["Land Features"] = features

    return data


def extract_unit_subareas_and_photos(unit_table, base_url="https://www.leepa.org"):
    rows = unit_table.find_all("tr")
    unit_subareas = []
    photos = {"Building Photo": None, "Unit Footprint": []}

    # Find the starting index of "Unit Subareas"
    subarea_start_idx = None
    for i, row in enumerate(rows):
        if "Unit Subareas" in row.get_text():
            subarea_start_idx = i
            break

    if subarea_start_idx is not None:
        for k in range(subarea_start_idx + 2, len(rows)):
            td_cols = rows[k].find_all("td")

            # If there's one TD with nested images, skip it and break
            if len(td_cols) == 1 and rows[k].find("img"):
                # Process images
                for img in rows[k].find_all("img"):
                    src = img.get("src")
                    if "photo.aspx" in src:
                        full_url = urljoin(base_url, src)
                        photos["Building Front Photo"] = full_url

                        # Try to get the date from the div nearby
                        photo_wrapper = img.find_parent("div")
                        if photo_wrapper:
                            # Search through all subsequent <div>s to find the "Photo Date"
                            for div in photo_wrapper.find_all_next("div"):
                                text = div.get_text(separator="\n", strip=True)
                                for line in text.split("\n"):
                                    if "Photo Date" in line:
                                        photos["Building Photo Date"] = line.strip()
                                        break

                    elif "FloorPlan" in src:
                        photos["Unit Footprint"].append(urljoin(base_url, src))
                break
            if len(td_cols) >= 3:
                unit_subareas.append({
                    "Description": td_cols[0].get_text(strip=True),
                    "Heated / Under Air": td_cols[1].get_text(strip=True),
                    "Area (Sq Ft)": td_cols[2].get_text(strip=True)
                })

    return unit_subareas, photos


def extract_alternate_address_info(section):
    data = {}
    # Try to find a table inside this section if available
    table = section.find("table", class_="detailsTable")
    if table:
        rows = table.find_all("tr")
        for row in rows:
            tds = row.find_all("td")
            if len(tds) >= 1:
                data["Alternate Address"] = " ".join(td.get_text(strip=True) for td in tds)
    else:
        # Fallback: just extract visible text if no table
        raw_text = section.get_text(" ", strip=True)
        if raw_text:
            data["Alternate Address"] = raw_text
    return data


def extract_buildings_info(section, base_url):
    buildings = []
    current_building = None
    current_section = None

    tables = section.find_all("table", class_="appraisalAttributes")

    for table in tables:
        rows = table.find_all("tr")
        if not rows:
            continue

        for i, row in enumerate(rows):
            ths = row.find_all("th")
            tds = row.find_all("td")

            # Check if row is a subheader
            if len(ths) == 1 and "subheader" in ths[0].get("class", []):
                header_text = ths[0].get_text(strip=True).lower()

                if "building" in header_text and "of" in header_text:
                    if current_building:
                        buildings.append(current_building)
                    current_building = {
                        "Building Subareas": [],
                        "Building Features": [],
                        "Photos and Footprint": {},
                        "Building Characteristics": {}
                    }
                    current_section = None
                    continue

                elif "building characteristics" in header_text:
                    current_section = "characteristics"
                    continue

                elif "building subareas" in header_text:
                    current_section = "subareas"
                    continue

                elif "building features" in header_text:
                    current_section = "features"
                    continue

                else:
                    current_section = None
                    continue

            if not current_building:
                continue  # skip until a building is initiated
                # Attach photos to last building (if any)
            if current_building and row.find("img"):
                for img in row.find_all("img"):
                    src = img.get("src")
                    if not src:
                        continue
                    full_url = urljoin(base_url, src)

                    if "photo.aspx" in src:
                        current_building["Photos and Footprint"]["Building Front Photo"] = full_url
                        date_div = img.find_parent("div")
                        if date_div:
                            sibling_div = date_div.find_next_sibling("div")
                            if sibling_div:
                                date_text = sibling_div.get_text(strip=True)
                                if "Photo Date" in date_text:
                                    current_building["Photos and Footprint"]["Building Photo Date"] = date_text

                    elif "FloorPlan" in src:
                        current_building["Photos and Footprint"].setdefault("Building Foot Print", []).append(
                            full_url)

            # Parse sections strictly after identifying subheaders
            if current_section == "characteristics" and len(tds) >= 2:
                values = [td.get_text(strip=True) for td in tds]
                if not current_building["Building Characteristics"].get("Improvement Type") and len(values) >= 4:
                    current_building["Building Characteristics"].update({
                        "Improvement Type": values[0],
                        "Model Type": values[1],
                        "Stories": values[2],
                        "Living Units": values[3]
                    })
                elif len(values) >= 4:
                    current_building["Building Characteristics"].update({
                        "Bedrooms": values[0],
                        "Bathrooms": values[1],
                        "Year Built": values[2],
                        "Effective Year Built": values[3]
                    })

            elif current_section == "subareas" and len(tds) >= 3:
                values = [td.get_text(strip=True) for td in tds]
                entry = {
                    "Description": values[0],
                    "Heated / Under Air": values[-2] if len(values) > 2 else "",
                    "Area (Sq Ft)": values[-1] if len(values) > 1 else ""
                }
                current_building["Building Subareas"].append(entry)

            elif current_section == "features" and len(tds) >= 3:
                values = [td.get_text(strip=True) for td in tds]
                entry = {
                    "Description": values[0],
                    "Year Added": values[-2] if len(values) > 2 else "",
                    "Units": values[-1] if len(values) > 1 else ""
                }
                current_building["Building Features"].append(entry)

    # Save the last parsed building
    if current_building:
        buildings.append(current_building)

    return buildings


def extract_condo_info(section, base_url="https://www.leepa.org"):
    data = {}

    # --- Complex Info ---
    table = section.find("table", class_="detailsTableLeft")
    if table:
        rows = table.find_all("tr")
        for row in rows:
            headers = row.find_all("th")
            values = row.find_all("td")
            for h, v in zip(headers, values):
                data[h.get_text(strip=True)] = v.get_text(strip=True)

    # --- Amenities ---
    amenities = section.find("div", class_="items")
    if amenities:
        amenity_list = [item.get_text(strip=True) for item in amenities.find_all("span")]
        if amenity_list:
            data["Amenities"] = amenity_list

    # --- Unit Detail + Unit Subareas ---
    tables = section.find_all("table", class_="detailsTableLeft")
    if len(tables) > 1:
        unit_table = tables[1]
        rows = unit_table.find_all("tr")
        current_section = None
        unit_detail = {}
        unit_subareas = []
        photos = {
            "Building Front Photo": None,
            "Unit Footprint": [],
            "Building Photo Date": None
        }

        for row in rows:
            ths = row.find_all("th")
            tds = row.find_all("td")

            # Switch section based on subheaders
            if ths and "subheader" in ths[0].get("class", []):
                header_text = ths[0].get_text(strip=True).lower()
                if "unit detail" in header_text:
                    current_section = "unit_detail"
                elif "unit subareas" in header_text:
                    current_section = "unit_subareas"
                else:
                    current_section = None
                continue

            # --- Unit Detail parsing ---
            if current_section == "unit_detail" and len(tds) == 2 and len(ths) == 2:
                key1 = ths[0].get_text(strip=True)
                val1 = tds[0].get_text(strip=True)
                key2 = ths[1].get_text(strip=True)
                val2 = tds[1].get_text(strip=True)
                unit_detail[key1] = val1
                unit_detail[key2] = val2

            # --- Unit Subareas parsing ---
            elif current_section == "unit_subareas" and len(tds) >= 3:
                unit_subareas.append({
                    "Description": tds[0].get_text(strip=True),
                    "Heated / Under Air": tds[-2].get_text(strip=True),
                    "Area (Sq Ft)": tds[-1].get_text(strip=True)
                })

        # Save parsed info
        if unit_detail:
            data["Unit Detail"] = unit_detail
        if unit_subareas:
            data["Unit Subareas"] = unit_subareas

        # --- Image section ---
        img_section = unit_table.find("div", class_="condo-flex-container")
        if img_section:
            for img in img_section.find_all("img"):
                src = img.get("src")
                if not src:
                    continue
                full_url = urljoin(base_url, src)
                if "photo.aspx" in src:
                    photos["Building Front Photo"] = full_url
                    date_div = img.find_parent("div")
                    if date_div:
                        for div in date_div.find_all_next("div"):
                            text = div.get_text(separator="\n", strip=True)
                            for line in text.split("\n"):
                                if "Photo Date" in line:
                                    photos["Building Photo Date"] = line.strip()
                                    break
                elif "FloorPlan" in src:
                    photos["Unit Footprint"].append(full_url)

            if photos["Building Front Photo"] or photos["Unit Footprint"]:
                data["Photos and Footprint"] = photos

    return data


def extract_links_from_cell(cell, base_url="https://www.leepa.org"):
    """Extract visible <a> tags or malformed ones with text outside, return as list of full URLs."""
    links = []
    for a in cell.find_all("a"):
        href = a.get("href")
        if not href:
            continue

        # Try to get text from inside <a>
        text = a.get_text(strip=True)

        # If no text inside <a>, try to read next sibling
        if not text and a.next_sibling:
            sibling_text = a.next_sibling.strip()
            if sibling_text:
                text = sibling_text

        if not text:
            continue  # Skip links with no visible text

        links.append(urljoin(base_url, href))

    return links


def extract_flood_and_storm_info(soup):
    data = {}
    flood_box = soup.find("div", id="ElevationDetails")
    if not flood_box:
        return data

    table = flood_box.find("table", class_="detailsTable")
    if not table:
        return data

    rows = table.find_all("tr")
    if len(rows) < 3:
        return data

    # Extract Flood Insurance link from first row
    flood_link_tag = rows[0].find("a", href=True)
    if flood_link_tag:
        data["Flood Insurance Link"] = flood_link_tag["href"]

    # Third row has values
    values = [td.get_text(strip=True) for td in rows[2].find_all("td")]

    # Align headers to values
    expected_labels = ["Community", "Panel", "Version", "Date", "Evacuation Zone"]
    for i, label in enumerate(expected_labels):
        if i < len(values):
            data[label] = values[i]

    return data


def extract_real_property_tag_info(soup):
    rp_div = soup.find("div", id="RPDetails")
    if not rp_div:
        return []

    tag_entries = []
    tag_tables = rp_div.find_all("table", class_="appraisalAttributes")
    for table in tag_tables:
        rows = table.find_all("tr")
        if len(rows) < 4:
            continue
        headers1 = [th.get_text(strip=True) for th in rows[1].find_all("th")]
        values1 = [td.get_text(strip=True) for td in rows[2].find_all("td")]

        headers2 = [th.get_text(strip=True) for th in rows[3].find_all("th")]
        values2 = [td.get_text(strip=True) for td in rows[4].find_all("td")]

        entry = {}
        for h, v in zip(headers1, values1):
            if h:
                entry[h.replace("DCA/HUD", "DCA / HUD")] = v
        for h, v in zip(headers2, values2):
            if h:
                entry[h] = v

        if entry:
            tag_entries.append(entry)

    return tag_entries


def extract_garbage_details(soup):
    data = {}
    garbage_div = soup.find("div", id="GarbageDetails")
    if garbage_div:
        table = garbage_div.find("table", class_="detailsTable")
        if table:
            rows = table.find_all("tr")
            if rows:
                # Try to extract headers and values from first two rows if available
                if len(rows) >= 2:
                    headers = [th.get_text(strip=True) for th in rows[0].find_all("th")]
                    values = [td.get_text(strip=True) for td in rows[1].find_all("td")]
                    for h, v in zip(headers, values):
                        data[h] = v

                # Look for row with "Collection Days"
                for row in rows:
                    if "Collection Days" in row.get_text():
                        tds = row.find_all("td")
                        if len(tds) >= 3:
                            data["Collection Days - Garbage"] = tds[0].get_text(strip=True)
                            data["Collection Days - Recycling"] = tds[1].get_text(strip=True)
                            data["Collection Days - Horticulture"] = tds[2].get_text(strip=True)
                        break  # done once we find it
    return data


def extract_property_attributes(soup):
    data = {}
    description_panel = None
    for subtitle in soup.find_all("div", class_="sectionSubTitle"):
        if "Property Description" in subtitle.get_text():
            description_panel = subtitle.find_next_sibling("div", class_="textPanel")
            break

    if description_panel:
        description_text = re.sub(r'\s+', ' ', description_panel.get_text(separator=" ", strip=True))
        data["Property Description"] = description_text

    attributes_table = soup.find("table", class_="appraisalDetails")
    if attributes_table:
        for row in attributes_table.find_all("tr"):
            cells = row.find_all("td")
            if not cells:
                continue
            header = row.find("th")
            if header:
                label = header.get_text(strip=True)
                value = cells[0].get_text(strip=True)
                data[label] = value

    location_table = soup.find("table", class_="appraisalDetailsLocation")
    if location_table:
        rows = location_table.find_all("tr")
        if len(rows) >= 3:
            headers = [th.get_text(strip=True) for th in rows[0].find_all("th")]
            values = [td.get_text(strip=True) for td in rows[1].find_all("td")]
            for h, v in zip(headers, values):
                data[h] = v

            headers_2 = [th.get_text(strip=True) for th in rows[2].find_all("th")]
            values_2 = [td.get_text(strip=True) for td in rows[3].find_all("td")]
            for h, v in zip(headers_2, values_2):
                data[h] = v

    for link in soup.find_all("a", href=True):
        href = link.get("href")
        text = link.get_text(strip=True)
        if "Google Maps" in text:
            data["Google Map Link"] = href
        elif "Tax Map Viewer" in text:
            data["Tax Map Link"] = href
        elif "Pictometry Aerial Viewer" in text:
            data["Pictometry Aerial Viewer"] = href

    image_link_tag = soup.select_one("div.imgDisplay a[href*='/dotnet/photo/photo.aspx']")
    if image_link_tag and image_link_tag.get("href"):
        data["Image of Structure"] = urljoin("https://www.leepa.org", image_link_tag["href"])

    # Extract Tax Map image from <img src=...> inside #divDisplayParcelTaxMap
    tax_map_img = soup.select_one("#divDisplayParcelTaxMap img[src*='TaxMapImage.aspx']")
    if tax_map_img and tax_map_img.get("src"):
        data["Building Aerial Viewer"] = tax_map_img["src"].replace("&amp;", "&")

    inspection_div = soup.find("div", class_="LastInspectionDiv")
    if inspection_div:
        match = re.search(r"Last Inspection Date:\s*(\d{2}/\d{2}/\d{4})", inspection_div.get_text())
        if match:
            data["Last Inspection Date"] = match.group(1)

    return data


def extract_address_history(soup):
    table = soup.find("table", class_="detailsTable")
    results = []
    if table:
        rows = table.find_all("tr")
        headers = [th.get_text(strip=True) for th in rows[0].find_all("th")]
        for row in rows[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all("td")]
            if len(cells) == len(headers):
                entry = dict(zip(headers, cells))
                entry["county_name"] = "lee"
                if all(not entry.get(header) for header in headers if header != "Maintenance Date"):
                    continue
                results.append(entry)
    return results


def get_owner_details(soup):
    owner_box = soup.find("div", id="divDisplayParcelOwner")
    if owner_box:
        panels = owner_box.find_all("div", class_="textPanel")
        if panels:
            lines = panels[0].get_text(separator="\n", strip=True).split("\n")
            names = lines[0] if len(lines) > 0 else ""
            street_number, street_name, city, zip_code, state = "", "", "", "", ""
            if len(lines) > 1:
                line = lines[1].split()
                street_number = line[0]
                street_name = " ".join(line[1:])
            if len(lines) > 2:
                line = lines[2].split()
                city = " ".join(line[:-2])
                state = line[-2]
                zip_code = line[-1]
            address = {
                "Street Number": street_number,
                "Street Name": street_name,
                "City": city,
                "Zip": zip_code,
                "state": state
            }
            return address, names
    return {}, ""


def parse_tables(soup, df=None, folio_id=None):
    results = {}
    results["Property Description"] = extract_property_attributes(soup)

    for box in soup.select("div.box"):
        section_title = box.find("div", class_="sectionTitle")
        if not section_title:
            continue

        main_title = section_title.find("a", class_="nonLinkLinks")
        section_name = main_title.get_text(strip=True) if main_title else \
            section_title.get_text(strip=True).split("Generated on")[0].strip()
        section_name_lower = section_name.lower()

        if "property details" in section_name_lower:
            continue

        if "alternate address information" in section_name_lower:
            alt_data = extract_alternate_address_info(box)
            if alt_data:
                results["Alternate Address Information"] = alt_data
            continue

        if "property data" in section_name_lower:
            text = box.get_text(" ", strip=True)
            strap = None
            extracted_folio = None
            if "strap:" in text.lower() and "folio id:" in text.lower():
                try:
                    strap = text.split("STRAP:")[1].split("Folio ID:")[0].strip()
                    extracted_folio = text.split("Folio ID:")[1].split()[0].strip()
                except:
                    pass

            # Fallback to passed-in folio_id and lookup in df if needed
            if not extracted_folio:
                extracted_folio = folio_id

            if (not strap or strap.lower() == 'none') and df is not None and extracted_folio in df.index:
                strap = df.loc[extracted_folio].get("STRAP", "").strip()

            results["Property Data"] = {
                "STRAP": strap,
                "Folio ID": extracted_folio,
                "Owner of Record": [],
                "Owner Address": [],
                "Site Address": extract_address_history(soup),
            }
            continue

        section_data = []
        for table in box.find_all("table"):
            headers = []
            rows = table.find_all("tr")
            if not rows:
                continue

            first_row_ths = rows[0].find_all("th")
            start_index = 1 if first_row_ths else 0
            if first_row_ths:
                headers = [th.get_text(strip=True) for th in first_row_ths]

            for row in rows[start_index:]:
                cells = row.find_all(["td", "th"])
                cell_values = [td.get_text(strip=True) for td in cells]
                if not any(cell_values):
                    continue

                row_dict = {}
                for col_idx, cell in enumerate(cells):
                    header = headers[col_idx] if col_idx < len(headers) else f"Col_{col_idx}"
                    text = cell.get_text(strip=True)
                    if any(excluded_text in text for excluded_text in
                           ["View Recorded Plat at LeeClerk.org", "freeProperty Fraud Alert"]):
                        continue
                    row_dict[header] = text

                    links = extract_links_from_cell(cell)

                    if links:
                        row_dict[f"{header}_Links"] = links

                if any(v.strip() not in ["", ".", "-", "N/A"] for v in row_dict.values() if isinstance(v, str)):
                    section_data.append(row_dict)

        if section_data:
            results[section_name] = section_data
    garbage_info = extract_garbage_details(soup)
    if garbage_info:
        results["Solid Waste (Garbage) Roll Data"] = garbage_info

    flood_info = extract_flood_and_storm_info(soup)
    if flood_info:
        results["Flood and Storm Information"] = flood_info

    real_property_data = extract_real_property_tag_info(soup)
    if real_property_data:
        results["Real Property Tag Information"] = real_property_data

    if "Property Data" in results:
        try:
            folio_id = results["Property Data"].get("Folio ID")
            if folio_id and folio_id in df.index:
                row = df.loc[folio_id]
                results["Property Data"]["STRAP"] = row.get("STRAP", "")
                results["Property Data"]["Folio ID"] = str(folio_id)
                owner1 = row.get("OwnerName", "")
                owner2 = row.get("Others", "")

                owner_list = []
                if pd.notna(owner1) and owner1.strip():
                    owner_list.append(owner1.strip())
                if pd.notna(owner2) and owner2.strip():
                    owner_list.append(owner2.strip())

                results["Property Data"]["Owner of Record"] = owner_list

                results["Property Data"]["Owner Address"] = {
                    "Street Number": row.get("OwnerAddress1", "").split()[0] if pd.notna(
                        row.get("OwnerAddress1", "")) else "",
                    "Street Name": " ".join(row.get("OwnerAddress1", "").split()[1:]) if pd.notna(
                        row.get("OwnerAddress1", "")) else "",
                    "City": row.get("OwnerCity", ""),
                    "Zip": row.get("OwnerZip", ""),
                    "state": row.get("OwnerState", "")
                }
        except Exception as e:
            logger.error(f"Error using CSV data for folio {folio_id}: {e}")

    # Extra property sections
    property_box = soup.find("div", id="PropertyDetailsCurrent")
    if property_box:
        results["Property Details"] = extract_property_details(soup)
    if property_box:
        buildings = extract_buildings_info(property_box, base_url="https://www.leepa.org")
        if buildings:
            results["Property Details"].update({"Building Info": buildings})

    condo_sections = soup.select("div.innerBox:has(div.sectionSubTitle:contains('Condominium'))")
    if condo_sections:
        results["Property Details"].update({"Condominium": extract_condo_info(condo_sections[0])})

    return results


def load_dataframe(csv_path):
    """Load and prepare the dataframe from CSV."""
    try:
        df = pd.read_csv(csv_path, dtype=str)
        df["FolioID"] = df["FolioID"].astype(str)
        df.set_index("FolioID", inplace=True)
        return df
    except Exception as e:
        logger.error(f"Error loading CSV file {csv_path}: {e}")
        return pd.DataFrame()


@lru_cache(maxsize=1)
def get_dataframe(csv_path):
    """Cached dataframe to avoid reloading in each process."""
    return load_dataframe(csv_path)


def process_html_file(args):
    """Process a single HTML file and return the result data."""
    filepath, csv_path, chunk_id = args
    try:
        # Get the dataframe (cached)
        df = get_dataframe(csv_path)
        
        with open(filepath, 'r', encoding='utf-8') as f:
            html = f.read()
        
        # Try to use lxml parser for better performance, fall back to html.parser if not available
        try:
            soup = BeautifulSoup(html, 'lxml')
        except Exception as parser_error:
            logger.warning(f"lxml parser not available, falling back to html.parser: {parser_error}")
            soup = BeautifulSoup(html, 'html.parser')
        folio_id = os.path.splitext(os.path.basename(filepath))[0]
        return folio_id, parse_tables(soup, df, folio_id=folio_id), None
    except Exception as e:
        logger.error(f"Error processing file {filepath}: {e}")
        print(traceback.format_exc())
        return os.path.basename(filepath), {"error": str(e)}, filepath


def chunk_files(file_list, num_chunks):
    """Divide the file list into chunks for better memory management."""
    chunk_size = max(1, len(file_list) // num_chunks)
    for i in range(0, len(file_list), chunk_size):
        yield file_list[i:i + chunk_size]


def process_chunk(chunk_files, csv_path, output_folder, failed_folder, chunk_id):
    """Process a chunk of files."""
    results = []
    
    tasks = [(filepath, csv_path, chunk_id) for filepath in chunk_files]
    
    with ProcessPoolExecutor(max_workers=max(1, os.cpu_count()-1)) as executor:
        for folio_id, data, failed_path in executor.map(process_html_file, tasks):
            if failed_path is None:
                output_path = os.path.join(output_folder, f"{folio_id}.json")
                with open(output_path, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2)
                results.append(folio_id)
            else:
                # Move failed file
                shutil.copy(failed_path, os.path.join(failed_folder, os.path.basename(failed_path)))
                logger.warning(f"Failed to process: {folio_id}")
    
    return results


def main():
    parser = argparse.ArgumentParser(description='Process HTML files to extract data.')
    parser.add_argument('--input', default=DEFAULT_INPUT_FOLDER, help='Input folder containing HTML files')
    parser.add_argument('--output', default=DEFAULT_OUTPUT_FOLDER, help='Output folder for JSON files')
    parser.add_argument('--csv', default=DEFAULT_CSV_PATH, help='Path to CSV file with property data')
    parser.add_argument('--chunk-size', type=int, default=1000, help='Number of files to process in each chunk')
    parser.add_argument('--processes', type=int, default=0, 
                        help='Number of processes to use (0 for auto-detection)')
    
    args = parser.parse_args()
    
    # Check for optimal dependencies
    try:
        import lxml
        logger.info("Using lxml parser for optimal performance")
    except ImportError:
        logger.warning("lxml parser not found. For better performance, install it with: pip install lxml")
    
    # Create output and failed directories
    os.makedirs(args.output, exist_ok=True)
    os.makedirs(FAILED_FOLDER, exist_ok=True)
    
    # Set processes to use
    num_processes = args.processes if args.processes > 0 else max(1, os.cpu_count() - 1)
    
    # Get list of HTML files
    html_files = [os.path.join(args.input, f) for f in os.listdir(args.input) if f.endswith(".html")]
    total_files = len(html_files)
    logger.info(f"Found {total_files} HTML files to process")
    
    # Determine optimal chunk size based on number of files
    num_chunks = max(1, total_files // args.chunk_size)
    logger.info(f"Processing files in {num_chunks} chunks with {num_processes} parallel processes")
    
    # Process files in chunks
    processed_count = 0
    
    for i, chunk in enumerate(chunk_files(html_files, num_chunks)):
        logger.info(f"Processing chunk {i+1}/{num_chunks} ({len(chunk)} files)")
        results = process_chunk(chunk, args.csv, args.output, FAILED_FOLDER, i)
        processed_count += len(results)
        
        # Report progress
        logger.info(f"Chunk {i+1} complete: {len(results)} files processed successfully")
        logger.info(f"Progress: {processed_count}/{total_files} ({processed_count/total_files*100:.1f}%)")
    
    logger.info(f"Processing complete. {processed_count} files processed successfully.")
    failed_count = total_files - processed_count
    if failed_count > 0:
        logger.warning(f"{failed_count} files failed processing and were moved to {FAILED_FOLDER}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Processing interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)