import os
import json
import ulid
import math
import re
import traceback
import pandas as pd
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import time
import argparse
import boto3
from botocore.exceptions import ClientError
import io
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

# Configuration parameters

INPUT_FOLDER = "total_lee_county_parsed"
OUTPUT_FOLDER = "mapped_outputs_total_lee_county_parsed/"


# INPUT_FOLDER = "test_download_lee"
# OUTPUT_FOLDER = "mapped_outputs_test_download_lee/"
STRAP_CSV_PATH = "../data/filtered_strap_data.csv"
S3_BUCKET = "lee-county-oracle-data"
S3_PREFIX = "lexicon-full"

# Load data
owners_df = pd.read_csv("../data/full_names_combined.csv", dtype={"folio_id": str})
owners_df = owners_df.where(pd.notna(owners_df), "")

strap_df = pd.read_csv(STRAP_CSV_PATH, dtype=str).fillna("")
strap_df.set_index("FolioID", inplace=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# Property type mapping
MAP_PROPERTY_TYPE = {
    "SINGLE FAMILY RESIDENTIAL": "SingleFamily",
    "MULTI-FAMILY LESS THAN 10": "MultipleFamily",
    "VACANT RESIDENTIAL": "VacantResidential",
    "CONDOMINIUM": "Condominium",
    "MULTI-FAMILY 10 OR MORE": "MultipleFamily",
    "COOPERATIVES": "Cooperative",
    "MOBILE HOME": "MobileHome",
}

# Directional mapping
DIRECTIONAL_MAP = {
    "NORTH": "N",
    "SOUTH": "S",
    "EAST": "E",
    "WEST": "W",
    "NORTHEAST": "NE",
    "NORTHWEST": "NW",
    "SOUTHEAST": "SE",
    "SOUTHWEST": "SW",
    "N": "N",
    "S": "S",
    "E": "E",
    "W": "W",
    "NE": "NE",
    "NW": "NW",
    "SE": "SE",
    "SW": "SW",
}

# USPS street suffixes
USPS_SUFFIXES = {
    "ALLEE": "ALY",
    "ALLEY": "ALY",
    "ALLY": "ALY",
    "ALY": "ALY",
    "ANEX": "ANX",
    "ANNEX": "ANX",
    "ANNX": "ANX",
    "ANX": "ANX",
    "ARC": "ARC",
    "ARCADE": "ARC",
    "AV": "AVE",
    "AVE": "AVE",
    "AVEN": "AVE",
    "AVENU": "AVE",
    "AVENUE": "AVE",
    "AVN": "AVE",
    "AVNUE": "AVE",
    "BAYOO": "BYU",
    "BAYOU": "BYU",
    "BCH": "BCH",
    "BEACH": "BCH",
    "BEND": "BND",
    "BND": "BND",
    "BLF": "BLF",
    "BLUF": "BLF",
    "BLUFF": "BLF",
    "BLUFFS": "BLFS",
    "BOT": "BTM",
    "BTM": "BTM",
    "BOTTM": "BTM",
    "BOTTOM": "BTM",
    "BLVD": "BLVD",
    "BOUL": "BLVD",
    "BOULEVARD": "BLVD",
    "BOULV": "BLVD",
    "BR": "BR",
    "BRNCH": "BR",
    "BRANCH": "BR",
    "BRDGE": "BRG",
    "BRG": "BRG",
    "BRIDGE": "BRG",
    "BRK": "BRK",
    "BROOK": "BRK",
    "BROOKS": "BRKS",
    "BURG": "BG",
    "BURGS": "BGS",
    "BYP": "BYP",
    "BYPA": "BYP",
    "BYPAS": "BYP",
    "BYPASS": "BYP",
    "BYPS": "BYP",
    "CAMP": "CP",
    "CP": "CP",
    "CMP": "CP",
    "CANYN": "CYN",
    "CANYON": "CYN",
    "CNYN": "CYN",
    "CAPE": "CPE",
    "CPE": "CPE",
    "CAUSEWAY": "CSWY",
    "CAUSWA": "CSWY",
    "CSWY": "CSWY",
    "CEN": "CTR",
    "CENT": "CTR",
    "CENTER": "CTR",
    "CENTR": "CTR",
    "CENTRE": "CTR",
    "CNTER": "CTR",
    "CNTR": "CTR",
    "CTR": "CTR",
    "CENTERS": "CTRS",
    "CIR": "CIR",
    "CIRC": "CIR",
    "CIRCL": "CIR",
    "CIRCLE": "CIR",
    "CRCL": "CIR",
    "CRCLE": "CIR",
    "CIRCLES": "CIRS",
    "CLF": "CLF",
    "CLIFF": "CLF",
    "CLFS": "CLFS",
    "CLIFFS": "CLFS",
    "CLB": "CLB",
    "CLUB": "CLB",
    "COMMON": "CMN",
    "COMMONS": "CMNS",
    "COR": "COR",
    "CORNER": "COR",
    "CORNERS": "CORS",
    "CORS": "CORS",
    "COURSE": "CRSE",
    "CRSE": "CRSE",
    "COURT": "CT",
    "CT": "CT",
    "COURTS": "CTS",
    "CTS": "CTS",
    "COVE": "CV",
    "CV": "CV",
    "COVES": "CVS",
    "CREEK": "CRK",
    "CRK": "CRK",
    "CRESCENT": "CRES",
    "CRES": "CRES",
    "CRSENT": "CRES",
    "CRSNT": "CRES",
    "CREST": "CRST",
    "CROSSING": "XING",
    "CRSSNG": "XING",
    "XING": "XING",
    "CROSSROAD": "XRD",
    "CROSSROADS": "XRDS",
    "CURVE": "CURV",
    "DALE": "DL",
    "DL": "DL",
    "DAM": "DM",
    "DM": "DM",
    "DIV": "DV",
    "DIVIDE": "DV",
    "DV": "DV",
    "DVD": "DV",
    "DR": "DR",
    "DRIV": "DR",
    "DRIVE": "DR",
    "DRV": "DR",
    "DRIVES": "DRS",
    "EST": "EST",
    "ESTATE": "EST",
    "ESTATES": "ESTS",
    "ESTS": "ESTS",
    "EXP": "EXPY",
    "EXPR": "EXPY",
    "EXPRESS": "EXPY",
    "EXPRESSWAY": "EXPY",
    "EXPW": "EXPY",
    "EXPY": "EXPY",
    "EXT": "EXT",
    "EXTENSION": "EXT",
    "EXTN": "EXT",
    "EXTNSN": "EXT",
    "EXTENSIONS": "EXTS",
    "EXTS": "EXTS",
    "FALL": "FALL",
    "FALLS": "FLS",
    "FLS": "FLS",
    "FERRY": "FRY",
    "FRRY": "FRY",
    "FRY": "FRY",
    "FIELD": "FLD",
    "FLD": "FLD",
    "FIELDS": "FLDS",
    "FLDS": "FLDS",
    "FLAT": "FLT",
    "FLT": "FLT",
    "FLATS": "FLTS",
    "FLTS": "FLTS",
    "FORD": "FRD",
    "FRD": "FRD",
    "FORDS": "FRDS",
    "FOREST": "FRST",
    "FORESTS": "FRST",
    "FRST": "FRST",
    "FORG": "FRG",
    "FORGE": "FRG",
    "FRG": "FRG",
    "FORGES": "FRGS",
    "FORK": "FRK",
    "FRK": "FRK",
    "FORKS": "FRKS",
    "FRKS": "FRKS",
    "FORT": "FT",
    "FRT": "FT",
    "FT": "FT",
    "FREEWAY": "FWY",
    "FREEWY": "FWY",
    "FRWAY": "FWY",
    "FRWY": "FWY",
    "FWY": "FWY",
    "GARDEN": "GDN",
    "GARDN": "GDN",
    "GRDEN": "GDN",
    "GRDN": "GDN",
    "GARDENS": "GDNS",
    "GDNS": "GDNS",
    "GRDNS": "GDNS",
    "GATEWAY": "GTWY",
    "GATEWY": "GTWY",
    "GATWAY": "GTWY",
    "GTWAY": "GTWY",
    "GTWY": "GTWY",
    "GLEN": "GLN",
    "GLN": "GLN",
    "GLENS": "GLNS",
    "GREEN": "GRN",
    "GRN": "GRN",
    "GREENS": "GRNS",
    "GROV": "GRV",
    "GROVE": "GRV",
    "GRV": "GRV",
    "GROVES": "GRVS",
    "HARB": "HBR",
    "HARBOR": "HBR",
    "HARBR": "HBR",
    "HBR": "HBR",
    "HRBOR": "HBR",
    "HARBORS": "HBRS",
    "HAVEN": "HVN",
    "HVN": "HVN",
    "HT": "HTS",
    "HTS": "HTS",
    "HIGHWAY": "HWY",
    "HIGHWY": "HWY",
    "HIWAY": "HWY",
    "HIWY": "HWY",
    "HWAY": "HWY",
    "HWY": "HWY",
    "HILL": "HL",
    "HL": "HL",
    "HILLS": "HLS",
    "HLS": "HLS",
    "HLLW": "HOLW",
    "HOLLOW": "HOLW",
    "HOLLOWS": "HOLW",
    "HOLW": "HOLW",
    "HOLWS": "HOLW",
    "INLET": "INLT",
    "INLT": "INLT",
    "IS": "IS",
    "ISLAND": "IS",
    "ISLND": "IS",
    "ISLANDS": "ISS",
    "ISLNDS": "ISS",
    "ISS": "ISS",
    "ISLE": "ISLE",
    "ISLES": "ISLE",
    "JCT": "JCT",
    "JCTION": "JCT",
    "JCTN": "JCT",
    "JUNCTION": "JCT",
    "JUNCTN": "JCT",
    "JUNCTON": "JCT",
    "JCTNS": "JCTS",
    "JCTS": "JCTS",
    "JUNCTIONS": "JCTS",
    "KEY": "KY",
    "KY": "KY",
    "KEYS": "KYS",
    "KYS": "KYS",
    "KNL": "KNL",
    "KNOL": "KNL",
    "KNOLL": "KNL",
    "KNLS": "KNLS",
    "KNOLLS": "KNLS",
    "LK": "LK",
    "LAKE": "LK",
    "LKS": "LKS",
    "LAKES": "LKS",
    "LAND": "LAND",
    "LANDING": "LNDG",
    "LNDG": "LNDG",
    "LNDNG": "LNDG",
    "LANE": "LN",
    "LN": "LN",
    "LGT": "LGT",
    "LIGHT": "LGT",
    "LIGHTS": "LGTS",
    "LF": "LF",
    "LOAF": "LF",
    "LCK": "LCK",
    "LOCK": "LCK",
    "LCKS": "LCKS",
    "LOCKS": "LCKS",
    "LDG": "LDG",
    "LDGE": "LDG",
    "LODG": "LDG",
    "LODGE": "LDG",
    "LOOP": "LOOP",
    "LOOPS": "LOOP",
    "MALL": "MALL",
    "MNR": "MNR",
    "MANOR": "MNR",
    "MANORS": "MNRS",
    "MNRS": "MNRS",
    "MEADOW": "MDW",
    "MDW": "MDW",
    "MDWS": "MDWS",
    "MEADOWS": "MDWS",
    "MEDOWS": "MDWS",
    "MEWS": "MEWS",
    "MILL": "ML",
    "MILLS": "MLS",
    "MISSION": "MSN",
    "MISSN": "MSN",
    "MSSN": "MSN",
    "MOTORWAY": "MTWY",
    "MNT": "MT",
    "MT": "MT",
    "MOUNT": "MT",
    "MNTAIN": "MTN",
    "MNTN": "MTN",
    "MOUNTAIN": "MTN",
    "MOUNTIN": "MTN",
    "MTIN": "MTN",
    "MTN": "MTN",
    "MNTNS": "MTNS",
    "MOUNTAINS": "MTNS",
    "NCK": "NCK",
    "NECK": "NCK",
    "ORCH": "ORCH",
    "ORCHARD": "ORCH",
    "ORCHRD": "ORCH",
    "OVAL": "OVAL",
    "OVL": "OVAL",
    "OVERPASS": "OPAS",
    "PARK": "PARK",
    "PRK": "PARK",
    "PARKS": "PARK",
    "PARKWAY": "PKWY",
    "PARKWY": "PKWY",
    "PKWAY": "PKWY",
    "PKWY": "PKWY",
    "PKY": "PKWY",
    "PARKWAYS": "PKWY",
    "PKWYS": "PKWY",
    "PASS": "PASS",
    "PASSAGE": "PSGE",
    "PATH": "PATH",
    "PATHS": "PATH",
    "PIKE": "PIKE",
    "PIKES": "PIKE",
    "PINE": "PNE",
    "PINES": "PNES",
    "PNES": "PNES",
    "PLACE": "PL",
    "PL": "PL",
    "PLAIN": "PLN",
    "PLN": "PLN",
    "PLAINS": "PLNS",
    "PLNS": "PLNS",
    "PLAZA": "PLZ",
    "PLZ": "PLZ",
    "PLZA": "PLZ",
    "POINT": "PT",
    "PT": "PT",
    "POINTS": "PTS",
    "PTS": "PTS",
    "PORT": "PRT",
    "PRT": "PRT",
    "PORTS": "PRTS",
    "PRTS": "PRTS",
    "PR": "PR",
    "PRAIRIE": "PR",
    "PRR": "PR",
    "RAD": "RADL",
    "RADIAL": "RADL",
    "RADIEL": "RADL",
    "RADL": "RADL",
    "RAMP": "RAMP",
    "RANCH": "RNCH",
    "RANCHES": "RNCH",
    "RNCH": "RNCH",
    "RNCHS": "RNCH",
    "RAPID": "RPD",
    "RPD": "RPD",
    "RAPIDS": "RPDS",
    "RPDS": "RPDS",
    "REST": "RST",
    "RST": "RST",
    "RDG": "RDG",
    "RDGE": "RDG",
    "RIDGE": "RDG",
    "RDGS": "RDGS",
    "RIDGES": "RDGS",
    "RIV": "RIV",
    "RIVER": "RIV",
    "RVR": "RIV",
    "RIVR": "RIV",
    "RD": "RD",
    "ROAD": "RD",
    "ROADS": "RDS",
    "RDS": "RDS",
    "ROUTE": "RTE",
    "ROW": "RTE",
    "RUE": "RUE",
    "RUN": "RUN",
    "SHL": "SHL",
    "SHOAL": "SHL",
    "SHLS": "SHLS",
    "SHOALS": "SHLS",
    "SHOAR": "SHR",
    "SHORE": "SHR",
    "SHR": "SHR",
    "SHOARS": "SHRS",
    "SHORES": "SHRS",
    "SHRS": "SHRS",
    "SKYWAY": "SKWY",
    "SPG": "SPG",
    "SPNG": "SPG",
    "SPRING": "SPG",
    "SPRNG": "SPG",
    "SPGS": "SPGS",
    "SPNGS": "SPGS",
    "SPRINGS": "SPGS",
    "SPRNGS": "SPGS",
    "SPUR": "SPUR",
    "SPURS": "SPUR",
    "SQ": "SQ",
    "SQR": "SQ",
    "SQRE": "SQ",
    "SQU": "SQ",
    "SQUARE": "SQ",
    "SQRS": "SQS",
    "SQUARES": "SQS",
    "STA": "STA",
    "STATION": "STA",
    "STATN": "STA",
    "STN": "STA",
    "STRA": "STRA",
    "STRAV": "STRA",
    "STRAVEN": "STRA",
    "STRAVENUE": "STRA",
    "STRAVN": "STRA",
    "STRVN": "STRA",
    "STRVNUE": "STRA",
    "STREAM": "STRM",
    "STREME": "STRM",
    "STRM": "STRM",
    "STREET": "ST",
    "STRT": "ST",
    "ST": "ST",
    "STR": "ST",
    "STREETS": "STS",
    "SMT": "SMT",
    "SUMIT": "SMT",
    "SUMITT": "SMT",
    "SUMMIT": "SMT",
    "TER": "TER",
    "TERR": "TER",
    "TERRACE": "TER",
    "THROUGHWAY": "TRWY",
    "TRACE": "TRCE",
    "TRACES": "TRCE",
    "TRCE": "TRCE",
    "TRACK": "TRAK",
    "TRACKS": "TRAK",
    "TRAK": "TRAK",
    "TRK": "TRAK",
    "TRKS": "TRAK",
    "TRAFFICWAY": "TRFY",
    "TRAIL": "TRL",
    "TRAILS": "TRL",
    "TRL": "TRL",
    "TRLS": "TRL",
    "TRAILER": "TRLR",
    "TRLR": "TRLR",
    "TRLRS": "TRLR",
    "TUNEL": "TUNL",
    "TUNL": "TUNL",
    "TUNLS": "TUNL",
    "TUNNEL": "TUNL",
    "TUNNELS": "TUNL",
    "TUNNL": "TUNL",
    "TRNPK": "TPKE",
    "TURNPIKE": "TPKE",
    "TURNPK": "TPKE",
    "UNDERPASS": "UPAS",
    "UN": "UN",
    "UNION": "UN",
    "UNIONS": "UNS",
    "VALLEY": "VLY",
    "VALLY": "VLY",
    "VLLY": "VLY",
    "VLY": "VLY",
    "VALLEYS": "VLYS",
    "VLYS": "VLYS",
    "VDCT": "VIA",
    "VIA": "VIA",
    "VIADCT": "VIA",
    "VIADUCT": "VIA",
    "VIEW": "VW",
    "VW": "VW",
    "VIEWS": "VWS",
    "VWS": "VWS",
    "VILL": "VLG",
    "VILLAG": "VLG",
    "VILLAGE": "VLG",
    "VILLG": "VLG",
    "VILLIAGE": "VLG",
    "VLG": "VLG",
    "VILLAGES": "VLGS",
    "VLGS": "VLGS",
    "VILLE": "VL",
    "VL": "VL",
    "VIS": "VIS",
    "VIST": "VIS",
    "VISTA": "VIS",
    "VST": "VIS",
    "VSTA": "VIS",
    "WALK": "WALK",
    "WALKS": "WALK",
    "WALL": "WALL",
    "WY": "WAY",
    "WAY": "WAY",
    "WAYS": "WAYS",
    "WELL": "WL",
    "WELLS": "WLS",
    "WLS": "WLS",
}

def normalize_city_name(city_name: str) -> str:
    return ' '.join(word.capitalize() for word in city_name.strip().split())

def normalize_directional(word):
    return DIRECTIONAL_MAP.get(word.upper())

def normalize_suffix(word):
    return USPS_SUFFIXES.get(word.upper())

def title_case_street_name(name):
    """Title case and keep words like 'Via Messina' correctly capitalized"""
    return " ".join(word.capitalize() for word in name.split())

def process_street_name(street_name: str):
    tokens = street_name.strip().upper().split()

    pre_dir = None
    post_dir = None
    suffix = None

    if tokens and tokens[0] in DIRECTIONAL_MAP:
        pre_dir = normalize_directional(tokens.pop(0))

    if tokens and tokens[-1] in DIRECTIONAL_MAP:
        post_dir = normalize_directional(tokens.pop())

    if tokens and tokens[-1] in USPS_SUFFIXES:
        suffix = normalize_suffix(tokens.pop())

    base_name = title_case_street_name(" ".join(tokens))

    return {
        "street_pre_directional_text": pre_dir,
        "street_name": base_name,
        "street_suffix_type": suffix,
        "street_post_directional_text": post_dir,
    }

def generate_id(_: str = "") -> str:
    """Generate a ULID-based ID prefixed with 01."""
    return str(ulid.new())

def safe_int(value, default=""):
    try:
        return int(str(value).replace(",", "").strip())
    except (ValueError, TypeError):
        return default

def normalize_parcel_identifier(raw_strap: str) -> str:
    return raw_strap.replace("-", "").replace(".", "")

def safe_float(value, default=""):
    try:
        return float(str(value).replace(",", "").strip())
    except (ValueError, TypeError):
        return default

def normalize_address(addr: str) -> str:
    return str(addr).replace("\r\n", " ").replace("\n", " ").replace("\r", " ").strip()

def map_addresses(tax_entries):
    address_id_map = {}
    for entry in tax_entries:
        addr = normalize_address(entry.get("Mailing Address", ""))
        if addr:
            address_id_map[addr] = generate_id()
    return address_id_map

def get_latest_sale_date(sales_data):
    valid_dates = []

    for sale in sales_data:
        date_str = sale.get("Date", "")
        try:
            # Parse the date from MM/DD/YYYY format
            date_obj = datetime.strptime(date_str, "%m/%d/%Y")
            valid_dates.append(date_obj)
        except ValueError:
            continue  # Skip rows with bad date format

    if not valid_dates:
        return None

    # Get the latest date and return it in ISO format YYYY-MM-DD
    latest_date = max(valid_dates)
    return latest_date.strftime("%Y-%m-%d")

def map_sales_histories(data, property_id, people, ownerships_id):
    sales_data = data.get("Sales / Transactions", [])
    sales_histories = []
    sales_documents = []
    valuations = []
    finance_relations = []
    latest_year = get_latest_sale_date(sales_data)

    for entry in sales_data:
        date_str = entry.get("Date")
        price_str = entry.get("Sale Price", "0")
        file_num = entry.get("ClerkFile Number", "").replace("Sales Questionnaire Complete", "").replace("Complete Sales Questionnaire","").strip()
        file_links = entry.get("ClerkFile Number_Links", [])

        # Format date
        formatted_date = None
        if date_str:
            try:
                formatted_date = datetime.strptime(date_str, "%m/%d/%Y").strftime("%Y-%m-%d")
            except ValueError:
                formatted_date = date_str

        # Document
        doc_id = None
        if file_links and file_links[0]!= "https://www.leepa.org":
            doc_id = generate_id()
            sales_documents.append({
                "@id": doc_id,
                "@type": "document",
                "instrument_number": file_num,
                "document_identifier": "Warranty Deed",
                "document_date": formatted_date,
                "document_url": file_links[0]
            })
        sales_id = generate_id()
        # Sale history
        sale = {
            "@id": sales_id,
            "@type": "sales_transaction",
            "sales_date": formatted_date,
            "sales_transaction_amount": safe_float(price_str),
        }
        if doc_id:
            sale["has_document"] = doc_id

        sales_histories.append(sale)


        finance_relation = {
            "@id": generate_id(),
            "@type": "finance_relation",
            "has_property": property_id,
            "has_sales_transactions": sales_id,
        }
        if safe_float(price_str) > 0:
            valuations_id = generate_id()
            valuations.append({
                "@id": valuations_id,
                "@type": "property_valuation",
                "valuation_date": formatted_date,
                "actual_value": safe_float(price_str),
                "valuation_method_type": "SalesTransaction"
            })
            finance_relation["has_property_valuation"] = valuations_id
        if formatted_date == latest_year:
            person_ids=[]
            for person in people:
                person_ids.append(person["@id"])
            finance_relation["has_ownership"] = ownerships_id
        finance_relations.append(finance_relation)
    return sales_histories, sales_documents, valuations, finance_relations, latest_year

def extract_people_ownerships_communications(folio_id, property_id, strap_row, data):
    ownerships = []
    companies = []
    people = []
    communications = []
    addresses = []
    ownerships_id = []
    sales_data = data.get("Sales / Transactions", [])
    latest_year = get_latest_sale_date(sales_data)
    property_owners = owners_df[owners_df["folio_id"] == str(folio_id)]
    # iterate over the rows
    for index, row in property_owners.iterrows():
        name_type = row["name_type"]
        address_id = generate_id()
        address_obj = {
            "@id": address_id,
            "@type": "address",
            "address_line_1": strap_row.get("OwnerAddress1", "").strip(),
            "address_line_2": strap_row.get("OwnerAddress2", "").strip(),
            "city_name": normalize_city_name(strap_row.get("OwnerCity", "").strip()),
            "state_code": strap_row.get("OwnerState", "").strip(),
            "postal_code": strap_row.get("OwnerZip", "").strip(),
            "country_name": strap_row.get("OwnerCountry", "").strip() or "USA"
        }

        addresses.append(address_obj)
        communication_id = generate_id()
        communications.append({
            "@id": communication_id,
            "@type": "communication",
            "has_mailing_address": address_id
        })
        if name_type == "person":
            # Create a person node
            person_id = generate_id()
            person_node = {
                "@id": person_id,
                "@type": "person",
                "first_name": row["first_name"].strip(),
                "last_name": row["last_name"].strip(),
                "middle_name": row["middle_name"].strip(),
                "prefix_name": row["prefix_name"].strip() or row["surname_prefix"].strip(),
                "suffix_name": row["suffix_name"].strip(),
                "raw_name": row["raw_name"].strip(),
                "has_communication_method": communication_id,
            }
            people.append(person_node)

            ownership_id = generate_id()
            # Create an ownership node
            ownership_node = {
                "@id": ownership_id,
                "@type": "ownership",
                "owned_by": person_id,
                "owned_property": property_id,
                "date_acquired": latest_year
            }
            ownerships.append(ownership_node)
            ownerships_id.append(ownership_id)
        else:
            # Create a company node
            company_id = generate_id()
            company_node = {
                "@id": company_id,
                "@type": "company",
                "name": row["raw_name"].strip(),
                "has_communication_method": communication_id,
            }
            companies.append(company_node)

            ownership_id = generate_id()
            # Create an ownership node
            ownership_node = {
                "@id": ownership_id,
                "@type": "ownership",
                "has_owner_company": company_id,
                "owned_property": property_id,
                "date_acquired": latest_year
            }
            ownerships.append(ownership_node)
            ownerships_id.append(ownership_id)
    return ownerships, companies, people, addresses, communications, ownerships_id


def clean_image_url(url):
    parsed = urlparse(url)

    netloc = parsed.netloc.replace('.www', '').replace('www.', '')

    query_params = parse_qs(parsed.query)
    query_params.pop('Height', None)
    query_params.pop('Width', None)

    cleaned_query = urlencode(query_params, doseq=True)
    cleaned_url = urlunparse((parsed.scheme, netloc, parsed.path, '', cleaned_query, ''))

    return cleaned_url

def map_property_photos(prop_desc, buildings_info, folio_id):
    """Creates document entries for known photo types and returns their IDs."""
    photos = []
    photo_ids = []

    PHOTO_FIELDS = {
        "Image of Structure": "StructureImage",
        # "Building Aerial Viewer": "BuildingAerialViewer",
        # "Google Map Link": "GoogleMapLink",
        # "Tax Map Link": "TaxMapLink",
        # "Pictometry Aerial Viewer": "PictometryAerialViewer"
    }

    # for field, doc_label in PHOTO_FIELDS.items():
    #     url = prop_desc.get(field)
    #     if url:
    #         pid = generate_id()
    #         photos.append({
    #             "@id": pid,
    #             "@type": "document",
    #             "document_identifier": doc_label,
    #             "property_image_url": clean_image_url(url)
    #         })
    #
    #         photo_ids.append(pid)
    # 2. Handle additional photos from local 33936/ folder using folio_id.json
    folder_path = "all_photos"
    photo_json_path = os.path.join(folder_path, f"{folio_id}.json")
    if os.path.isfile(photo_json_path):
        try:
            with open(photo_json_path, "r", encoding="utf-8") as f:
                photo_entries = json.load(f)
            for entry in photo_entries:
                url = entry.get("image_url")
                if url:
                    pid = generate_id()
                    photos.append({
                        "@id": pid,
                        "@type": "document",
                        "document_identifier": "StructureImage",
                        "property_image_url": clean_image_url(url)
                    })
                    photo_ids.append(pid)
        except Exception as e:
            print(f"Error reading image JSON for folio {folio_id}: {e}")

    # for building in buildings_info:
    #     # Also check for building footprint in building info
    #     building_info = building.get("Photos and Footprint", {})
    #     footprint_urls = building_info.get("Building Foot Print", [])
    #     for url in footprint_urls:
    #         pid = generate_id()
    #         photos.append({
    #             "@id": pid,
    #             "@type": "document",
    #             "document_identifier": "BuildingFootprint",
    #             "property_image_url": url
    #         })
    #         photo_ids.append(pid)

    return photos, photo_ids

def extract_latest_trim_values(data):
    """
    Extracts the latest year with valid Market Assessed, Taxable, and Exemptions values
    from the 'Property Values / Exemptions / TRIM Notices' section.
    """
    trim_list = data.get("Property Values / Exemptions / TRIM Notices", [])
    latest_year = None
    latest_entry = None

    for entry in trim_list:
        tax_year_str = entry.get("Tax Year", "")
        match = re.search(r"\b(19|20)\d{2}\b", tax_year_str)
        if not match:
            continue
        year = int(match.group(0))
        if (latest_year is None) or (year > latest_year):
            latest_year = year
            latest_entry = entry
    market_assessed = None
    taxable = None
    exemptions = None

    if latest_entry:
        market_assessed = safe_float(latest_entry.get("Market Assessed", "0"))
        taxable = safe_float(latest_entry.get("Taxable", "0"))
        exemptions = safe_float(latest_entry.get("Exemptions", "0"))
    return taxable, market_assessed, exemptions

def remove_empty_values(obj):
    """
    Recursively remove:
      - None or empty string values from dicts and lists
      - empty dicts
      - empty lists
    Returns cleaned object or None if the entire structure is effectively empty.
    """

    invalid_values = ["", "N/A", "None", "null", None]

    if isinstance(obj, dict):
        # We'll build a new dict with only non-empty items
        new_dict = {}
        for key, value in obj.items():
            cleaned_value = remove_empty_values(value)
            # Skip if it became None or an empty container
            if cleaned_value is None:
                continue
            if isinstance(cleaned_value, dict) and not cleaned_value:
                # empty dict
                continue
            if isinstance(cleaned_value, list) and not cleaned_value:
                # empty list
                continue
            if cleaned_value in invalid_values:
                # empty string
                continue

            new_dict[key] = cleaned_value

        # If the resulting dict is empty, return None (indicates to the parent that this should be dropped)
        if not new_dict:
            return None
        return new_dict

    elif isinstance(obj, list):
        # Build a new list with only non-empty items
        new_list = []
        for item in obj:
            cleaned_item = remove_empty_values(item)
            # Skip if None or empty container/string
            if cleaned_item is None:
                continue
            if isinstance(cleaned_item, dict) and not cleaned_item:
                continue
            if isinstance(cleaned_item, list) and not cleaned_item:
                continue
            if cleaned_item in invalid_values:
                continue
            new_list.append(cleaned_item)
        return new_list

    else:
        # For scalars (strings, numbers, booleans, etc.)
        # Remove if it's None or empty string
        if obj is None:
            return None
        if obj in invalid_values:
            return None
        # Keep everything else (e.g., numeric 0 is kept, booleans are kept)
        return obj

MAP_USE_CODE = {
    "1": "SingleFamily",
    "2": "MobileHome",
    "3": "MultipleFamily",
    "4": "Condominium",
    "5": "Cooperative",
    "8": "MultipleFamily"
}
def get_property_type(property_description):
    use_code = property_description.get("Use Code", None)
    if not use_code:
        return None
    if len(use_code) == 4 and use_code.startswith("0"):
        use_code = use_code[1]
    else:
        use_code = use_code[0]
    return MAP_USE_CODE.get(use_code, "Other")


def transform(data):
    try:
        folio_id = data["Property Data"]["Folio ID"]
        property_id = generate_id()
        prop_desc = data["Property Description"]
        buildings = data.get("Property Details", {}).get("Building Info", [])
        land_tracts = data.get("Property Details", {}).get("Land Tracts", {})
        photo_docs, photo_ids = map_property_photos(prop_desc, buildings, folio_id)

        use_code = data.get("Property Details", {}).get("Land Tracts",{}).get("Use Code", None)
        if (
                not use_code or
                use_code in ["300", "900", "0"] or
                (use_code.startswith("9") and len(use_code) in [3, 4])
        ):
            return None


        prop_desc = data.get("Property Description", {})
        address_id = generate_id()

        if str(folio_id) not in strap_df.index:
            return None
        strap_row = strap_df.loc[str(folio_id)] if str(folio_id) in strap_df.index else None

        ownerships, companies, people, comm_addresses, communications, ownerships_id = extract_people_ownerships_communications(folio_id, property_id, strap_row, data)
        sales_histories, sales_docs, property_valuations, finance_relations, latest_year = map_sales_histories(data, property_id, people, ownerships_id)

        if strap_row is not None:
            site_number = strap_row["SiteStreetNumber"].strip()
            site_name = strap_row["SiteStreetName"].strip()
            city = strap_row.get("SiteCity").strip()
            zip_code = strap_row.get("SiteZIP").strip()
            unit_number =strap_row.get("SiteUnit").strip()
            # print(site_name)
            street_parts = process_street_name(site_name)
            sequence_number = site_number

            # address_line_1 = f"{sequence_number} {street_parts.get("street_name")}".strip()
            # full_address = f"{address_line_1}, {city}, FL {zip_code}"

            address_obj = {
                "@id": address_id,
                "@type": "address",
                # "full_address": full_address,
                # "address_line_1": address_line_1,
                "sequence_number": sequence_number,
                "street_name": street_parts["street_name"],
                "city_name": normalize_city_name(city.strip()),
                "street_post_directional_text": street_parts.get("street_post_directional_text"),
                "street_pre_directional_text": street_parts.get("street_pre_directional_text"),
                "street_suffix_type": street_parts.get("street_suffix_type"),
                "postal_code": zip_code,
                "state_code": "FL",
                "county_name": "Lee County",
                "country_code": "US",
                "unit_identifier": unit_number,
                "latitude": float(prop_desc.get("Latitude")) if prop_desc.get("Latitude") else None,
                "longitude": float(prop_desc.get("Longitude>")) if prop_desc.get("Longitude>") else None,
                # "municipality_name": prop_desc.get("Municipality", "")
            }
        property_taxable_value_amount, property_assessed_value_amount, property_exemption_amount = extract_latest_trim_values(data)
        property_obj = {
            "@id": property_id,
            "@type": "property",
            "parcel_identifier": normalize_parcel_identifier(data["Property Data"]["STRAP"]),
            "property_condition_description": prop_desc.get("Property Description", ""),
            "lot_size_square_feet": safe_int(
                next((v for k, v in prop_desc.items() if "gross building area" in k.lower()), "")),
            "lot_living_size_square_feet": safe_int(prop_desc.get("Gross Living Area")),
            "property_taxable_value_amount": property_taxable_value_amount,
            "property_assessed_value_amount": property_assessed_value_amount,
            "property_exemption_amount": property_exemption_amount,
            "lot": prop_desc.get("Lot"),
            "section": prop_desc.get("Section"),
            "block": prop_desc.get("Block"),
            "township": prop_desc.get("Township"),
            "range": prop_desc.get("Range"),
            "has_address": address_id,
            "property_type": get_property_type(land_tracts)
        }

        if photo_ids:
            property_obj["has_photos"] = photo_ids

        if prop_desc.get("Total Bedrooms / Bathrooms", "0/0") != "0":
            beds, baths = prop_desc.get("Total Bedrooms / Bathrooms", "0/0").split("/")
            property_obj.update({
                "full_bathroom_count": math.floor(safe_float(baths)),
                "half_bathroom_count": None if math.ceil(
                    safe_float(baths) - math.floor(safe_float(baths))) == 0 else math.ceil(
                    safe_float(baths) - math.floor(safe_float(baths))),
                "bedroom_count": safe_int(beds),
            })

        if prop_desc.get("1st Year Building on Tax Roll") and prop_desc["1st Year Building on Tax Roll"] != "N/A":
            property_obj["property_structure_built_year"] = safe_int(prop_desc["1st Year Building on Tax Roll"])

        tax_entries = data.get("Taxing Authorities", [])
        address_id_map = map_addresses(tax_entries)
        addresses = addresses = comm_addresses + [address_obj]
        # print(people)
        # print(sales_histories)
        lexicon_data = {
            "companies": companies,
            "people": people,
            "communications": communications,
            "ownerships": ownerships,
            "property_valuations": property_valuations,
            "properties": [property_obj],
            "documents": photo_docs + sales_docs,
            "addresses": addresses,
            "sales_transactions": sales_histories,
            "relationships": finance_relations,
        }
        lexicon_data = remove_empty_values(lexicon_data)
        return lexicon_data
    except Exception as e:
        print(f"Error in transform for folio: {data.get('Property Data', {}).get('Folio ID', 'Unknown')}")
        print(traceback.format_exc())
        return None

def upload_to_s3(s3_client, json_content, filename, bucket, prefix):
    """Upload JSON content to S3 bucket"""
    try:
        key = f"{prefix}/{filename}"
        s3_client.upload_fileobj(
            io.BytesIO(json.dumps(json_content).encode('utf-8')),
            bucket,
            key,
            ExtraArgs={'ContentType': 'application/json'}
        )
        return True
    except ClientError as e:
        print(f"Error uploading {filename} to S3: {e}")
        return False

def process_file(filename, input_folder, output_folder, s3_client, s3_bucket, s3_prefix):
    try:
        path = os.path.join(input_folder, filename)
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        mapped = transform(data)
        if mapped is None:
            return (filename, "skipped")
        
        # Save locally
        out_path = os.path.join(output_folder, filename)
        with open(out_path, "w", encoding="utf-8") as outf:
            json.dump(mapped, outf, indent=2)
        
        # Upload to S3
        upload_success = upload_to_s3(s3_client, mapped, filename, s3_bucket, s3_prefix)
        if upload_success:
            return (filename, "success")
        else:
            return (filename, "s3_error")
            
    except Exception as e:
        print(f"Failed to process {filename}: {e}")
        return (filename, f"error: {str(e)}")

def main():
    parser = argparse.ArgumentParser(description='Convert property data to lexicon format using threading and upload to S3.')
    parser.add_argument('--input_folder', type=str, default=INPUT_FOLDER, help='Input folder path')
    parser.add_argument('--output_folder', type=str, default=OUTPUT_FOLDER, help='Output folder path')
    parser.add_argument('--max_workers', type=int, default=30, help='Maximum number of worker threads')
    parser.add_argument('--s3_bucket', type=str, default=S3_BUCKET, help='S3 bucket name')
    parser.add_argument('--s3_prefix', type=str, default=S3_PREFIX, help='S3 prefix/folder name')
    
    args = parser.parse_args()
    
    input_folder = args.input_folder
    output_folder = args.output_folder
    max_workers = args.max_workers
    s3_bucket = args.s3_bucket
    s3_prefix = args.s3_prefix
    
    # Initialize S3 client
    try:
        # Try default credentials first
        s3_client = boto3.client('s3')
        # Test connection
        s3_client.list_buckets()
        print("Using default AWS credentials")
    except Exception:
        try:
            # Try using a named profile
            session = boto3.Session(profile_name="cerf-dev")
            s3_client = session.client('s3')
            # Test connection
            s3_client.list_buckets()
            print("Using cerf-dev AWS profile credentials")
        except Exception as e:
            print(f"Error connecting to AWS: {e}")
            print("Please ensure AWS credentials are properly configured")
            return
    
    os.makedirs(output_folder, exist_ok=True)
    files = [f for f in os.listdir(input_folder) if f.endswith(".json")]
    
    print(f"Processing {len(files)} files using {max_workers} threads...")
    print(f"Files will be saved locally to {output_folder}")
    print(f"Files will be uploaded to S3 bucket {s3_bucket} with prefix {s3_prefix}")
    
    start_time = time.time()
    
    success_count = 0
    skipped_count = 0
    s3_error_count = 0
    error_count = 0
    
    # Process files using ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Create a dictionary of futures
        future_to_file = {
            executor.submit(
                process_file, filename, input_folder, output_folder, s3_client, s3_bucket, s3_prefix
            ): filename for filename in files
        }
        
        # Track progress with tqdm
        for future in tqdm(as_completed(future_to_file), total=len(files), desc="Converting files"):
            filename = future_to_file[future]
            try:
                result = future.result()
                if result[1] == "success":
                    success_count += 1
                elif result[1] == "skipped":
                    skipped_count += 1
                elif result[1] == "s3_error":
                    s3_error_count += 1
                else:
                    error_count += 1
            except Exception as e:
                print(f"Error processing {filename}: {e}")
                error_count += 1
    
    end_time = time.time()
    duration = end_time - start_time
    
    print("\nProcessing complete!")
    print(f"Successful conversions and uploads: {success_count}")
    print(f"Skipped files: {skipped_count}")
    print(f"S3 upload errors: {s3_error_count}")
    print(f"Processing errors: {error_count}")
    print(f"Total time taken: {duration:.2f} seconds")
    print(f"Average time per file: {duration/len(files):.2f} seconds")

if __name__ == '__main__':
    main()