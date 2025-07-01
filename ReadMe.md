
# 🏡 Extracting Property Information from County Appraisal Sites

This guide walks you through the process of extracting property information from county appraiser websites. It is the first step before mapping property data into our structured **Lexicon Schema**.

---

## 📍 Step 1: Access the County Appraisal Site

Start by identifying which county the property is in and visiting the appropriate official website. Examples:

- **Broward County Appraiser**: [https://bcpa.net/](https://bcpa.net/)
- **Lee County Property Appraiser**: [https://www.leepa.org/](https://www.leepa.org/)

> 🛠 Note: Each county uses different UIs, backend structures, and URL schemes. Understanding these is essential for building automated scrapers.

---

## 🔍 Step 2: Search for a Property

Use any available identifier to find the property:

- Address
- Parcel ID
- Folio ID
- Owner Name *(rarely used in automation)*

Each site has a search bar or API endpoint. Submitting this information redirects you to the detailed property page.

---

## 🧠 Step 3: Understand the Site Structure

Once on the property detail page, investigate how data is rendered:

### 🧱 A. Static Page

- URL may include a parameter like `FolioID=xxxxx`.
- Changing this value redirects to another property.

**Example – Lee County:**

You can directly update the URL with a new Folio ID to navigate and scrape.

### ⚙️ B. Dynamic Page (JavaScript-Rendered or API-Driven)

- Data may not appear in the page source.
- Use browser dev tools (Network tab) to find `XHR` or `Fetch` requests.
- These often return JSON responses.

**Example – Broward County:**

Broward uses two key API endpoints:

- `GetData`: Resolves address → parcel
- `getParcelInformation`: Returns full parcel data

Understanding these allows for API-based extraction, bypassing UI scraping.

---

## 💻 Step 4: Build Your Scraper

Once you understand how to access the data:

1. Loop through your list of addresses, Parcel IDs, or Folio IDs.
2. Fetch the page or call the API.
3. Parse HTML or JSON responses to extract:

Example implementations of county-specific scraper scripts:

- **Lee County Scraper**: [1-download_lee_county_html.py](https://github.com/elephant-xyz/County-scraper/blob/main/1-download_lee_county_html.py)
- **Broward County Scraper**: [download_broward_properties_info.py](https://github.com/elephant-xyz/County-scraper/blob/main/download_broward_properties_info.py)

These examples demonstrate how to construct targeted extractors tailored to each county’s data delivery method—whether static HTML or dynamic API calls.


### 🗂 Required Data:

- Property Information (structure, tax, owner, etc.)
- Document History (sales deeds, permits)
- Property Images (if available)

> ✅ **Use the Lexicon Schema** as your reference. All fields should map to this schema.
[Lexicon Schema](https://lexicon.elephant.xyz/)

---

## 🧠 Step 5: Map to Lexicon Schema with AI-Agent

After extracting raw property data, the next step is to transform it into a structured format using our internal **Elephant Knowledge Graph Schema**.

👉 **Launch AI-Agent for Property Mapping**:  
The AI-Agent automates the conversion of raw county data into a structured JSON format, ready to be minted on-chain. through only one step using[AI-Agent GitHub Repository](https://github.com/elephant-xyz/AI-Agent)

### 🧩 How It Works

1. **Understands County Data Structure**  
   It reads and interprets the raw data format from each county (e.g., HTML or JSON structure of Lee or Broward County data).

2. **Understands Lexicon Schema**  
   It aligns the data with the fields and structure defined in the Lexicon Schema, including sections like:
   - Property details
   - Owner and tax data
   - Sales and deed documents
   - Building structures and photos

3. **Creates a Mapping Script**  
   The AI-Agent then generates or uses a Python script to map county-specific fields to the standardized Lexicon format.

This process yields a final JSON object that conforms to the Lexicon schema and is ready for minting.
---

## 🧼 Property Address Normalization

Before querying a county site or API, normalize raw property addresses to:

- Match correct formats
- Avoid typos or mismatches
- Link to known Parcel or Folio IDs

### ✅ Method 1: Google Address Normalization (Geocoding API)

- Converts messy input into clean, structured data.

**Pros:**

- High accuracy
- Latitude/longitude output

**Cons:**

- Paid API after free tier
- Requires API key

### 🌍 Method 2: OpenAddresses Dataset (Open Source)

- Open-source address repository with lat/lon and parcel IDs

- Website: [openaddresses.io](https://openaddresses.io/)
- GitHub: [github.com/openaddresses](https://github.com/openaddresses/openaddresses)

**Steps:**

1. Download the dataset for your region.
2. Load and match against your raw inputs.

**Pros:**

- Free and offline-friendly
- Includes Parcel IDs (if available)

**Cons:**

- Coverage varies
- Not always up to date

---

## ✅ Summary Checklist

- [x] Identify the correct county appraisal site  
- [x] Determine if the site is static or dynamic  
- [x] Reverse-engineer the navigation or API structure  
- [x] Normalize property addresses or Parcel IDs  
  - Google Maps Geocoding API  
  - OpenAddresses dataset  
- [x] Scrape or fetch the data for each property  
- [x] Extract:
  - Property Info  
  - Documents (deeds, permits, etc.)  
  - Photos  
- [x] Use the AI-agent to convert data to the Elephant schema  
