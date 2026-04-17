import requests
import gspread
from google.oauth2.service_account import Credentials
import json
import re
from datetime import datetime

# Settings
API_KEY = "mRy0FLps7XvFSfet7nyP7lqKo1DMnDZi"
GOOGLE_SHEET_ID = "10Tg9Pu1Cm-4u8-efQrFSvgedIg3Q-iN-ZSdsDZJIiQg"
GOOGLE_SHEET_TAB = "Calling_Log"
YOUR_EMAIL = "roshnibabakitalo@gmail.com"
CREDENTIALS_FILE = "imartcredential.json"

ENQUIRY_KEYWORDS = {
    "GRANITE": [
        "granite", "black galaxy", "z black", "r black",
        "lapatro", "alaska", "p white", "tan brown",
        "steel grey", "moon white", "galaxy black"
    ],
    "ITALIAN": [
        "italian", "onyx", "marble", "travertine", "statuario",
        "carrara", "cloud blue", "dyna", "sofita", "volakas",
        "satvario", "spider grey", "michelangelo"
    ],
    "GLASS": [
        "glass", "mirror", "toughened", "tempered", "upvc",
        "window", "baba glass", "sliding door", "sliding window",
        "upvc door", "upvc window", "led mirror"
    ],
    "KALINGA": [
        "kalinga", "quartz", "countertop"
    ],
    "INDIAN MARBLE": [
        "indian marble", "makrana", "banswara", "nano", "katni"
    ],
}

def detect_enquiry_type(text):
    if not text:
        return "OTHER"
    t = text.lower()
    for etype, keywords in ENQUIRY_KEYWORDS.items():
        for kw in keywords:
            if kw in t:
                return etype
    return "OTHER"

def extract_quantity(message):
    if not message:
        return ""
    match = re.search(
        r'(\d+[\.,]?\d*)\s*(piece|pcs|kg|ton|sqft|sq\.ft|meter|mtr|unit|nos|sq|feet|ft)',
        message, re.IGNORECASE
    )
    return match.group(0) if match else ""

def get_sheet():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(
        CREDENTIALS_FILE, scopes=scopes
    )
    client = gspread.authorize(creds)
    return client.open_by_key(GOOGLE_SHEET_ID).worksheet(GOOGLE_SHEET_TAB)

def fetch_leads():
    url = "https://arms.indiamart.com/arms/api/getContactList"
    params = {
        "glusr_usr_key": API_KEY,
        "start_time": "16-APR-2026 00:00:00",
        "end_time": datetime.now().strftime("%d-%b-%Y %H:%M:%S").upper(),
    }
    response = requests.get(url, params=params, timeout=30)
    print(f"Status: {response.status_code}")
    print(f"Response: {response.text[:500]}")
    return response.json()

def main():
    print("Starting Pull API...")
    
    sheet = get_sheet()
    existing_ids = set(sheet.col_values(2)[1:])
    print(f"Existing leads: {len(existing_ids)}")

    data = fetch_leads()

    if not data or data.get("CODE") != 200:
        print(f"API Error: {data}")
        return

    leads = data.get("RESPONSE", [])
    print(f"Total leads from API: {len(leads)}")

    added = 0
    for lead in leads:
        qid = str(lead.get("UNIQUE_QUERY_ID", ""))

        if qid in existing_ids:
            print(f"Duplicate skipped: {qid}")
            continue

        name    = str(lead.get("SENDER_NAME", "") or "")
        phone   = str(lead.get("SENDER_MOBILE", "") or "").replace("+91-", "").replace("+91", "")
        email   = str(lead.get("SENDER_EMAIL", "") or "")
        city    = str(lead.get("SENDER_CITY", "") or "")
        address = str(lead.get("SENDER_ADDRESS", "") or "")
        subject = str(lead.get("SUBJECT", "") or lead.get("QUERY_PRODUCT_NAME", "") or "")
        message = str(lead.get("QUERY_MESSAGE", "") or "")
        time    = str(lead.get("QUERY_TIME", "") or "")

        full_address = ""
        if city and city.lower() != "noida":
            full_address = city
        if address and city.lower() != "noida":
            full_address = address

        if not name and not phone:
            print("Empty lead skipped!")
            continue

        row = [
            YOUR_EMAIL,
            qid,
            time,
            "INDIAMART",
            name,
            phone,
            email,
            full_address,
            subject,
            detect_enquiry_type(subject),
            extract_quantity(message),
            "COLD",
            "", "",
            "", "", "", "",
            "", "", "", "",
            "", "", "", "",
            "", ""
        ]

        sheet.append_row(row, value_input_option="USER_ENTERED")
        existing_ids.add(qid)
        added += 1
        print(f"✅ Added: {name} | {phone} | {subject}")

    print(f"Total added: {added}")

if __name__ == "__main__":
    main()