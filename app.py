from flask import Flask, request, jsonify
import gspread
from google.oauth2.service_account import Credentials
import json
import re
import os
import requests
from datetime import datetime
import threading
import time

app = Flask(__name__)

GOOGLE_SHEET_ID  = "10Tg9Pu1Cm-4u8-efQrFSvgedIg3Q-iN-ZSdsDZJIiQg"
GOOGLE_SHEET_TAB = "Calling_Log"
YOUR_EMAIL       = "roshnibabakitalo@gmail.com"
CREDENTIALS_FILE = "imartcredential.json"
API_KEY          = "mRy0FLps7XvFSfet7nyP7lqKo1DMnDZi"

def get_sheet():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds  = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=scopes)
    client = gspread.authorize(creds)
    sheet  = client.open_by_key(GOOGLE_SHEET_ID).worksheet(GOOGLE_SHEET_TAB)
    return sheet

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

def detect_enquiry_type(product_name):
    if not product_name:
        return "OTHER"
    p = product_name.lower()
    for enquiry_type, keywords in ENQUIRY_KEYWORDS.items():
        for keyword in keywords:
            if keyword in p:
                return enquiry_type
    return "OTHER"

def extract_quantity(message):
    if not message:
        return ""
    match = re.search(
        r'(\d+[\.,]?\d*)\s*(piece|pcs|kg|ton|sqft|sq\.ft|meter|mtr|unit|nos|sq|feet|ft)',
        message, re.IGNORECASE
    )
    return match.group(0) if match else ""

def parse_field(value):
    if value is None:
        return ""
    if isinstance(value, list):
        try:
            return ''.join([chr(int(x)) for x in value])
        except:
            return str(value)
    return str(value).strip()

def find_lead_fields(obj):
    if isinstance(obj, dict):
        if any(key in obj for key in [
            "SENDER_NAME", "SENDER_MOBILE",
            "UNIQUE_QUERY_ID", "SUBJECT",
            "QUERY_TIME", "SENDER_CITY"
        ]):
            return obj
        for key, value in obj.items():
            result = find_lead_fields(value)
            if result:
                return result
    elif isinstance(obj, list):
        for item in obj:
            result = find_lead_fields(item)
            if result:
                return result
    return None

def extract_lead(data):
    if isinstance(data, str):
        try:
            data = json.loads(data)
        except:
            return {}
    lead = find_lead_fields(data)
    return lead if lead else {}

def save_lead_to_sheet(sheet, lead, existing_ids):
    qid          = parse_field(lead.get("UNIQUE_QUERY_ID", ""))
    product_name = parse_field(
        lead.get("SUBJECT", "") or
        lead.get("QUERY_PRODUCT_NAME", "") or
        lead.get("QUERY_MCAT_NAME", "")
    )
    message      = parse_field(lead.get("QUERY_MESSAGE", ""))
    sender_name  = parse_field(lead.get("SENDER_NAME", ""))
    sender_phone = parse_field(
        lead.get("SENDER_MOBILE", "") or
        lead.get("SENDER_PHONE", "") or
        lead.get("SENDER_MOBILE_ALT", "")
    )
    sender_email = parse_field(lead.get("SENDER_EMAIL", ""))
    sender_city  = parse_field(lead.get("SENDER_CITY", ""))
    sender_address = parse_field(lead.get("SENDER_ADDRESS", ""))
    query_time   = parse_field(lead.get("QUERY_TIME", ""))

    # Clean phone
    sender_phone = sender_phone.replace("+91-", "").replace("+91", "").strip()

    # Full address
    full_address = ""
    if sender_city and sender_city.lower() != "noida":
        full_address = sender_city
    if sender_address and sender_city.lower() != "noida":
        full_address = sender_address

    # Skip empty leads
    if not sender_name and not sender_phone:
        return False

    # Check duplicate
    if qid and qid in existing_ids:
        return False

    row = [
        YOUR_EMAIL,        # EMAIL ID
        qid,               # Call_ID (Key)
        query_time,        # ENQ.DATE
        "INDIAMART",       # LEAD SOURCE
        sender_name,       # CUSTOMER NAME
        sender_phone,      # CONTACT NUMBER
        sender_email,      # CUSTOMER EMAIL
        full_address,      # ADDRESS
        product_name,      # ENQUIRE FOR
        detect_enquiry_type(product_name),  # ENQUIRE TYPE
        extract_quantity(message),          # QTY.REQD
        "COLD",            # LEAD TYPE
        "", "",            # FOLLOW UP DATE, REMARKS
        "", "", "", "",    # FOLLOW UP 1,2
        "", "", "", "",    # FOLLOW UP 3,4
        "", "", "", "",    # FOLLOW UP 5,6
        "", ""             # Call Start, Call End
    ]

    sheet.append_row(row, value_input_option="USER_ENTERED")
    existing_ids.add(qid)
    print(f"🎉 Lead saved: {sender_name} | {sender_phone} | {product_name}")
    return True

def pull_inquiry_leads():
    print("🔄 Starting Pull API...")
    try:
        # Try different API URLs
        urls = [
            "https://seller.indiamart.com/webservice/getcontactlist",
            "https://seller.indiamart.com/webservice/getContactList",
            "https://seller.indiamart.com/lmsapi/getcontactlist",
        ]

        now = datetime.now()
        params = {
            "glusr_usr_key": API_KEY,
            "start_time": "01-APR-2026 00:00:00",
            "end_time": now.strftime("%d-%b-%Y %H:%M:%S").upper(),
            "result_count": 10,
            "page_number": 1,
        }
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json",
        }

        for url in urls:
            print(f"Trying URL: {url}")
            response = requests.get(
                url, params=params,
                headers=headers, timeout=30
            )
            print(f"Status: {response.status_code}")
            print(f"Response: {response.text[:300]}")

            if response.status_code == 200:
                try:
                    data = response.json()
                    if data.get("CODE") == 200:
                        leads = data.get("RESPONSE", [])
                        print(f"Total leads: {len(leads)}")

                        sheet = get_sheet()
                        existing_ids = set(sheet.col_values(2)[1:])

                        added = 0
                        for lead in leads:
                            if save_lead_to_sheet(sheet, lead, existing_ids):
                                added += 1

                        print(f"✅ Done! Added: {added} new leads")
                        return
                    else:
                        print(f"API Error: {data}")
                except Exception as e:
                    print(f"JSON Error: {e}")
                    continue

        print("❌ All URLs failed!")

    except Exception as e:
        print(f"❌ Pull API Error: {e}")

def run_pull_scheduler():
    time.sleep(60)
    while True:
        pull_inquiry_leads()
        print("⏰ Next pull in 2 hours...")
        time.sleep(7200)

@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        raw = request.get_data(as_text=True)
        print(f"📥 Raw: {raw}")

        data = request.get_json(force=True, silent=True)
        if not data:
            print("⚠️ No JSON received!")
            return jsonify({"status": "no data"}), 200

        print(f"📥 JSON: {json.dumps(data, indent=2)}")

        lead = extract_lead(data)
        print(f"📋 Lead found: {json.dumps(lead, indent=2)}")

        sheet = get_sheet()
        existing_ids = set(sheet.col_values(2)[1:])

        qid = parse_field(lead.get("UNIQUE_QUERY_ID", ""))

        if qid and qid in existing_ids:
            print(f"⚠️ Duplicate: {qid}")
            return jsonify({"status": "duplicate"}), 200

        if save_lead_to_sheet(sheet, lead, existing_ids):
            return jsonify({"status": "success"}), 200
        else:
            print("⚠️ Empty lead - skipping!")
            return jsonify({"status": "skipped"}), 200

    except Exception as e:
        print(f"❌ Error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/pull", methods=["GET"])
def manual_pull():
    thread = threading.Thread(target=pull_inquiry_leads)
    thread.start()
    return jsonify({"status": "pull started"}), 200

@app.route("/", methods=["GET"])
def home():
    return jsonify({
        "status": "running",
        "message": "IndiaMART Webhook Server is Live! ⚡",
        "time": datetime.now().strftime("%d-%b-%Y %H:%M:%S")
    })

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "healthy"}), 200

if __name__ == "__main__":
    scheduler = threading.Thread(target=run_pull_scheduler)
    scheduler.daemon = True
    scheduler.start()

    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)