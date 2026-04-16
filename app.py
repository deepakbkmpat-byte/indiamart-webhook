from flask import Flask, request, jsonify
import gspread
from google.oauth2.service_account import Credentials
import json
import re
import os
from datetime import datetime

app = Flask(__name__)

GOOGLE_SHEET_ID  = "10Tg9Pu1Cm-4u8-efQrFSvgedIg3Q-iN-ZSdsDZJIiQg"
GOOGLE_SHEET_TAB = "Calling_Log"
YOUR_EMAIL       = "roshnibabakitalo@gmail.com"
CREDENTIALS_FILE = "imartcredential.json"

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

def extract_lead(data):
    # Try RESPONSE key first
    if isinstance(data, dict) and "RESPONSE" in data:
        response = data["RESPONSE"]
        if isinstance(response, list) and len(response) > 0:
            return response[0]
        elif isinstance(response, dict):
            return response

    # Try direct list
    if isinstance(data, list) and len(data) > 0:
        return data[0]

    # Try direct dict
    if isinstance(data, dict):
        # Check if it has lead fields directly
        if "SENDER_NAME" in data or "SENDER_MOBILE" in data:
            return data

    return {}

@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        raw = request.get_data(as_text=True)
        print(f"📥 Raw data: {raw}")

        data = request.get_json(force=True, silent=True)
        if not data:
            print("⚠️ No JSON data received!")
            return jsonify({"status": "no data"}), 200

        print(f"📥 Parsed JSON: {json.dumps(data, indent=2)}")

        lead = extract_lead(data)
        print(f"📋 Extracted lead: {json.dumps(lead, indent=2)}")

        qid          = parse_field(lead.get("UNIQUE_QUERY_ID", ""))
        product_name = parse_field(lead.get("SUBJECT", "") or lead.get("QUERY_PRODUCT_NAME", "") or lead.get("QUERY_MCAT_NAME", ""))
        message      = parse_field(lead.get("QUERY_MESSAGE", ""))
        sender_name  = parse_field(lead.get("SENDER_NAME", ""))
        sender_phone = parse_field(lead.get("SENDER_MOBILE", "") or lead.get("SENDER_PHONE", "") or lead.get("RECEIVER_MOBILE", ""))
        sender_email = parse_field(lead.get("SENDER_EMAIL", ""))
        sender_city  = parse_field(lead.get("SENDER_CITY", ""))
        query_time   = parse_field(lead.get("QUERY_TIME", ""))

        # Clean phone number
        sender_phone = sender_phone.replace("+91-", "").replace("+91", "").strip()

        print(f"✅ Name: {sender_name} | Phone: {sender_phone} | Product: {product_name} | City: {sender_city}")

        # Skip if no useful data
        if not sender_name and not sender_phone:
            print("⚠️ Empty lead - skipping!")
            return jsonify({"status": "skipped"}), 200

        sheet = get_sheet()

        # Check duplicate
        existing_ids = set(sheet.col_values(2)[1:])
        if qid and qid in existing_ids:
            print(f"⚠️ Duplicate: {qid}")
            return jsonify({"status": "duplicate"}), 200

        row = [
            YOUR_EMAIL,
            qid,
            query_time,
            "INDIAMART",
            sender_name,
            sender_phone,
            sender_email,
            product_name,
            detect_enquiry_type(product_name),
            extract_quantity(message),
            "COLD",
            "", "",
            "", "", "", "",
            "", "", "", "",
            "", "", "", "",
            "", ""
        ]

        sheet.append_row(row, value_input_option="USER_ENTERED")
        print(f"✅ Lead saved successfully: {sender_name} | {sender_phone} | {product_name}")

        return jsonify({"status": "success"}), 200

    except Exception as e:
        print(f"❌ Error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

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
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)