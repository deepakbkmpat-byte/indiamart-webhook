"""
IndiaMART Push API Webhook Server
Receives leads instantly and saves to Google Sheet!
"""

from flask import Flask, request, jsonify
import gspread
from google.oauth2.service_account import Credentials
import json
import re
import os
from datetime import datetime

app = Flask(__name__)

# ============================================================
# CONFIG
# ============================================================

GOOGLE_SHEET_ID  = "10Tg9Pu1Cm-4u8-efQrFSvgedIg3Q-iN-ZSdsDZJIiQg"
GOOGLE_SHEET_TAB = "Calling_Log"
YOUR_EMAIL       = "roshnibabakitalo@gmail.com"
CREDENTIALS_FILE = "imartcredential.json"

# ============================================================
# Google Sheet Connection
# ============================================================

def get_sheet():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds  = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=scopes)
    client = gspread.authorize(creds)
    sheet  = client.open_by_key(GOOGLE_SHEET_ID).worksheet(GOOGLE_SHEET_TAB)
    return sheet

# ============================================================
# Smart Enquiry Type Detection
# ============================================================

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
        "window", "baba glass", "sliding door"
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

# ============================================================
# Webhook Endpoint — IndiaMART sends leads here!
# ============================================================

@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        data = request.get_json(force=True)
        print(f"📥 Lead received: {json.dumps(data, indent=2)}")

        # Get lead details
        lead         = data if isinstance(data, dict) else data[0]
        qid          = lead.get("UNIQUE_QUERY_ID", "")
        product_name = lead.get("SUBJECT", "")
        message      = lead.get("QUERY_MESSAGE", "")

        # Connect to Google Sheet
        sheet = get_sheet()

        # Check duplicate
        existing_ids = set(sheet.col_values(2)[1:])
        if qid and qid in existing_ids:
            print(f"⚠️ Duplicate lead ignored: {qid}")
            return jsonify({"status": "duplicate"}), 200

        # Format row
        row = [
            YOUR_EMAIL,
            qid,
            lead.get("QUERY_TIME", ""),
            "INDIAMART",
            lead.get("SENDER_NAME", ""),
            lead.get("SENDER_MOBILE", ""),
            lead.get("SENDER_EMAIL", ""),
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

        # Add to sheet
        sheet.append_row(row, value_input_option="USER_ENTERED")
        print(f"✅ Lead added: {lead.get('SENDER_NAME')} | {product_name}")

        return jsonify({"status": "success"}), 200

    except Exception as e:
        print(f"❌ Error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

# ============================================================
# Health Check
# ============================================================

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

# ============================================================
# Run Server
# ============================================================

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)