from supabase import create_client
import pdfplumber
import re
import os
from dotenv import load_dotenv
load_dotenv(".env.local")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

PDF_PATH = "Docs/ward_suchna_english.pdf"

WARD_NO_RE = re.compile(r"^\d+$")
MOHALLA_RE = re.compile(r"^\d+\s+(.*)$")


def extract_and_store_lucknow_wards():
    city = "lucknow"
    current_ward_id = None

    with pdfplumber.open(PDF_PATH) as pdf:
        for page in pdf.pages:
            table = page.extract_table()
            if not table:
                continue

            for row in table[1:]:  # skip header
                ward_no, ward_name, mohalla_cell = row

                # Detect new ward
                if ward_no and WARD_NO_RE.match(ward_no.strip()):
                    ward_number = int(ward_no.strip())
                    ward_name = ward_name.strip()

                    ward = supabase.table("wards").upsert({
                        "city": city,
                        "ward_number": ward_number,
                        "ward_name": ward_name
                    }, on_conflict="city,ward_number").execute()

                    current_ward_id = ward.data[0]["id"]

                # Extract mohallas
                if mohalla_cell and current_ward_id:
                    for line in mohalla_cell.split("\n"):
                        match = MOHALLA_RE.match(line.strip())
                        if match:
                            supabase.table("mohallas").insert({
                                "ward_id": current_ward_id,
                                "mohalla_name": match.group(1).strip()
                            }).execute()

def clear_lucknow_data():
    
    wards = supabase.table("wards") \
        .select("id") \
        .eq("city", "lucknow") \
        .execute()

    ward_ids = [w["id"] for w in wards.data]

    if ward_ids:
        supabase.table("mohallas") \
            .delete() \
            .in_("ward_id", ward_ids) \
            .execute()

    # Then delete wards
    supabase.table("wards") \
        .delete() \
        .eq("city", "lucknow") \
        .execute()



if __name__ == "__main__":
    clear_lucknow_data()
    extract_and_store_lucknow_wards()