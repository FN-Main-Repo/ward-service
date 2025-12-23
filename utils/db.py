from supabase import create_client, Client
import os
import re
import logging
logger = logging.getLogger(__name__)


STOPWORDS = {
    "near", "opp", "opposite", "beside", "behind",
    "road", "rd", "street", "st", "lane", "ln",
    "primary", "school", "temple", "masjid",
    "park", "hospital"
}

def normalize_address(text: str) -> str:
    text = text.lower()
    text = re.sub(r"[^\w\s]", " ", text)
    tokens = [t for t in text.split() if t not in STOPWORDS]
    return " ".join(tokens)


def extract_address_tokens(address: str):
    normalized = normalize_address(address)
    parts = [p for p in normalized.split() if not p.isdigit()]

    tokens = set(parts)

    
    for i in range(len(parts) - 1):
        tokens.add(parts[i] + " " + parts[i + 1])
        tokens.add(parts[i] + parts[i + 1])

    
    for i in range(len(parts) - 2):
        tokens.add(parts[i] + " " + parts[i + 1] + " " + parts[i + 2])
        tokens.add(parts[i] + parts[i + 1] + parts[i + 2])

    return list(tokens)

def fuzzy_match_ward_name(supabase, address: str, min_score=0.5):
    tokens = extract_address_tokens(address)
    best = None

    for token in tokens:
        resp = supabase.rpc(
            "search_ward_phonetic",
            {"p_query": token}
        ).execute()

        for row in resp.data or []:
            if row["score"] >= min_score:
                if not best or row["score"] > best["score"]:
                    best = row

    return best


def fuzzy_match_mohalla(supabase, address: str, min_score: float = 0.3):
    tokens = extract_address_tokens(address)

    best_match = None

    for token in tokens:
        resp = supabase.rpc(
            "fuzzy_search_mohallas",
            {"p_query": token}
        ).execute()

        for row in resp.data or []:
            if row["score"] >= min_score:
                if not best_match or row["score"] > best_match["score"]:
                    best_match = row

    return best_match


def resolve_ward_from_address(supabase, address: str, city="lucknow"):
    result = {
        "resolved": False,
        "ward_number": None,
        "ward_name": None,
        "matched_mohalla": None,
        "confidence": 0.0,
        "basis": None,
        "reason": None
    }

    mohalla_match = fuzzy_match_mohalla(supabase, address)
    ward_match = fuzzy_match_ward_name(supabase, address)
    
    # Strong mohalla
    if mohalla_match and mohalla_match["score"] >= 0.5:
        ward = (
            supabase.table("wards")
            .select("ward_number, ward_name")
            .eq("id", mohalla_match["ward_id"])
            .execute()
        ).data[0]

        result.update({
            "resolved": True,
            "ward_number": ward["ward_number"],
            "ward_name": ward["ward_name"],
            "matched_mohalla": mohalla_match["mohalla_name"],
            "confidence": round(mohalla_match["score"], 2),
            "basis": "mohalla"
        })
        return result


    if ward_match and ward_match["score"] >= 0.75:
        result.update({
            "resolved": True,
            "ward_number": ward_match["ward_number"],
            "ward_name": ward_match["ward_name"],
            "confidence": round(ward_match["score"], 2),
            "basis": "ward_name"
        })
        return result


    result["reason"] = "Insufficient confidence"
    return result



if __name__ == "__main__":
    import dotenv
    from supabase import create_client
    dotenv.load_dotenv('.env.local')
    supabase = create_client(
        os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    )
    test_addresses = [
        "462/236 ramganj hussianabad lucknow"
    ]

    for addr in test_addresses:
        result = resolve_ward_from_address(supabase, addr)
        print(f"Address: {addr}")
        print(f"Result: {result}")
        print("-" * 40)
