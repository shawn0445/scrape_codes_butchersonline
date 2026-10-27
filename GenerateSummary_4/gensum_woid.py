import pandas as pd
import re
from typing import Dict, List, Tuple

# ==========================================
# 1. TEXT CLEANING & NORMALIZATION HELPERS
# ==========================================

def clean_text(text: str) -> str:
    """Normalizes string values for joining across CSVs."""
    if pd.isna(text):
        return ""
    s = str(text).lower().strip()
    s = re.sub(r'[^a-z0-9\s]', '', s)
    s = re.sub(r'\s+', ' ', s)
    return s

def format_phone(phone: str) -> str:
    """Formats phone numbers for cleaner prose reading."""
    if not phone or pd.isna(phone) or str(phone).lower() == 'nan':
        return ""
    p = str(phone).strip()
    return p

def clean_website(url: str) -> str:
    """Extracts a readable domain or clean link from a website URL."""
    if not url or pd.isna(url) or str(url).lower() == 'nan':
        return ""
    clean = str(url).strip()
    # Strip tracking parameters if present
    clean = clean.split('?')[0].rstrip('/')
    return clean


# ==========================================
# 2. SMART HOURS GROUPING LOGIC
# ==========================================

import re
import pandas as pd

def clean_time_string(time_str: str) -> str:
    """
    Cleans character artifacts (e.g., '8â€¯AM-5:30â€¯PM') 
    and normalizes them to clean English ('8:00 AM to 5:30 PM').
    """
    if not time_str or pd.isna(time_str):
        return ""
    
    s = str(time_str)
    
    # 1. Remove UTF-8 mojibake and narrow non-breaking spaces (\u202f, \xa0, etc.)
    s = s.encode('ascii', 'ignore').decode('ascii')  # Strips rogue non-ascii artifacts
    s = re.sub(r'[\s\u202f\xa0]+', ' ', s).strip()
    
    # If the string is empty or just 'Closed'
    if not s or s.lower() == 'closed':
        return 'Closed'

    # 2. Separate hyphenated time ranges cleanly
    # Handles 8:00AM-5:30PM -> 8:00 AM to 5:30 PM
    s = re.sub(r'(\d+:\d+|\d+)\s*(AM|PM|am|pm)?\s*[\-\–\—]\s*(\d+:\d+|\d+)\s*(AM|PM|am|pm)', r'\1 \2 to \3 \4', s, flags=re.IGNORECASE)
    
    # 3. Clean up double spaces or dangling spaces
    s = re.sub(r'\s+', ' ', s).strip()
    
    return s


def summarize_operating_hours(row: pd.Series) -> str:
    """
    Groups matching consecutive days to create human-sounding schedules
    with clean, artifact-free time strings.
    """
    days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    daily_hours = []

    for day in days:
        col = f"hours_{day.lower()}"
        raw_val = row.get(col, '')
        cleaned_val = clean_time_string(raw_val)
        if cleaned_val:
            daily_hours.append((day, cleaned_val))

    if not daily_hours or len(daily_hours) < 7:
        return ""

    # Group consecutive days with identical hours
    grouped = []
    for day, hrs in daily_hours:
        if not grouped:
            grouped.append(([day], hrs))
        else:
            prev_days, prev_hrs = grouped[-1]
            if prev_hrs.lower() == hrs.lower():
                prev_days.append(day)
            else:
                grouped.append(([day], hrs))

    open_phrases = []
    closed_days = []

    for day_list, hrs in grouped:
        if hrs.lower() == 'closed':
            if len(day_list) == 1:
                closed_days.append(day_list[0] + 's')
            else:
                closed_days.append(f"{day_list[0]}s and {day_list[-1]}s")
        else:
            if len(day_list) == 1:
                day_str = day_list[0] + 's'
            elif len(day_list) == 2:
                day_str = f"{day_list[0]}s and {day_list[1]}s"
            else:
                day_str = f"{day_list[0]} through {day_list[-1]}"
            
            open_phrases.append(f"{day_str} from {hrs}")

    if not open_phrases:
        return "The shop is currently closed."

    hours_text = "The shop is open " + ", ".join(open_phrases)
    if closed_days:
        hours_text += f" (closed {', '.join(closed_days)})"
    
    return hours_text + "."


# ==========================================
# 3. TITLE GENERATOR
# ==========================================

def infer_shop_type(name: str) -> str:
    """Infers a readable shop category from the business name."""
    n = (name or "").lower()

    if any(k in n for k in ["boucherie", "boucher", "butcher"]):
        return "Butcher Shop"
    if "deli" in n:
        return "Meat Market & Deli"
    if any(k in n for k in ["seafood", "fish market", "fishmonger"]):
        return "Seafood Shop"
    if any(k in n for k in ["grocery", "food market", "supermarket"]):
        return "Specialty Food Market"
    if any(k in n for k in ["meat market", "meats", "meat"]):
        return "Meat Market"
    return "Butcher Shop"


def generate_shop_title(row: pd.Series) -> str:
    """Builds a concise listing title: Name | Type in City, Province."""
    name = str(row.get("Name", row.get("name", "Local Shop"))).strip()
    if not name or name.lower() == "nan":
        name = "Local Shop"

    city = str(row.get("City", row.get("city", ""))).strip()
    province = str(row.get("Province", row.get("province", ""))).strip()
    if city.lower() == "nan":
        city = ""
    if province.lower() == "nan":
        province = ""

    shop_type = infer_shop_type(name)
    location = f"{city}, {province}" if city and province else city

    name_already_has_type = any(
        k in name.lower()
        for k in ["butcher", "boucherie", "boucher", "meat market", "meats", "deli", "seafood"]
    )

    if location:
        if name_already_has_type:
            return f"{name} | {location}"
        return f"{name} | {shop_type} in {location}"

    if name_already_has_type:
        return name
    return f"{name} | {shop_type}"


# ==========================================
# 4. ORGANIC SENTENCE BUILDER
# ==========================================

def generate_organic_description(row: pd.Series) -> str:
    """Assembles all merged data points into a fluent, organic paragraph."""
    
    # --- A. Identity & Clean Location ---
    name = str(row.get('Name', row.get('name', 'This shop'))).strip()
    
    # CSV1 'Address' usually already contains street, city, province, country.
    address = str(row.get('Address', row.get('address', ''))).strip()
    city = str(row.get('City', row.get('city', ''))).strip()
    province = str(row.get('Province', row.get('province', ''))).strip()
    
    if address and address.lower() != 'nan':
        clean_loc = address
    elif city and city.lower() != 'nan':
        clean_loc = f"{city}, {province}".strip(', ')
    else:
        clean_loc = ""

    # Rating & Reviews
    rating = row.get('Rating', '')
    reviews = row.get('Reviews', '')
    
    rating_valid = pd.notna(rating) and str(rating).lower() != 'nan' and float(rating) > 0
    reviews_valid = pd.notna(reviews) and str(reviews).lower() != 'nan' and float(reviews) > 0
    
    # Intro sentence building
    sentences = []
    
    if rating_valid and float(rating) >= 4.5:
        intro = f"{name} is a highly rated business"
    else:
        intro = f"{name} is"

    if clean_loc:
        intro += f" located at {clean_loc}"
    
    if rating_valid:
        if reviews_valid:
            intro += f", boasting a {float(rating):.1f}-star rating across {int(float(reviews)):,} customer reviews."
        else:
            intro += f", with a {float(rating):.1f}-star customer rating."
    else:
        intro += "."
        
    sentences.append(intro)

    # --- B. Services, Convenience & E-Commerce ---
    service_opts = str(row.get('serviceOptions', '')).strip()
    site_type = str(row.get('Site_Type', '')).strip()
    
    has_services = service_opts and service_opts.lower() != 'nan'
    combined_services = f"{service_opts.lower()} {site_type.lower()}"
    
    is_ecom_available = any(k in combined_services for k in ["delivery", "in-store pick-up", "e-commerce", "online shopping", "order online"])
    
    service_sentence = ""
    if has_services:
        opts_list = [s.strip().lower() for s in service_opts.split(',')]
        formatted_opts = ", ".join(opts_list)
        
        if is_ecom_available:
            service_sentence = f"They offer convenient options including {formatted_opts}, with online ordering and pickup/delivery available."
        else:
            service_sentence = f"Service options include {formatted_opts}, focusing on in-store customer shopping."
    elif is_ecom_available:
        service_sentence = "Online shopping and order services are available for customers."
    else:
        service_sentence = "Online ordering is not currently supported for this shop."

    sentences.append(service_sentence)

    # --- C. Operating Hours ---
    hours_summary = summarize_operating_hours(row)
    if hours_summary:
        sentences.append(hours_summary)

    # --- D. Contact Details & Website ---
    phone = format_phone(row.get('Phone', ''))
    website = clean_website(row.get('Website', ''))
    
    contact_parts = []
    if website:
        contact_parts.append(f"visit their official site at {website}")
    if phone:
        contact_parts.append(f"call {phone}")
        
    if contact_parts:
        contact_sentence = "For inquiries or orders, customers can " + " or ".join(contact_parts) + "."
        sentences.append(contact_sentence)

    # Merge all sentences into a cohesive paragraph
    return " ".join(sentences)


# ==========================================
# 5. MAIN PROCESSING PIPELINE
# ==========================================

def generate_summary(
    shops_csv,
    hours_csv,
    services_csv,
    output_csv,
) -> dict:
    """Merge shops + hours + service options into titles and descriptions."""
    from pathlib import Path

    shops_csv = Path(shops_csv)
    hours_csv = Path(hours_csv)
    services_csv = Path(services_csv)
    output_csv = Path(output_csv)

    if not shops_csv.exists():
        raise FileNotFoundError(f"Shops CSV not found: {shops_csv}")

    df1 = pd.read_csv(shops_csv, encoding="utf-8-sig")
    df2 = (
        pd.read_csv(hours_csv, encoding="utf-8-sig")
        if hours_csv.exists()
        else pd.DataFrame()
    )
    df3 = (
        pd.read_csv(services_csv, encoding="utf-8-sig")
        if services_csv.exists()
        else pd.DataFrame()
    )

    name_col = "Name" if "Name" in df1.columns else "name"
    city_col = "City" if "City" in df1.columns else "city"
    df1["clean_name"] = df1[name_col].apply(clean_text)
    df1["clean_city"] = df1[city_col].apply(clean_text)

    if not df2.empty:
        name_col = "name" if "name" in df2.columns else "Name"
        city_col = "city" if "city" in df2.columns else "City"
        df2["clean_name"] = df2[name_col].apply(clean_text)
        df2["clean_city"] = df2[city_col].apply(clean_text) if city_col in df2.columns else ""
        df2_dedup = df2.drop_duplicates(subset=["clean_name", "clean_city"])
        merged_df = df1.merge(
            df2_dedup,
            on=["clean_name", "clean_city"],
            how="left",
            suffixes=("", "_csv2"),
        )
        if "hours_monday" in merged_df.columns:
            unmatched_mask = merged_df["hours_monday"].isna()
            if unmatched_mask.any():
                df2_name_only = df2.drop_duplicates(subset=["clean_name"])
                fallback_matches = df1.loc[unmatched_mask, ["clean_name"]].merge(
                    df2_name_only,
                    on="clean_name",
                    how="left",
                    suffixes=("", "_csv2"),
                )
                for col in df2.columns:
                    if col not in ["clean_name", "clean_city"] and col in merged_df.columns:
                        if col in fallback_matches.columns:
                            merged_df.loc[unmatched_mask, col] = fallback_matches[col].values
    else:
        merged_df = df1.copy()

    if not df3.empty:
        name_col = "name" if "name" in df3.columns else "Name"
        df3["clean_name"] = df3[name_col].apply(clean_text)
        df3_dedup = df3.drop_duplicates(subset=["clean_name"])
        merged_df = merged_df.merge(
            df3_dedup,
            on="clean_name",
            how="left",
            suffixes=("", "_csv3"),
        )

    merged_df["title"] = merged_df.apply(generate_shop_title, axis=1)
    merged_df["description"] = merged_df.apply(generate_organic_description, axis=1)

    helper_cols = {"clean_name", "clean_city"}
    base_columns = [c for c in df1.columns if c not in helper_cols]
    final_columns = base_columns + ["title", "description"]
    final_df = merged_df[final_columns]

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    final_df.to_csv(output_csv, index=False)
    message = (
        f"Summary CSV generated with {len(final_df)} listings -> {output_csv}"
    )
    print(message)
    return {
        "status": "completed",
        "output": str(output_csv),
        "rows": len(final_df),
        "message": message,
    }


def main():
    generate_summary("11.csv", "2.csv", "3.csv", "summary_shops_woid.csv")

if __name__ == "__main__":
    main()