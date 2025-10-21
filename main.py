"""
Voxmill Lead Intelligence Miner - Simple Version
Mines leads and writes to Google Sheet
"""
import asyncio
import aiohttp
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import re
from bs4 import BeautifulSoup
import os
from datetime import datetime

# ============================================
# CONFIGURATION
# ============================================
GOOGLE_PLACES_API = os.getenv('GOOGLE_PLACES_API', 'AIzaSyECVDGLffKyYBsn1U1aPs4nXubAtzA')
SERP_API = os.getenv('SERP_API', 'effe5fa5c5a4a81fffe1a32ea2a257f6a2097fc38ca5ca5d5a67bd29f7e0303d')
YELP_API = os.getenv('YELP_API', 'RP4QNPPXDJLioJAPyQcQ9hKnzsGZJ_PjpkYVcpokpE4nrqPElt4qhGk3GyuEcHiRPc2wE3gjtFG9rFV8WqR8fPYBcuqPJWaJdPTpjbcxmj')

# Google Sheet ID
SHEET_ID = '1JDtLSSf4bT_l4oMNps9y__M_GVmM7_BfWtyNdxsXF4o'

# Target cities
UK_CITIES = ['London', 'Manchester', 'Birmingham', 'Leeds', 'Edinburgh']
US_CITIES = ['New York', 'Los Angeles', 'Miami', 'Chicago', 'San Francisco']

# Voxmill high-ticket queries
VOXMILL_QUERIES = [
    'boutique real estate',
    'luxury property agent',
    'prestige car dealership',
    'luxury car sales',
    'yacht charter',
    'private jet charter',
    'boutique hotel',
    'luxury restaurant'
]

# Agency struggling-SMB queries
AGENCY_QUERIES = [
    'new restaurant',
    'small gym',
    'independent cafe',
    'local salon',
    'small hotel',
    'yoga studio'
]


# ============================================
# GOOGLE SHEETS CONNECTION
# ============================================
def connect_to_sheet():
    """Connect to Google Sheet using service account"""
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    
    # Render will set this as environment variable
    creds_json = os.getenv('GOOGLE_SHEETS_CREDENTIALS')
    
    if creds_json:
        import json
        creds_dict = json.loads(creds_json)
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    else:
        # Local development - use credentials.json file
        creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
    
    client = gspread.authorize(creds)
    sheet = client.open_by_key(SHEET_ID)
    return sheet


# ============================================
# LEAD MINING ENGINE
# ============================================
async def mine_leads(queries, cities, country, target_type):
    """Mine leads from Google Places"""
    all_leads = []
    seen_place_ids = set()
    
    async with aiohttp.ClientSession() as session:
        for city in cities:
            for query in queries:
                search_query = f"{query} in {city} {country}"
                print(f"🔍 Mining: {search_query}")
                
                # Search Google Places
                places = await search_google_places(session, search_query)
                
                for place in places:
                    place_id = place.get('place_id')
                    if place_id in seen_place_ids:
                        continue
                    seen_place_ids.add(place_id)
                    
                    # Get details
                    details = await get_place_details(session, place_id)
                    if not details:
                        continue
                    
                    # Build lead object
                    lead = await process_lead(session, details, query, city, country, target_type)
                    all_leads.append(lead)
                    
                    await asyncio.sleep(0.2)  # Rate limiting
                
                await asyncio.sleep(0.5)
    
    print(f"✅ Mined {len(all_leads)} unique leads")
    return all_leads


async def search_google_places(session, query):
    """Search Google Places API"""
    url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
    params = {'query': query, 'key': GOOGLE_PLACES_API}
    
    try:
        async with session.get(url, params=params) as response:
            data = await response.json()
            if data.get('status') == 'OK':
                return data.get('results', [])[:10]
    except Exception as e:
        print(f"❌ Google Places error: {e}")
    
    return []


async def get_place_details(session, place_id):
    """Get place details"""
    url = "https://maps.googleapis.com/maps/api/place/details/json"
    fields = "name,formatted_address,formatted_phone_number,website,rating,user_ratings_total,price_level,url"
    params = {'place_id': place_id, 'fields': fields, 'key': GOOGLE_PLACES_API}
    
    try:
        async with session.get(url, params=params) as response:
            data = await response.json()
            return data.get('result')
    except Exception as e:
        print(f"❌ Details error: {e}")
    
    return None


async def process_lead(session, details, category, city, country, target_type):
    """Process lead and add intelligence"""
    name = details.get('name', 'Unknown')
    address = details.get('formatted_address', '')
    phone = details.get('formatted_phone_number', 'Not listed')
    website = details.get('website', 'No website')
    rating = details.get('rating', 0)
    reviews = details.get('user_ratings_total', 0)
    maps_url = details.get('url', '')
    
    # Get additional intelligence
    email = await detect_email(session, website)
    socials = await find_socials(session, website, name)
    tech_stack = await analyze_website(session, website)
    
    # Calculate scores
    social_score = calculate_social_score(socials, website)
    priority = calculate_priority(reviews, rating, website, tech_stack, social_score, target_type)
    
    # Generate intelligence
    if target_type == 'voxmill':
        intel = generate_voxmill_intel(name, category, city, reviews, rating, website, tech_stack, social_score)
    else:
        intel = generate_agency_intel(name, category, city, reviews, rating, website, tech_stack, social_score)
    
    return {
        'name': name,
        'category': category,
        'address': address,
        'city': city,
        'country': country,
        'phone': phone,
        'email': email,
        'website': website,
        'maps_url': maps_url,
        'rating': rating,
        'reviews': reviews,
        'instagram': socials.get('instagram', ''),
        'facebook': socials.get('facebook', ''),
        'linkedin': socials.get('linkedin', ''),
        'tech_stack': tech_stack,
        'social_score': social_score,
        'priority': priority,
        **intel
    }


async def detect_email(session, website):
    """Detect email from website"""
    if not website or website == 'No website':
        return 'None'
    
    try:
        async with session.get(website, timeout=5) as response:
            html = await response.text()
            match = re.search(r'([a-zA-Z0-9._-]+@[a-zA-Z0-9._-]+\.[a-zA-Z0-9_-]+)', html)
            if match:
                return match.group(1)
    except:
        pass
    
    return 'Check website'


async def find_socials(session, website, business_name):
    """Find social media handles"""
    socials = {'instagram': '', 'facebook': '', 'linkedin': ''}
    
    if not website or website == 'No website':
        return socials
    
    try:
        async with session.get(website, timeout=5) as response:
            html = await response.text()
            
            ig = re.search(r'instagram\.com/([a-zA-Z0-9._]+)', html)
            if ig:
                socials['instagram'] = '@' + ig.group(1)
            
            fb = re.search(r'facebook\.com/([a-zA-Z0-9.]+)', html)
            if fb:
                socials['facebook'] = 'fb.com/' + fb.group(1)
            
            li = re.search(r'linkedin\.com/company/([a-zA-Z0-9-]+)', html)
            if li:
                socials['linkedin'] = 'linkedin.com/company/' + li.group(1)
    except:
        pass
    
    return socials


async def analyze_website(session, website):
    """Analyze website tech stack"""
    if not website or website == 'No website':
        return 'No website'
    
    try:
        async with session.get(website, timeout=5) as response:
            html = await response.text()
            
            if 'wix.com' in html:
                return 'Wix'
            elif 'wordpress' in html.lower():
                return 'WordPress'
            elif 'squarespace' in html.lower():
                return 'Squarespace'
            elif 'shopify' in html.lower():
                return 'Shopify'
            else:
                return 'Custom'
    except:
        return 'Unable to load'


def calculate_social_score(socials, website):
    """Calculate social presence score"""
    score = 0
    if socials.get('instagram') and '@' in socials['instagram']:
        score += 3
    if socials.get('facebook') and 'fb.com/' in socials['facebook']:
        score += 3
    if socials.get('linkedin'):
        score += 2
    if website and website != 'No website':
        score += 2
    return score


def calculate_priority(reviews, rating, website, tech_stack, social_score, target_type):
    """Calculate priority score 1-10"""
    score = 5
    
    if target_type == 'voxmill':
        if reviews > 100:
            score += 2
        if rating > 4.5:
            score += 1
        if tech_stack not in ['No website', 'Wix']:
            score += 1
        if social_score < 5:
            score += 1
    else:
        if reviews < 20:
            score += 2
        if rating < 4.0:
            score += 1
        if tech_stack in ['No website', 'Unable to load']:
            score += 2
        if social_score < 3:
            score += 1
    
    return min(max(score, 1), 10)


def generate_voxmill_intel(name, category, city, reviews, rating, website, tech_stack, social_score):
    """Generate Voxmill intelligence"""
    flaws = []
    if reviews < 50:
        flaws.append('Low market validation')
    if rating < 4.0:
        flaws.append('Reputation risk')
    if website == 'No website':
        flaws.append('No digital presence')
    if tech_stack == 'Wix':
        flaws.append('Amateur website')
    if social_score < 4:
        flaws.append('Weak social media')
    
    hook = f"I noticed {name} isn't ranking well for {category} searches in {city} - I can show you the exact competitor movements hurting your visibility."
    
    return {
        'flaws': ' | '.join(flaws) if flaws else 'None',
        'hook': hook
    }


def generate_agency_intel(name, category, city, reviews, rating, website, tech_stack, social_score):
    """Generate Agency intelligence"""
    red_flags = []
    if reviews < 10:
        red_flags.append('New/unproven')
    if rating < 3.5:
        red_flags.append('Struggling reputation')
    if website == 'No website':
        red_flags.append('No online presence')
    if social_score < 3:
        red_flags.append('Invisible on social')
    
    if website == 'No website':
        solution = f"I build websites + social automation for {category} businesses in {city} - £2k setup + £600/mo."
    else:
        solution = f"I automate marketing for {category} businesses - £600/mo, you focus on service delivery."
    
    return {
        'red_flags': ' | '.join(red_flags) if red_flags else 'None',
        'solution': solution
    }


# ============================================
# WRITE TO GOOGLE SHEETS
# ============================================
def write_to_sheet(leads, sheet_name):
    """Write leads to Google Sheet"""
    print(f"📊 Writing {len(leads)} leads to sheet '{sheet_name}'...")
    
    sheet = connect_to_sheet()
    
    # Get or create worksheet
    try:
        worksheet = sheet.worksheet(sheet_name)
        worksheet.clear()
    except:
        worksheet = sheet.add_worksheet(title=sheet_name, rows=1000, cols=20)
    
    # Headers
    headers = [
        'Name', 'Category', 'Address', 'City', 'Country', 'Phone', 'Email',
        'Website', 'Google Maps', 'Instagram', 'Facebook', 'LinkedIn',
        'Rating', 'Reviews', 'Tech Stack', 'Social Score', 'Priority',
        'Intelligence 1', 'Intelligence 2'
    ]
    
    # Build rows
    rows = [headers]
    for lead in leads:
        if sheet_name.startswith('VOXMILL'):
            intel1 = lead.get('flaws', '')
            intel2 = lead.get('hook', '')
        else:
            intel1 = lead.get('red_flags', '')
            intel2 = lead.get('solution', '')
        
        row = [
            lead.get('name', ''),
            lead.get('category', ''),
            lead.get('address', ''),
            lead.get('city', ''),
            lead.get('country', ''),
            lead.get('phone', ''),
            lead.get('email', ''),
            lead.get('website', ''),
            lead.get('maps_url', ''),
            lead.get('instagram', ''),
            lead.get('facebook', ''),
            lead.get('linkedin', ''),
            lead.get('rating', 0),
            lead.get('reviews', 0),
            lead.get('tech_stack', ''),
            lead.get('social_score', 0),
            lead.get('priority', 0),
            intel1,
            intel2
        ]
        rows.append(row)
    
    # Write all at once
    worksheet.update(rows, 'A1')
    
    # Format header
    worksheet.format('A1:S1', {
        'backgroundColor': {'red': 0, 'green': 0, 'blue': 0},
        'textFormat': {'foregroundColor': {'red': 1, 'green': 0.84, 'blue': 0}, 'bold': True}
    })
    
    print(f"✅ Sheet '{sheet_name}' updated!")


# ============================================
# MAIN EXECUTION
# ============================================
async def main():
    """Main execution"""
    print("🚀 Voxmill Lead Intelligence Miner Starting...\n")
    
    # Mine Voxmill leads (UK)
    print("🎯 Mining VOXMILL targets (UK)...")
    voxmill_uk = await mine_leads(VOXMILL_QUERIES, UK_CITIES, 'UK', 'voxmill')
    
    # Mine Voxmill leads (US)
    print("\n🎯 Mining VOXMILL targets (US)...")
    voxmill_us = await mine_leads(VOXMILL_QUERIES, US_CITIES, 'US', 'voxmill')
    
    # Mine Agency leads (UK)
    print("\n🎯 Mining AGENCY targets (UK)...")
    agency_uk = await mine_leads(AGENCY_QUERIES, UK_CITIES, 'UK', 'agency')
    
    # Mine Agency leads (US)
    print("\n🎯 Mining AGENCY targets (US)...")
    agency_us = await mine_leads(AGENCY_QUERIES, US_CITIES, 'US', 'agency')
    
    # Combine and write
    all_voxmill = voxmill_uk + voxmill_us
    all_agency = agency_uk + agency_us
    
    print(f"\n📊 RESULTS:")
    print(f"  Voxmill Targets: {len(all_voxmill)}")
    print(f"  Agency Targets: {len(all_agency)}")
    
    # Write to sheets
    write_to_sheet(all_voxmill, 'VOXMILL - High Ticket')
    write_to_sheet(all_agency, 'AGENCY - Struggling SMBs')
    
    print("\n✨ ALL DONE! Check your Google Sheet.")


if __name__ == "__main__":
    asyncio.run(main())
```

---

## **FILE 2: requirements.txt**
```
aiohttp==3.9.1
gspread==5.12.0
oauth2client==4.1.3
beautifulsoup4==4.12.2
lxml==4.9.3
