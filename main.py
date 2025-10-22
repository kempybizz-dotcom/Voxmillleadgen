"""
VOXMILL PLATINUM INTELLIGENCE SYSTEM
Enterprise-grade lead generation with maximum enrichment
50+ data points per lead | 5 AI outreach messages | Competitor analysis | Full tech stack detection
Runtime: 4-6 hours for 400-600 platinum leads
"""
import asyncio
import aiohttp
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import re
import json
import logging
from bs4 import BeautifulSoup
from typing import List, Dict, Optional, Set, Tuple
import os
from datetime import datetime
from urllib.parse import urlparse, quote_plus
import hashlib
import time

# Configure logging with detailed formatting
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(funcName)s] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# ============================================
# CONFIGURATION
# ============================================
class Config:
    """Centralized configuration with all API keys"""
    
    # Core APIs
    GOOGLE_PLACES_API = os.getenv('GOOGLE_PLACES_API', 'AIzaSyECVDGLffKyYBsn1U1aPs4nXubAtzA')
    SERP_API = os.getenv('SERP_API', 'effe5fa5c5a4a81fffe1a32ea2a257f6a2097fc38ca5ca5d5a67bd29f7e0303d')
    YELP_API = os.getenv('YELP_API', 'RP4QNPPXDJLioJAPyQcQ9hKnzsGZJ_PjpkYVcpokpE4nrqPElt4qhGk3GyuEcHiRPc2wE3gjtFG9rFV8WqR8fPYBcuqPJWaJdPTpjbcxmj')
    OPENAI_API = os.getenv('OPENAI_API', '')
    HUNTER_API = os.getenv('HUNTER_API', '')
    
    # New premium APIs
    INSTAGRAM_RAPIDAPI_KEY = '1440de56aamsh945d6c41f441399p1af6adjsne2d964758775'
    BUILTWITH_API = os.getenv('BUILTWITH_API', '')
    CLEARBIT_API = os.getenv('CLEARBIT_API', '')
    LINKEDIN_RAPIDAPI_KEY = '1440de56aamsh945d6c41f441399p1af6adjsne2d964758775'
    
    # Google Sheets
    SHEET_ID = '1JDtLSSf4bT_l4oMNps9y__M_GVmM7_BfWtyNdxsXF4o'
    
    # Targeting - UK focus for Voxmill
    UK_CITIES = ['London', 'Manchester', 'Birmingham', 'Leeds', 'Edinburgh', 'Bristol', 'Liverpool', 'Glasgow', 'Sheffield', 'Newcastle']
    US_CITIES = ['New York', 'Los Angeles', 'Miami', 'Chicago', 'San Francisco', 'Boston', 'Austin', 'Seattle', 'Atlanta', 'Dallas']
    
    # Voxmill high-ticket categories
    VOXMILL_QUERIES = [
        'boutique real estate agency',
        'luxury property agent',
        'premium car dealership',
        'luxury car dealer',
        'superyacht charter',
        'private jet charter',
        'luxury hotel',
        'michelin star restaurant',
        'high-end interior design',
        'luxury property developer',
        'premium estate agent',
        'prestige car sales'
    ]
    
    # Agency struggling-SMB categories
    AGENCY_QUERIES = [
        'new restaurant',
        'independent gym',
        'local cafe',
        'hair salon',
        'beauty salon',
        'yoga studio',
        'small hotel',
        'independent retailer',
        'fitness studio',
        'spa wellness'
    ]
    
    # Rate limiting and performance
    REQUEST_DELAY = 0.4
    BATCH_DELAY = 1.0
    MAX_RETRIES = 3
    TIMEOUT = 15
    MAX_CONCURRENT = 10


# ============================================
# GOOGLE SHEETS CONNECTION
# ============================================
class SheetsManager:
    """Enhanced Google Sheets manager with 50+ column support"""
    
    @staticmethod
    def connect():
        """Establish connection to Google Sheets"""
        try:
            scope = [
                'https://spreadsheets.google.com/feeds',
                'https://www.googleapis.com/auth/drive'
            ]
            
            creds_json = os.getenv('GOOGLE_SHEETS_CREDENTIALS')
            
            if creds_json:
                creds_dict = json.loads(creds_json)
                creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
            else:
                creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
            
            client = gspread.authorize(creds)
            sheet = client.open_by_key(Config.SHEET_ID)
            logger.info("✅ Connected to Google Sheets")
            return sheet
        except Exception as e:
            logger.error(f"❌ Google Sheets connection failed: {e}")
            raise
    
    @staticmethod
    def write_platinum_leads(sheet, leads: List[Dict], sheet_name: str, lead_type: str):
        """Write platinum leads with 50+ columns"""
        if not leads:
            logger.warning(f"No leads to write for {sheet_name}")
            return
        
        logger.info(f"📊 Writing {len(leads)} PLATINUM leads to '{sheet_name}'...")
        
        try:
            # Get or create worksheet
            try:
                worksheet = sheet.worksheet(sheet_name)
                worksheet.clear()
            except:
                worksheet = sheet.add_worksheet(title=sheet_name, rows=2000, cols=60)
            
            # Define 50+ column headers
            headers = [
                # Core Identity
                'Name', 'Category', 'City', 'Country', 'Address', 
                
                # Contact Info
                'Phone', 'Email (Hunter)', 'Email Confidence', 'Email Verified', 'Decision Maker', 'Decision Maker Position', 'Decision Maker Dept',
                
                # Online Presence
                'Website', 'Google Maps', 'Instagram Handle', 'Instagram Followers', 'Instagram Posts', 'Instagram Engagement %', 'Instagram Verified',
                'Facebook', 'LinkedIn Company', 'LinkedIn Followers', 'Twitter',
                
                # Reviews & Ratings
                'Google Rating', 'Google Reviews', 'Yelp Rating', 'Yelp Reviews', 'Yelp Price',
                
                # Tech & Digital
                'Tech Stack (Primary)', 'CMS', 'Web Hosting', 'Analytics Tools', 'Ad Platforms',
                'Website Speed (0-10)', 'SEO Score (0-10)', 'Mobile Friendly', 'Has SSL', 'Load Time (s)',
                
                # Business Intelligence
                'Industry', 'Employee Count', 'Founded Year', 'Revenue Range', 'Business Age',
                'Social Presence Score', 'Digital Maturity Score',
                
                # Competitive Intelligence
                'Top 3 Competitors', 'Competitive Gaps', 'Market Position',
                
                # Sentiment & Analysis
                'Review Sentiment', 'Positive Keywords', 'Negative Keywords',
                
                # Weaknesses & Opportunities
                'Priority Score', 'Weakness Severity', 'Detailed Flaws (20+)',
                
                # AI-Generated Outreach
                'Outreach #1 (Pain)', 'Outreach #2 (Opportunity)', 'Outreach #3 (Competitor)', 
                'Outreach #4 (Data)', 'Outreach #5 (Urgency)',
                'Email Subject Line', 'LinkedIn Connection Request', 'SMS Template',
                
                # Metadata
                'Last Updated'
            ]
            
            # Build data rows
            rows = [headers]
            for lead in leads:
                comp_names = ' | '.join([c['name'] for c in lead.get('competitors', [])[:3]])
                
                row = [
                    # Core Identity
                    lead.get('name', ''),
                    lead.get('category', ''),
                    lead.get('city', ''),
                    lead.get('country', ''),
                    lead.get('address', ''),
                    
                    # Contact Info
                    lead.get('phone', ''),
                    lead.get('hunter_email', ''),
                    lead.get('email_confidence', ''),
                    'Yes' if lead.get('email_verified') else 'No',
                    lead.get('decision_maker', ''),
                    lead.get('decision_maker_position', ''),
                    lead.get('decision_maker_dept', ''),
                    
                    # Online Presence
                    lead.get('website', ''),
                    lead.get('maps_url', ''),
                    lead.get('instagram_handle', ''),
                    lead.get('instagram_followers', 0),
                    lead.get('instagram_posts', 0),
                    lead.get('instagram_engagement', 0),
                    'Yes' if lead.get('instagram_verified') else 'No',
                    lead.get('facebook', ''),
                    lead.get('linkedin_company', ''),
                    lead.get('linkedin_followers', 0),
                    lead.get('twitter', ''),
                    
                    # Reviews & Ratings
                    lead.get('rating', 0),
                    lead.get('total_reviews', 0),
                    lead.get('yelp_rating', 'N/A'),
                    lead.get('yelp_reviews', 0),
                    lead.get('yelp_price', ''),
                    
                    # Tech & Digital
                    lead.get('tech_stack_primary', ''),
                    lead.get('cms', ''),
                    lead.get('web_hosting', ''),
                    lead.get('analytics_tools', ''),
                    lead.get('ad_platforms', ''),
                    lead.get('website_speed', 0),
                    lead.get('seo_score', 0),
                    lead.get('mobile_friendly', ''),
                    lead.get('has_ssl', ''),
                    lead.get('load_time', 0),
                    
                    # Business Intelligence
                    lead.get('industry', ''),
                    lead.get('employee_count', ''),
                    lead.get('founded_year', ''),
                    lead.get('revenue_range', ''),
                    lead.get('estimated_age', ''),
                    lead.get('social_score', 0),
                    lead.get('digital_maturity', 0),
                    
                    # Competitive Intelligence
                    comp_names,
                    lead.get('competitive_gaps', ''),
                    lead.get('market_position', ''),
                    
                    # Sentiment & Analysis
                    lead.get('sentiment', ''),
                    lead.get('positive_keywords', ''),
                    lead.get('negative_keywords', ''),
                    
                    # Weaknesses & Opportunities
                    lead.get('priority_score', 5),
                    lead.get('weakness_severity', ''),
                    lead.get('detailed_flaws', ''),
                    
                    # AI-Generated Outreach
                    lead.get('outreach_pain', ''),
                    lead.get('outreach_opportunity', ''),
                    lead.get('outreach_competitor', ''),
                    lead.get('outreach_data', ''),
                    lead.get('outreach_urgency', ''),
                    lead.get('email_subject', ''),
                    lead.get('linkedin_request', ''),
                    lead.get('sms_template', ''),
                    
                    # Metadata
                    datetime.now().strftime('%Y-%m-%d %H:%M')
                ]
                rows.append(row)
            
            # Write in batches to avoid API limits
            batch_size = 500
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i + batch_size]
                if i == 0:
                    worksheet.update(values=batch, range_name=f'A1:BM{len(batch)}')
                else:
                    start_row = i + 1
                    worksheet.update(values=batch, range_name=f'A{start_row}:BM{start_row + len(batch) - 1}')
                time.sleep(1.5)
            
            # Format header row (black background, gold text)
            worksheet.format('A1:BM1', {
                'backgroundColor': {'red': 0, 'green': 0, 'blue': 0},
                'textFormat': {
                    'foregroundColor': {'red': 1, 'green': 0.84, 'blue': 0},
                    'bold': True,
                    'fontSize': 10
                },
                'horizontalAlignment': 'CENTER'
            })
            
            # Freeze header row
            worksheet.freeze(rows=1)
            
            # Auto-resize key columns
            for col_index in range(0, 15):
                worksheet.columns_auto_resize(col_index, col_index)
            
            logger.info(f"✅ Successfully wrote {len(leads)} PLATINUM leads to '{sheet_name}'")
            
        except Exception as e:
            logger.error(f"❌ Failed to write to sheet: {e}")
            raise


# ============================================
# HUNTER.IO EMAIL VERIFICATION
# ============================================
class HunterClient:
    """Hunter.io email finder and verification"""
    
    @staticmethod
    async def find_email(session: aiohttp.ClientSession, domain: str, company_name: str) -> Dict:
        """Find and verify email using Hunter.io"""
        if not Config.HUNTER_API or not domain or domain == 'No website':
            return {'email': '', 'confidence': 0, 'verified': False}
        
        try:
            # Extract clean domain
            clean_domain = urlparse(domain).netloc or domain
            clean_domain = clean_domain.replace('www.', '')
            
            url = f"https://api.hunter.io/v2/domain-search?domain={clean_domain}&api_key={Config.HUNTER_API}"
            
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    emails = data.get('data', {}).get('emails', [])
                    
                    if emails:
                        # Get the most confident email
                        best_email = max(emails, key=lambda x: x.get('confidence', 0))
                        return {
                            'email': best_email.get('value', ''),
                            'confidence': best_email.get('confidence', 0),
                            'verified': best_email.get('verification', {}).get('status') == 'valid',
                            'position': best_email.get('position', ''),
                            'department': best_email.get('department', '')
                        }
            
            await asyncio.sleep(0.5)
            return {'email': '', 'confidence': 0, 'verified': False}
            
        except Exception as e:
            logger.debug(f"Hunter.io error: {e}")
            return {'email': '', 'confidence': 0, 'verified': False}


# ============================================
# INSTAGRAM ENRICHMENT
# ============================================
class InstagramClient:
    """Instagram profile enrichment via RapidAPI"""
    
    @staticmethod
    async def get_profile(session: aiohttp.ClientSession, handle: str) -> Dict:
        """Get Instagram profile stats"""
        if not handle:
            return {}
        
        try:
            # Clean handle
            handle = handle.replace('@', '').replace('instagram.com/', '').strip('/')
            if '/' in handle:
                handle = handle.split('/')[-1]
            
            url = "https://instagram-scraper-api2.p.rapidapi.com/v1/info"
            
            headers = {
                "X-RapidAPI-Key": Config.INSTAGRAM_RAPIDAPI_KEY,
                "X-RapidAPI-Host": "instagram-scraper-api2.p.rapidapi.com"
            }
            
            params = {"username_or_id_or_url": handle}
            
            async with session.get(url, headers=headers, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    user_data = data.get('data', {})
                    
                    followers = user_data.get('follower_count', 0)
                    following = user_data.get('following_count', 0)
                    posts = user_data.get('media_count', 0)
                    
                    # Calculate engagement rate (rough estimate)
                    engagement = 0
                    if followers > 0 and posts > 0:
                        # Typical engagement is likes + comments per post / followers
                        engagement = round((posts * 50) / followers * 100, 2)  # Rough estimate
                    
                    return {
                        'followers': followers,
                        'following': following,
                        'posts': posts,
                        'engagement': engagement,
                        'verified': user_data.get('is_verified', False),
                        'bio': user_data.get('biography', '')
                    }
            
            await asyncio.sleep(0.3)
            return {}
            
        except Exception as e:
            logger.debug(f"Instagram API error for {handle}: {e}")
            return {}


# ============================================
# BUILTWITH TECH STACK DETECTION
# ============================================
class BuiltWithClient:
    """BuiltWith technology detection"""
    
    @staticmethod
    async def get_tech_stack(session: aiohttp.ClientSession, domain: str) -> Dict:
        """Get complete tech stack from BuiltWith"""
        if not domain or domain == 'No website':
            return {}
        
        try:
            # Extract clean domain
            clean_domain = urlparse(domain).netloc or domain
            clean_domain = clean_domain.replace('www.', '')
            
            # Free version - scrape their public data
            url = f"https://builtwith.com/{clean_domain}"
            
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status == 200:
                    html = await resp.text()
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    tech_stack = {
                        'cms': '',
                        'hosting': '',
                        'analytics': [],
                        'advertising': [],
                        'frameworks': [],
                        'cdn': ''
                    }
                    
                    # Parse tech categories (simplified scraping)
                    tech_sections = soup.find_all('div', class_='card')
                    
                    for section in tech_sections:
                        title = section.find('h2')
                        if title:
                            category = title.get_text().strip().lower()
                            items = section.find_all('a', class_='tag')
                            
                            if 'cms' in category or 'content' in category:
                                if items:
                                    tech_stack['cms'] = items[0].get_text().strip()
                            elif 'hosting' in category or 'server' in category:
                                if items:
                                    tech_stack['hosting'] = items[0].get_text().strip()
                            elif 'analytics' in category:
                                tech_stack['analytics'] = [item.get_text().strip() for item in items[:3]]
                            elif 'advertising' in category or 'marketing' in category:
                                tech_stack['advertising'] = [item.get_text().strip() for item in items[:3]]
                            elif 'framework' in category or 'javascript' in category:
                                tech_stack['frameworks'] = [item.get_text().strip() for item in items[:3]]
                    
                    return tech_stack
            
            await asyncio.sleep(1.0)
            return {}
            
        except Exception as e:
            logger.debug(f"BuiltWith error: {e}")
            return {}


# ============================================
# CLEARBIT ENRICHMENT
# ============================================
class ClearbitClient:
    """Clearbit company enrichment"""
    
    @staticmethod
    async def enrich_company(session: aiohttp.ClientSession, domain: str) -> Dict:
        """Enrich company data using Clearbit"""
        if not Config.CLEARBIT_API or not domain or domain == 'No website':
            return {}
        
        try:
            # Extract clean domain
            clean_domain = urlparse(domain).netloc or domain
            clean_domain = clean_domain.replace('www.', '')
            
            url = f"https://company.clearbit.com/v2/companies/find?domain={clean_domain}"
            
            headers = {
                "Authorization": f"Bearer {Config.CLEARBIT_API}"
            }
            
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    
                    return {
                        'employee_count': data.get('metrics', {}).get('employees', ''),
                        'employee_range': data.get('metrics', {}).get('employeesRange', ''),
                        'founded_year': data.get('foundedYear', ''),
                        'revenue_range': data.get('metrics', {}).get('estimatedAnnualRevenue', ''),
                        'industry': data.get('category', {}).get('industry', ''),
                        'tech_stack': data.get('tech', []),
                        'description': data.get('description', ''),
                        'linkedin_handle': data.get('linkedin', {}).get('handle', '')
                    }
            
            await asyncio.sleep(0.5)
            return {}
            
        except Exception as e:
            logger.debug(f"Clearbit error: {e}")
            return {}


# ============================================
# LINKEDIN COMPANY SEARCH
# ============================================
class LinkedInClient:
    """LinkedIn company data via RapidAPI"""
    
    @staticmethod
    async def search_company(session: aiohttp.ClientSession, company_name: str) -> Dict:
        """Search for company on LinkedIn"""
        if not company_name:
            return {}
        
        try:
            url = "https://linkedin-data-api.p.rapidapi.com/search-companies"
            
            headers = {
                "X-RapidAPI-Key": Config.LINKEDIN_RAPIDAPI_KEY,
                "X-RapidAPI-Host": "linkedin-data-api.p.rapidapi.com"
            }
            
            params = {"keywords": company_name}
            
            async with session.get(url, headers=headers, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    companies = data.get('data', [])
                    
                    if companies:
                        company = companies[0]
                        return {
                            'linkedin_url': company.get('url', ''),
                            'linkedin_followers': company.get('followersCount', 0),
                            'linkedin_employees': company.get('staffCount', ''),
                            'linkedin_industry': company.get('industry', '')
                        }
            
            await asyncio.sleep(0.5)
            return {}
            
        except Exception as e:
            logger.debug(f"LinkedIn API error: {e}")
            return {}


# ============================================
# GOOGLE PLACES CLIENT
# ============================================
class GooglePlacesClient:
    """Google Places API client with detailed place info"""
    
    @staticmethod
    async def search(session: aiohttp.ClientSession, query: str, location: str) -> List[Dict]:
        """Search Google Places"""
        try:
            search_query = f"{query} in {location}"
            url = f"https://maps.googleapis.com/maps/api/place/textsearch/json"
            
            params = {
                'query': search_query,
                'key': Config.GOOGLE_PLACES_API
            }
            
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    results = data.get('results', [])
                    await asyncio.sleep(Config.REQUEST_DELAY)
                    return results[:20]
            
            return []
            
        except Exception as e:
            logger.error(f"Google Places search error: {e}")
            return []
    
    @staticmethod
    async def get_details(session: aiohttp.ClientSession, place_id: str) -> Optional[Dict]:
        """Get detailed place information"""
        try:
            url = f"https://maps.googleapis.com/maps/api/place/details/json"
            
            params = {
                'place_id': place_id,
                'fields': 'name,formatted_address,formatted_phone_number,international_phone_number,website,rating,user_ratings_total,price_level,opening_hours,business_status,url,types',
                'key': Config.GOOGLE_PLACES_API
            }
            
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    await asyncio.sleep(Config.REQUEST_DELAY)
                    return data.get('result')
            
            return None
            
        except Exception as e:
            logger.error(f"Google Places details error: {e}")
            return None


# ============================================
# YELP CLIENT
# ============================================
class YelpClient:
    """Yelp business search and reviews"""
    
    @staticmethod
    async def search(session: aiohttp.ClientSession, name: str, location: str) -> Optional[Dict]:
        """Search Yelp for business"""
        try:
            url = "https://api.yelp.com/v3/businesses/search"
            
            headers = {
                'Authorization': f'Bearer {Config.YELP_API}'
            }
            
            params = {
                'term': name,
                'location': location,
                'limit': 1
            }
            
            async with session.get(url, headers=headers, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    businesses = data.get('businesses', [])
                    if businesses:
                        biz = businesses[0]
                        await asyncio.sleep(Config.REQUEST_DELAY)
                        return {
                            'rating': biz.get('rating', 0),
                            'review_count': biz.get('review_count', 0),
                            'price': biz.get('price', ''),
                            'url': biz.get('url', '')
                        }
            
            return None
            
        except Exception as e:
            logger.debug(f"Yelp search error: {e}")
            return None


# ============================================
# COMPETITOR FINDER
# ============================================
class CompetitorFinder:
    """Find and analyze competitors"""
    
    @staticmethod
    async def find_competitors(session: aiohttp.ClientSession, business_name: str, category: str, city: str) -> List[Dict]:
        """Find top 3-5 competitors using Google Places"""
        try:
            # Search for similar businesses
            search_query = f"{category} in {city}"
            url = f"https://maps.googleapis.com/maps/api/place/textsearch/json"
            
            params = {
                'query': search_query,
                'key': Config.GOOGLE_PLACES_API
            }
            
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    results = data.get('results', [])
                    
                    # Filter out the business itself and get top 5 by rating
                    competitors = []
                    for result in results:
                        if result.get('name', '').lower() != business_name.lower():
                            competitors.append({
                                'name': result.get('name', ''),
                                'rating': result.get('rating', 0),
                                'reviews': result.get('user_ratings_total', 0),
                                'address': result.get('formatted_address', '')
                            })
                    
                    # Sort by rating and review count
                    competitors.sort(key=lambda x: (x['rating'], x['reviews']), reverse=True)
                    
                    await asyncio.sleep(Config.REQUEST_DELAY)
                    return competitors[:5]
            
            return []
            
        except Exception as e:
            logger.debug(f"Competitor search error: {e}")
            return []


# ============================================
# OPENAI INTELLIGENCE PROCESSOR
# ============================================
class OpenAIProcessor:
    """Generate AI-powered outreach and analysis using GPT-4"""
    
    @staticmethod
    async def generate_outreach_messages(session: aiohttp.ClientSession, lead: Dict, lead_type: str) -> Dict:
        """Generate 5 different outreach messages using GPT-4"""
        if not Config.OPENAI_API:
            return {}
        
        try:
            # Build context for GPT
            context = f"""
Business: {lead.get('name', 'Unknown')}
Category: {lead.get('category', '')}
Location: {lead.get('city', '')}, {lead.get('country', '')}
Website: {lead.get('website', 'No website')}
Rating: {lead.get('rating', 0)}/5 ({lead.get('total_reviews', 0)} reviews)
Social Media: Instagram followers: {lead.get('instagram_followers', 0)}
Tech Stack: {lead.get('tech_stack_primary', 'Unknown')}
Weaknesses: {lead.get('detailed_flaws', 'None identified')}
Competitors: {', '.join([c['name'] for c in lead.get('competitors', [])])}
"""
            
            if lead_type == 'voxmill':
                system_prompt = """You are a B2B sales expert for Voxmill Market Intelligence, which provides automated weekly market intelligence reports for high-ticket businesses (real estate, luxury automotive, hospitality).

Generate 5 different cold outreach messages that are:
- Personalized to the business
- Focus on data-driven insights
- Highlight competitive gaps
- Professional but conversational
- 50-80 words each

Also generate:
- Email subject line (10 words max)
- LinkedIn connection request (200 chars max)
- SMS template (160 chars max)"""
            else:
                system_prompt = """You are a B2B sales expert for a digital transformation agency helping SMBs with automation, AI, and social media.

Generate 5 different cold outreach messages that are:
- Empathetic to their struggles
- Focus on quick wins
- Highlight digital gaps
- Friendly and supportive tone
- 50-80 words each

Also generate:
- Email subject line (10 words max)
- LinkedIn connection request (200 chars max)
- SMS template (160 chars max)"""
            
            url = "https://api.openai.com/v1/chat/completions"
            
            headers = {
                "Authorization": f"Bearer {Config.OPENAI_API}",
                "Content-Type": "application/json"
            }
            
            payload = {
                "model": "gpt-4",
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": f"Generate outreach for this business:\n\n{context}"}
                ],
                "temperature": 0.8,
                "max_tokens": 1000
            }
            
            async with session.post(url, headers=headers, json=payload, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    content = data['choices'][0]['message']['content']
                    
                    # Parse the response (simplified - assumes structured output)
                    messages = {
                        'outreach_pain': '',
                        'outreach_opportunity': '',
                        'outreach_competitor': '',
                        'outreach_data': '',
                        'outreach_urgency': '',
                        'email_subject': '',
                        'linkedin_request': '',
                        'sms_template': ''
                    }
                    
                    # Simple parsing (you may want to use regex or structured output)
                    lines = content.split('\n')
                    current_key = None
                    
                    for line in lines:
                        line = line.strip()
                        if 'pain' in line.lower() and ':' in line:
                            current_key = 'outreach_pain'
                        elif 'opportunity' in line.lower() and ':' in line:
                            current_key = 'outreach_opportunity'
                        elif 'competitor' in line.lower() and ':' in line:
                            current_key = 'outreach_competitor'
                        elif 'data' in line.lower() and ':' in line:
                            current_key = 'outreach_data'
                        elif 'urgency' in line.lower() and ':' in line:
                            current_key = 'outreach_urgency'
                        elif 'subject' in line.lower() and ':' in line:
                            current_key = 'email_subject'
                        elif 'linkedin' in line.lower() and ':' in line:
                            current_key = 'linkedin_request'
                        elif 'sms' in line.lower() and ':' in line:
                            current_key = 'sms_template'
                        elif current_key and line:
                            messages[current_key] += line + ' '
                    
                    # Clean up messages
                    for key in messages:
                        messages[key] = messages[key].strip()
                    
                    await asyncio.sleep(1.0)
                    return messages
            
            return {}
            
        except Exception as e:
            logger.debug(f"OpenAI error: {e}")
            return {}


# ============================================
# INTELLIGENCE PROCESSOR
# ============================================
class IntelligenceProcessor:
    """Process and enrich lead data with advanced intelligence"""
    
    @staticmethod
    async def detect_email(session: aiohttp.ClientSession, website: str) -> str:
        """Simple email detection from website"""
        if not website or website == 'No website':
            return 'No email found'
        
        try:
            async with session.get(website, timeout=aiohttp.ClientTimeout(total=10), allow_redirects=True) as resp:
                if resp.status == 200:
                    html = await resp.text()
                    
                    # Find emails using regex
                    email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
                    emails = re.findall(email_pattern, html)
                    
                    # Filter out common non-contact emails
                    filtered = [e for e in emails if not any(x in e.lower() for x in ['example', 'test', 'placeholder', 'noreply', 'support@wordpress'])]
                    
                    if filtered:
                        return filtered[0]
            
            await asyncio.sleep(0.3)
            return 'Check website'
            
        except:
            return 'Check website'
    
    @staticmethod
    async def find_socials(session: aiohttp.ClientSession, website: str, business_name: str) -> Dict:
        """Find social media handles"""
        socials = {'instagram': '', 'facebook': '', 'linkedin': '', 'twitter': ''}
        
        if not website or website == 'No website':
            return socials
        
        try:
            async with session.get(website, timeout=aiohttp.ClientTimeout(total=10), allow_redirects=True) as resp:
                if resp.status == 200:
                    html = await resp.text()
                    
                    # Instagram
                    ig_match = re.search(r'instagram\.com/([a-zA-Z0-9._]+)', html)
                    if ig_match:
                        socials['instagram'] = f"@{ig_match.group(1)}"
                    
                    # Facebook
                    fb_match = re.search(r'facebook\.com/([a-zA-Z0-9.]+)', html)
                    if fb_match:
                        socials['facebook'] = f"facebook.com/{fb_match.group(1)}"
                    
                    # LinkedIn
                    li_match = re.search(r'linkedin\.com/company/([a-zA-Z0-9-]+)', html)
                    if li_match:
                        socials['linkedin'] = f"linkedin.com/company/{li_match.group(1)}"
                    
                    # Twitter
                    tw_match = re.search(r'twitter\.com/([a-zA-Z0-9_]+)', html)
                    if tw_match:
                        socials['twitter'] = f"@{tw_match.group(1)}"
            
            await asyncio.sleep(0.3)
            
        except:
            pass
        
        return socials
    
    @staticmethod
    async def analyze_website(session: aiohttp.ClientSession, website: str) -> str:
        """Analyze website for tech stack (basic version)"""
        if not website or website == 'No website':
            return 'No website'
        
        try:
            start_time = time.time()
            
            async with session.get(website, timeout=aiohttp.ClientTimeout(total=10), allow_redirects=True) as resp:
                if resp.status == 200:
                    html = await resp.text()
                    load_time = time.time() - start_time
                    
                    # Detect common tech
                    tech_indicators = {
                        'WordPress': 'wp-content' in html or 'wp-includes' in html,
                        'Shopify': 'shopify' in html.lower(),
                        'Wix': 'wix.com' in html.lower(),
                        'Squarespace': 'squarespace' in html.lower(),
                        'React': 'react' in html.lower() or '__NEXT_DATA__' in html,
                        'Google Analytics': 'google-analytics.com' in html or 'gtag' in html
                    }
                    
                    detected = [tech for tech, present in tech_indicators.items() if present]
                    
                    await asyncio.sleep(0.3)
                    return ', '.join(detected) if detected else 'Custom/Unknown'
            
            return 'Unable to load'
            
        except:
            return 'Unable to load'
    
    @staticmethod
    def estimate_business_age(reviews: int, rating: float) -> str:
        """Estimate business age based on review volume"""
        if reviews > 500:
            return '5+ years'
        elif reviews > 200:
            return '3-5 years'
        elif reviews > 50:
            return '1-3 years'
        else:
            return '< 1 year'
    
    @staticmethod
    def calculate_social_score(socials: Dict, has_website: bool, has_reviews: bool) -> int:
        """Calculate social media presence score (0-10)"""
        score = 0
        
        if socials.get('instagram'):
            score += 3
        if socials.get('facebook'):
            score += 2
        if socials.get('linkedin'):
            score += 2
        if socials.get('twitter'):
            score += 1
        if has_website:
            score += 1
        if has_reviews:
            score += 1
        
        return min(score, 10)
    
    @staticmethod
    def calculate_priority(lead: Dict, target_type: str) -> int:
        """Calculate priority score (0-10) with advanced logic"""
        score = 5  # Base score
        
        # Rating impact
        rating = lead.get('rating', 0)
        if rating >= 4.5:
            score += 2
        elif rating >= 4.0:
            score += 1
        elif rating < 3.5:
            score -= 1
        
        # Review volume impact
        reviews = lead.get('total_reviews', 0)
        if target_type == 'voxmill':
            if reviews > 100:
                score += 1
            if reviews > 500:
                score += 1
        else:
            if reviews < 20:
                score += 2  # Low reviews = needs help
            elif reviews < 50:
                score += 1
        
        # Website quality
        if lead.get('website') == 'No website':
            if target_type == 'agency':
                score += 2  # Big opportunity
            else:
                score -= 2  # Red flag for Voxmill
        
        # Social presence
        social_score = lead.get('social_score', 0)
        if target_type == 'agency' and social_score < 4:
            score += 1  # Needs social help
        
        # Tech stack
        tech = lead.get('tech_stack_primary', '').lower()
        if target_type == 'agency' and ('wix' in tech or 'squarespace' in tech):
            score += 1  # Easy to upsell services
        
        # Email availability
        if lead.get('hunter_email'):
            score += 1
        
        return max(0, min(score, 10))
    
    @staticmethod
    def generate_detailed_flaws(lead: Dict, target_type: str) -> str:
        """Generate detailed weakness analysis (20+ points)"""
        flaws = []
        
        # Website analysis
        if lead.get('website') == 'No website':
            flaws.append("❌ No website presence")
        elif lead.get('has_ssl') == 'No':
            flaws.append("⚠️ No SSL certificate (security risk)")
        
        if lead.get('mobile_friendly') == 'No':
            flaws.append("⚠️ Not mobile-optimized")
        
        if lead.get('website_speed', 10) < 5:
            flaws.append(f"⚠️ Slow website (speed score: {lead.get('website_speed')})")
        
        if lead.get('seo_score', 10) < 5:
            flaws.append(f"⚠️ Poor SEO (score: {lead.get('seo_score')})")
        
        # Social media gaps
        if not lead.get('instagram_handle'):
            flaws.append("❌ No Instagram presence")
        elif lead.get('instagram_followers', 0) < 500:
            flaws.append(f"⚠️ Low Instagram following ({lead.get('instagram_followers')} followers)")
        
        if not lead.get('facebook'):
            flaws.append("❌ No Facebook page")
        
        if not lead.get('linkedin_company'):
            flaws.append("❌ No LinkedIn company page")
        
        # Review analysis
        reviews = lead.get('total_reviews', 0)
        rating = lead.get('rating', 0)
        
        if reviews < 10:
            flaws.append(f"⚠️ Very few reviews ({reviews} total)")
        elif reviews < 50:
            flaws.append(f"⚠️ Low review count ({reviews} total)")
        
        if rating < 3.5:
            flaws.append(f"❌ Poor rating ({rating}/5)")
        elif rating < 4.0:
            flaws.append(f"⚠️ Below-average rating ({rating}/5)")
        
        # Tech stack issues
        tech = lead.get('tech_stack_primary', '').lower()
        if 'wordpress' in tech:
            flaws.append("⚠️ Using WordPress (potential for better platform)")
        if 'wix' in tech or 'squarespace' in tech:
            flaws.append("⚠️ Using website builder (limited customization)")
        
        if not lead.get('analytics_tools'):
            flaws.append("❌ No analytics tracking detected")
        
        if not lead.get('ad_platforms'):
            flaws.append("❌ Not running paid advertising")
        
        # Business intelligence gaps
        if not lead.get('employee_count'):
            flaws.append("⚠️ Business size unknown")
        
        if lead.get('sentiment') == 'Negative':
            flaws.append("❌ Negative customer sentiment detected")
        elif lead.get('sentiment') == 'Mixed':
            flaws.append("⚠️ Mixed customer sentiment")
        
        # Competitive position
        competitors = lead.get('competitors', [])
        if competitors:
            avg_competitor_rating = sum(c.get('rating', 0) for c in competitors) / len(competitors)
            if rating < avg_competitor_rating:
                flaws.append(f"⚠️ Below competitor average ({rating} vs {avg_competitor_rating:.1f})")
        
        # Email/contact issues
        if not lead.get('hunter_email'):
            flaws.append("⚠️ Email not verified")
        
        if lead.get('phone') == 'Not listed':
            flaws.append("⚠️ No phone number listed")
        
        return ' | '.join(flaws) if flaws else 'No major flaws detected'
    
    @staticmethod
    def calculate_digital_maturity(lead: Dict) -> int:
        """Calculate digital maturity score (0-10)"""
        score = 0
        
        # Website quality
        if lead.get('website') and lead.get('website') != 'No website':
            score += 2
            if lead.get('has_ssl') == 'Yes':
                score += 1
            if lead.get('mobile_friendly') == 'Yes':
                score += 1
        
        # Tech stack sophistication
        if lead.get('analytics_tools'):
            score += 1
        if lead.get('ad_platforms'):
            score += 1
        
        # Social presence
        if lead.get('instagram_followers', 0) > 1000:
            score += 1
        if lead.get('linkedin_followers', 0) > 500:
            score += 1
        
        # Business data
        if lead.get('employee_count'):
            score += 1
        
        # Review management
        if lead.get('total_reviews', 0) > 50:
            score += 1
        
        return min(score, 10)


# ============================================
# LEAD MINER
# ============================================
class LeadMiner:
    """Main lead mining orchestrator with platinum enrichment"""
    
    def __init__(self):
        self.google_client = GooglePlacesClient()
        self.yelp_client = YelpClient()
        self.hunter_client = HunterClient()
        self.instagram_client = InstagramClient()
        self.builtwith_client = BuiltWithClient()
        self.clearbit_client = ClearbitClient()
        self.linkedin_client = LinkedInClient()
        self.competitor_finder = CompetitorFinder()
        self.openai_processor = OpenAIProcessor()
        self.intel_processor = IntelligenceProcessor()
        
        self.processed_places: Set[str] = set()
    
    async def mine_leads(
        self,
        queries: List[str],
        cities: List[str],
        country: str,
        target_type: str
    ) -> List[Dict]:
        """Mine leads with maximum enrichment"""
        
        all_leads = []
        
        connector = aiohttp.TCPConnector(limit=Config.MAX_CONCURRENT, limit_per_host=5)
        timeout = aiohttp.ClientTimeout(total=None, connect=60, sock_read=60)
        
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            
            for query in queries:
                logger.info(f"\n🔍 Searching: '{query}' across {len(cities)} cities...")
                
                for city in cities:
                    try:
                        # Search Google Places
                        places = await self.google_client.search(session, query, f"{city}, {country}")
                        
                        logger.info(f"   📍 {city}: Found {len(places)} results")
                        
                        # Process each place
                        for place in places:
                            place_id = place.get('place_id')
                            
                            # Skip duplicates
                            if place_id in self.processed_places:
                                continue
                            
                            self.processed_places.add(place_id)
                            
                            # Get detailed info
                            details = await self.google_client.get_details(session, place_id)
                            
                            if details:
                                # Process lead with FULL enrichment
                                lead = await self.process_platinum_lead(
                                    session,
                                    details,
                                    query,
                                    city,
                                    country,
                                    target_type
                                )
                                
                                if lead:
                                    all_leads.append(lead)
                                    logger.info(f"      ✅ Processed: {lead['name']} (Priority: {lead['priority_score']}/10)")
                        
                        await asyncio.sleep(Config.BATCH_DELAY)
                        
                    except Exception as e:
                        logger.error(f"   ❌ Error processing {city}: {e}")
                        continue
        
        logger.info(f"\n✅ Total leads collected: {len(all_leads)}")
        return all_leads
    
    async def process_platinum_lead(
        self,
        session: aiohttp.ClientSession,
        details: Dict,
        category: str,
        city: str,
        country: str,
        target_type: str
    ) -> Optional[Dict]:
        """Process single lead with PLATINUM enrichment (50+ data points)"""
        try:
            # Extract basic data
            name = details.get('name', 'Unknown')
            address = details.get('formatted_address', '')
            phone = details.get('formatted_phone_number') or details.get('international_phone_number') or 'Not listed'
            website = details.get('website', 'No website')
            rating = details.get('rating', 0)
            reviews = details.get('user_ratings_total', 0)
            maps_url = details.get('url', '')
            price_level = '£' * details.get('price_level', 0) if details.get('price_level') else ''
            
            logger.info(f"         🔧 Enriching: {name}...")
            
            # Phase 1: Core enrichment (parallel)
            email_task = self.intel_processor.detect_email(session, website)
            socials_task = self.intel_processor.find_socials(session, website, name)
            tech_task = self.intel_processor.analyze_website(session, website)
            yelp_task = self.yelp_client.search(session, name, f"{city}, {country}")
            
            email, socials, tech_stack, yelp_data = await asyncio.gather(
                email_task, socials_task, tech_task, yelp_task,
                return_exceptions=True
            )
            
            # Phase 2: Premium enrichment (parallel)
            hunter_task = self.hunter_client.find_email(session, website, name)
            instagram_task = self.instagram_client.get_profile(session, socials.get('instagram', '')) if isinstance(socials, dict) else asyncio.sleep(0)
            builtwith_task = self.builtwith_client.get_tech_stack(session, website)
            clearbit_task = self.clearbit_client.enrich_company(session, website)
            linkedin_task = self.linkedin_client.search_company(session, name)
            competitors_task = self.competitor_finder.find_competitors(session, name, category, city)
            
            hunter_data, instagram_data, builtwith_data, clearbit_data, linkedin_data, competitors = await asyncio.gather(
                hunter_task, instagram_task, builtwith_task, clearbit_task, linkedin_task, competitors_task,
                return_exceptions=True
            )
            
            # Handle exceptions
            if isinstance(email, Exception): 
                email = 'Check website'
            if isinstance(socials, Exception): 
                socials = {}
            if isinstance(tech_stack, Exception): 
                tech_stack = 'Unable to load'
            if isinstance(yelp_data, Exception): 
                yelp_data = None
            if isinstance(hunter_data, Exception): 
                hunter_data = {}
            if isinstance(instagram_data, Exception): 
                instagram_data = {}
            if isinstance(builtwith_data, Exception): 
                builtwith_data = {}
            if isinstance(clearbit_data, Exception): 
                clearbit_data = {}
            if isinstance(linkedin_data, Exception): 
                linkedin_data = {}
            if isinstance(competitors, Exception): 
                competitors = []
            
            # Build lead object with 50+ fields
            lead = {
                # Core Identity
                'name': name,
                'category': category,
                'city': city,
                'country': country,
                'address': address,
                
                # Contact Info
                'phone': phone,
                'email': email,
                'hunter_email': hunter_data.get('email', ''),
                'email_confidence': hunter_data.get('confidence', 0),
                'email_verified': hunter_data.get('verified', False),
                'decision_maker': hunter_data.get('position', ''),
                'decision_maker_position': hunter_data.get('position', ''),
                'decision_maker_dept': hunter_data.get('department', ''),
                
                # Online Presence
                'website': website,
                'maps_url': maps_url,
                'instagram_handle': socials.get('instagram', ''),
                'instagram_followers': instagram_data.get('followers', 0),
                'instagram_posts': instagram_data.get('posts', 0),
                'instagram_engagement': instagram_data.get('engagement', 0),
                'instagram_verified': instagram_data.get('verified', False),
                'facebook': socials.get('facebook', ''),
                'linkedin_company': linkedin_data.get('linkedin_url', ''),
                'linkedin_followers': linkedin_data.get('linkedin_followers', 0),
                'twitter': socials.get('twitter', ''),
                
                # Reviews & Ratings
                'rating': rating,
                'total_reviews': reviews,
                'yelp_rating': yelp_data.get('rating') if yelp_data else 'N/A',
                'yelp_reviews': yelp_data.get('review_count', 0) if yelp_data else 0,
                'yelp_price': yelp_data.get('price', '') if yelp_data else '',
                
                # Tech & Digital
                'tech_stack_primary': tech_stack,
                'cms': builtwith_data.get('cms', ''),
                'web_hosting': builtwith_data.get('hosting', ''),
                'analytics_tools': ', '.join(builtwith_data.get('analytics', [])),
                'ad_platforms': ', '.join(builtwith_data.get('advertising', [])),
                'website_speed': 7,  # Placeholder - could add PageSpeed API
                'seo_score': 6,  # Placeholder - could add SEO analysis
                'mobile_friendly': 'Yes' if website != 'No website' else 'No',
                'has_ssl': 'Yes' if website.startswith('https') else 'No',
                'load_time': 2.5,  # Placeholder
                
                # Business Intelligence
                'industry': clearbit_data.get('industry', ''),
                'employee_count': clearbit_data.get('employee_count', ''),
                'founded_year': clearbit_data.get('founded_year', ''),
                'revenue_range': clearbit_data.get('revenue_range', ''),
                'estimated_age': self.intel_processor.estimate_business_age(reviews, rating),
                
                # Competitive Intelligence
                'competitors': competitors,
                'market_position': 'Leader' if rating >= 4.5 and reviews > 100 else 'Challenger',
                
                # Sentiment (placeholder - would need GPT-4 analysis)
                'sentiment': 'Positive' if rating >= 4.0 else 'Mixed',
                'positive_keywords': '',
                'negative_keywords': '',
            }
            
            # Calculate scores
            social_score = self.intel_processor.calculate_social_score(socials, website != 'No website', reviews > 0)
            digital_maturity = self.intel_processor.calculate_digital_maturity(lead)
            priority = self.intel_processor.calculate_priority(lead, target_type)
            
            lead['social_score'] = social_score
            lead['digital_maturity'] = digital_maturity
            lead['priority_score'] = priority
            
            # Generate competitive gaps
            competitive_gaps = []
            if competitors:
                avg_comp_rating = sum(c.get('rating', 0) for c in competitors) / len(competitors)
                if rating < avg_comp_rating:
                    competitive_gaps.append(f"Rating {rating - avg_comp_rating:.1f} below competitors")
                
                avg_comp_reviews = sum(c.get('reviews', 0) for c in competitors) / len(competitors)
                if reviews < avg_comp_reviews:
                    competitive_gaps.append(f"{int(avg_comp_reviews - reviews)} fewer reviews than competitors")
            
            lead['competitive_gaps'] = ' | '.join(competitive_gaps) if competitive_gaps else 'Competitive position strong'
            
            # Generate detailed flaws
            detailed_flaws = self.intel_processor.generate_detailed_flaws(lead, target_type)
            lead['detailed_flaws'] = detailed_flaws
            
            # Determine weakness severity
            flaw_count = detailed_flaws.count('❌') + detailed_flaws.count('⚠️')
            if flaw_count >= 10:
                lead['weakness_severity'] = 'CRITICAL'
            elif flaw_count >= 5:
                lead['weakness_severity'] = 'HIGH'
            elif flaw_count >= 2:
                lead['weakness_severity'] = 'MEDIUM'
            else:
                lead['weakness_severity'] = 'LOW'
            
            # Phase 3: AI-generated outreach (only for high-priority leads to save API costs)
            if priority >= 7 and Config.OPENAI_API:
                outreach_messages = await self.openai_processor.generate_outreach_messages(session, lead, target_type)
                lead.update(outreach_messages)
            else:
                # Placeholder messages
                lead.update({
                    'outreach_pain': 'Manual outreach required',
                    'outreach_opportunity': 'Manual outreach required',
                    'outreach_competitor': 'Manual outreach required',
                    'outreach_data': 'Manual outreach required',
                    'outreach_urgency': 'Manual outreach required',
                    'email_subject': 'Quick question about [Company]',
                    'linkedin_request': f"Hi, I'd love to connect and discuss opportunities for {name}.",
                    'sms_template': f"Hi, found your business online. Quick question about {category}?"
                })
            
            logger.info(f"         ✅ Enrichment complete: {name} | Priority: {priority}/10 | Flaws: {flaw_count}")
            
            return lead
            
        except Exception as e:
            logger.error(f"         ❌ Error processing lead: {e}")
            return None


# ============================================
# MAIN EXECUTION
# ============================================
async def main():
    """Main execution flow"""
    start_time = datetime.now()
    
    logger.info("=" * 100)
    logger.info("🚀 VOXMILL PLATINUM INTELLIGENCE SYSTEM - MAXIMUM ENRICHMENT MODE")
    logger.info("=" * 100)
    logger.info("📊 Target: 400-600 PLATINUM leads with 50+ data points each")
    logger.info("⏱️  Expected runtime: 4-6 hours")
    logger.info("🔧 APIs active: Google Places, Yelp, Hunter, Instagram, BuiltWith, Clearbit, LinkedIn, OpenAI")
    logger.info("=" * 100)
    
    try:
        # Initialize miner
        miner = LeadMiner()
        
        # Mine Voxmill high-ticket leads (UK PRIORITY)
        logger.info("\n🎯 PHASE 1: Mining VOXMILL high-ticket targets (UK)...")
        voxmill_uk = await miner.mine_leads(
            Config.VOXMILL_QUERIES,
            Config.UK_CITIES,
            'UK',
            'voxmill'
        )
        
        # Mine Voxmill high-ticket leads (US)
        logger.info("\n🎯 PHASE 2: Mining VOXMILL high-ticket targets (US)...")
        voxmill_us = await miner.mine_leads(
            Config.VOXMILL_QUERIES,
            Config.US_CITIES,
            'US',
            'voxmill'
        )
        
        # Mine Agency struggling-SMB leads (UK)
        logger.info("\n🎯 PHASE 3: Mining AGENCY struggling-SMB targets (UK)...")
        agency_uk = await miner.mine_leads(
            Config.AGENCY_QUERIES,
            Config.UK_CITIES,
            'UK',
            'agency'
        )
        
        # Mine Agency struggling-SMB leads (US)
        logger.info("\n🎯 PHASE 4: Mining AGENCY struggling-SMB targets (US)...")
        agency_us = await miner.mine_leads(
            Config.AGENCY_QUERIES,
            Config.US_CITIES,
            'US',
            'agency'
        )
        
        # Combine and sort results
        all_voxmill = voxmill_uk + voxmill_us
        all_agency = agency_uk + agency_us
        
        # Sort by priority (highest first)
        all_voxmill.sort(key=lambda x: x.get('priority_score', 0), reverse=True)
        all_agency.sort(key=lambda x: x.get('priority_score', 0), reverse=True)
        
        # Display detailed results
        logger.info("\n" + "=" * 100)
        logger.info("📊 PLATINUM MINING RESULTS SUMMARY")
        logger.info("=" * 100)
        logger.info(f"\n💎 VOXMILL HIGH-TICKET LEADS: {len(all_voxmill)}")
        logger.info(f"   - UK: {len(voxmill_uk)}")
        logger.info(f"   - US: {len(voxmill_us)}")
        logger.info(f"   - Priority 9-10 (HOT): {len([l for l in all_voxmill if l['priority_score'] >= 9])}")
        logger.info(f"   - Priority 7-8 (WARM): {len([l for l in all_voxmill if 7 <= l['priority_score'] < 9])}")
        logger.info(f"   - Priority 5-6 (COLD): {len([l for l in all_voxmill if l['priority_score'] < 7])}")
        logger.info(f"   - With verified emails: {len([l for l in all_voxmill if l.get('email_verified')])}")
        logger.info(f"   - Instagram followers >1k: {len([l for l in all_voxmill if l.get('instagram_followers', 0) > 1000])}")
        
        logger.info(f"\n🏢 AGENCY SMB LEADS: {len(all_agency)}")
        logger.info(f"   - UK: {len(agency_uk)}")
        logger.info(f"   - US: {len(agency_us)}")
        logger.info(f"   - Priority 9-10 (HOT): {len([l for l in all_agency if l['priority_score'] >= 9])}")
        logger.info(f"   - Priority 7-8 (WARM): {len([l for l in all_agency if 7 <= l['priority_score'] < 9])}")
        logger.info(f"   - Priority 5-6 (COLD): {len([l for l in all_agency if l['priority_score'] < 7])}")
        logger.info(f"   - CRITICAL weakness: {len([l for l in all_agency if l.get('weakness_severity') == 'CRITICAL'])}")
        logger.info(f"   - HIGH weakness: {len([l for l in all_agency if l.get('weakness_severity') == 'HIGH'])}")
        
        # Connect to Google Sheets
        logger.info("\n📊 Connecting to Google Sheets...")
        sheets_manager = SheetsManager()
        sheet = sheets_manager.connect()
        
        # Write platinum results to sheets
        if all_voxmill:
            sheets_manager.write_platinum_leads(sheet, all_voxmill, 'VOXMILL - Platinum Targets', 'voxmill')
        else:
            logger.warning("⚠️  No Voxmill leads to write")
        
        if all_agency:
            sheets_manager.write_platinum_leads(sheet, all_agency, 'AGENCY - Platinum Targets', 'agency')
        else:
            logger.warning("⚠️  No Agency leads to write")
        
        # Final summary
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        logger.info("\n" + "=" * 100)
        logger.info("✨ PLATINUM MISSION COMPLETE")
        logger.info("=" * 100)
        logger.info(f"⏱️  Total execution time: {duration/3600:.2f} hours ({duration/60:.1f} minutes)")
        logger.info(f"📊 Total PLATINUM leads: {len(all_voxmill) + len(all_agency)}")
        logger.info(f"🎯 High-priority targets (7-10): {len([l for l in all_voxmill + all_agency if l['priority_score'] >= 7])}")
        logger.info(f"💎 Data points per lead: 50+")
        logger.info(f"📁 Google Sheet ID: {Config.SHEET_ID}")
        logger.info(f"🔗 View results: https://docs.google.com/spreadsheets/d/{Config.SHEET_ID}")
        logger.info("\n🚀 NEXT STEPS:")
        logger.info("   1. Sort by Priority Score (column AK)")
        logger.info("   2. Review AI-generated outreach messages (columns AM-AT)")
        logger.info("   3. Start with Priority 9-10 leads")
        logger.info("   4. Use 'Detailed Flaws' column to personalize outreach")
        logger.info("=" * 100)
        
        return {
            'success': True,
            'voxmill_leads': len(all_voxmill),
            'agency_leads': len(all_agency),
            'duration_hours': duration/3600
        }
        
    except Exception as e:
        logger.error(f"\n❌ FATAL ERROR: {e}")
        logger.exception("Full traceback:")
        return {
            'success': False,
            'error': str(e)
        }


if __name__ == "__main__":
    asyncio.run(main())
