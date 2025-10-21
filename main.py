"""
VOXMILL ULTIMATE LEAD INTELLIGENCE MINER
Production-grade lead generation with multi-API redundancy and advanced intelligence
"""
import asyncio
import aiohttp
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import re
import json
import logging
from bs4 import BeautifulSoup
from typing import List, Dict, Optional, Set
import os
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================
# CONFIGURATION
# ============================================
class Config:
    """Centralized configuration"""
    GOOGLE_PLACES_API = os.getenv('GOOGLE_PLACES_API', 'AIzaSyECVDGLffKyYBsn1U1aPs4nXubAtzA')
    SERP_API = os.getenv('SERP_API', 'effe5fa5c5a4a81fffe1a32ea2a257f6a2097fc38ca5ca5d5a67bd29f7e0303d')
    YELP_API = os.getenv('YELP_API', 'RP4QNPPXDJLioJAPyQcQ9hKnzsGZJ_PjpkYVcpokpE4nrqPElt4qhGk3GyuEcHiRPc2wE3gjtFG9rFV8WqR8fPYBcuqPJWaJdPTpjbcxmj')
    SHEET_ID = '1JDtLSSf4bT_l4oMNps9y__M_GVmM7_BfWtyNdxsXF4o'
    
    # Targeting
    UK_CITIES = ['London', 'Manchester', 'Birmingham', 'Leeds', 'Edinburgh', 'Bristol', 'Liverpool']
    US_CITIES = ['New York', 'Los Angeles', 'Miami', 'Chicago', 'San Francisco', 'Boston', 'Austin']
    
    # Voxmill high-ticket categories
    VOXMILL_QUERIES = [
        'boutique real estate agency',
        'luxury property agent',
        'premium car dealership',
        'luxury car dealer',
        'yacht charter service',
        'private aviation',
        'boutique hotel',
        'fine dining restaurant',
        'luxury interior design',
        'high-end property developer'
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
        'independent retailer'
    ]
    
    # Rate limiting
    REQUEST_DELAY = 0.3
    BATCH_DELAY = 0.8
    MAX_RETRIES = 3
    TIMEOUT = 10


# ============================================
# GOOGLE SHEETS CONNECTION
# ============================================
class SheetsManager:
    """Manages Google Sheets connection and operations"""
    
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
    def write_leads(sheet, leads: List[Dict], sheet_name: str, lead_type: str):
        """Write leads to specified worksheet"""
        if not leads:
            logger.warning(f"No leads to write for {sheet_name}")
            return
        
        logger.info(f"📊 Writing {len(leads)} leads to '{sheet_name}'...")
        
        try:
            # Get or create worksheet
            try:
                worksheet = sheet.worksheet(sheet_name)
                worksheet.clear()
            except:
                worksheet = sheet.add_worksheet(title=sheet_name, rows=2000, cols=25)
            
            # Define headers
            headers = [
                'Name', 'Category', 'Address', 'City', 'Country', 
                'Phone', 'Email', 'Website', 'Google Maps URL',
                'Instagram', 'Facebook', 'LinkedIn',
                'Google Rating', 'Google Reviews', 'Yelp Rating', 
                'Price Level', 'Tech Stack', 'Social Score', 'Est. Age',
                'Priority Score', 'Intelligence 1', 'Intelligence 2', 'Intelligence 3',
                'Scraped Date', 'Notes'
            ]
            
            # Build data rows
            rows = [headers]
            for lead in leads:
                if lead_type == 'voxmill':
                    intel1 = lead.get('critical_flaws', '')
                    intel2 = lead.get('opportunity_angles', '')
                    intel3 = lead.get('outreach_hook', '')
                else:
                    intel1 = lead.get('red_flags', '')
                    intel2 = lead.get('pain_points', '')
                    intel3 = lead.get('solution_angle', '')
                
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
                    lead.get('total_reviews', 0),
                    lead.get('yelp_rating', 'N/A'),
                    lead.get('price_level', ''),
                    lead.get('tech_stack', ''),
                    lead.get('social_score', 0),
                    lead.get('estimated_age', ''),
                    lead.get('priority_score', 5),
                    intel1,
                    intel2,
                    intel3,
                    datetime.now().strftime('%Y-%m-%d %H:%M'),
                    ''
                ]
                rows.append(row)
            
            # Write in batches to avoid API limits
            batch_size = 1000
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i + batch_size]
                if i == 0:
                    worksheet.update(batch, 'A1')
                else:
                    worksheet.append_rows(batch)
            
            # Format header row
            worksheet.format('A1:Y1', {
                'backgroundColor': {'red': 0, 'green': 0, 'blue': 0},
                'textFormat': {
                    'foregroundColor': {'red': 1, 'green': 0.84, 'blue': 0},
                    'bold': True,
                    'fontSize': 11
                },
                'horizontalAlignment': 'CENTER'
            })
            
            # Freeze header row
            worksheet.freeze(rows=1)
            
            # Auto-resize columns
            worksheet.columns_auto_resize(0, len(headers))
            
            logger.info(f"✅ Successfully wrote {len(leads)} leads to '{sheet_name}'")
            
        except Exception as e:
            logger.error(f"❌ Failed to write to sheet: {e}")
            raise


# ============================================
# API CLIENTS
# ============================================
class GooglePlacesClient:
    """Google Places API client with retry logic"""
    
    @staticmethod
    async def search(session: aiohttp.ClientSession, query: str) -> List[Dict]:
        """Search Google Places with retry logic"""
        url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
        params = {
            'query': query,
            'key': Config.GOOGLE_PLACES_API
        }
        
        for attempt in range(Config.MAX_RETRIES):
            try:
                async with session.get(url, params=params, timeout=Config.TIMEOUT) as response:
                    data = await response.json()
                    
                    status = data.get('status')
                    
                    if status == 'OK':
                        results = data.get('results', [])[:15]
                        logger.info(f"✅ Google Places: Found {len(results)} results for '{query}'")
                        return results
                    elif status == 'ZERO_RESULTS':
                        logger.warning(f"⚠️  Google Places: No results for '{query}'")
                        return []
                    elif status == 'OVER_QUERY_LIMIT':
                        logger.error(f"❌ Google Places: Quota exceeded")
                        await asyncio.sleep(2)
                        continue
                    elif status == 'REQUEST_DENIED':
                        logger.error(f"❌ Google Places: Request denied - check API key and billing")
                        return []
                    else:
                        logger.warning(f"⚠️  Google Places: Status '{status}' for '{query}'")
                        return []
                        
            except asyncio.TimeoutError:
                logger.warning(f"⏱️  Timeout on attempt {attempt + 1} for '{query}'")
                if attempt < Config.MAX_RETRIES - 1:
                    await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"❌ Error searching Google Places: {e}")
                return []
        
        return []
    
    @staticmethod
    async def get_details(session: aiohttp.ClientSession, place_id: str) -> Optional[Dict]:
        """Get detailed place information"""
        url = "https://maps.googleapis.com/maps/api/place/details/json"
        fields = "name,formatted_address,formatted_phone_number,international_phone_number,website,rating,user_ratings_total,price_level,url,business_status,types,opening_hours"
        params = {
            'place_id': place_id,
            'fields': fields,
            'key': Config.GOOGLE_PLACES_API
        }
        
        try:
            async with session.get(url, params=params, timeout=Config.TIMEOUT) as response:
                data = await response.json()
                if data.get('status') == 'OK':
                    return data.get('result')
        except Exception as e:
            logger.error(f"❌ Error getting place details: {e}")
        
        return None


class YelpClient:
    """Yelp API client"""
    
    @staticmethod
    async def search(session: aiohttp.ClientSession, business_name: str, location: str) -> Optional[Dict]:
        """Search Yelp for business"""
        url = "https://api.yelp.com/v3/businesses/search"
        headers = {'Authorization': f'Bearer {Config.YELP_API}'}
        params = {
            'term': business_name,
            'location': location,
            'limit': 1
        }
        
        try:
            async with session.get(url, headers=headers, params=params, timeout=Config.TIMEOUT) as response:
                if response.status == 200:
                    data = await response.json()
                    businesses = data.get('businesses', [])
                    if businesses:
                        return businesses[0]
        except Exception as e:
            logger.debug(f"Yelp search failed: {e}")
        
        return None


# ============================================
# INTELLIGENCE PROCESSOR
# ============================================
class IntelligenceProcessor:
    """Advanced lead intelligence processing"""
    
    @staticmethod
    async def detect_email(session: aiohttp.ClientSession, website: str) -> str:
        """Extract email from website"""
        if not website or website == 'No website':
            return 'None'
        
        try:
            async with session.get(website, timeout=5, allow_redirects=True) as response:
                html = await response.text()
                
                # Common email patterns
                patterns = [
                    r'mailto:([a-zA-Z0-9._-]+@[a-zA-Z0-9._-]+\.[a-zA-Z0-9_-]+)',
                    r'([a-zA-Z0-9._-]+@[a-zA-Z0-9._-]+\.[a-zA-Z0-9_-]+)'
                ]
                
                for pattern in patterns:
                    match = re.search(pattern, html)
                    if match:
                        email = match.group(1) if 'mailto:' in pattern else match.group(0)
                        # Filter out common false positives
                        if not any(x in email.lower() for x in ['example', 'test', 'placeholder', 'domain']):
                            return email
        except:
            pass
        
        return 'Check website'
    
    @staticmethod
    async def find_socials(session: aiohttp.ClientSession, website: str, business_name: str) -> Dict[str, str]:
        """Extract social media handles"""
        socials = {'instagram': '', 'facebook': '', 'linkedin': ''}
        
        if not website or website == 'No website':
            return socials
        
        try:
            async with session.get(website, timeout=5, allow_redirects=True) as response:
                html = await response.text()
                
                # Instagram
                ig_patterns = [
                    r'instagram\.com/([a-zA-Z0-9._]+)',
                    r'@([a-zA-Z0-9._]{3,30})'
                ]
                for pattern in ig_patterns:
                    match = re.search(pattern, html)
                    if match:
                        handle = match.group(1)
                        if len(handle) > 2 and not handle.startswith('http'):
                            socials['instagram'] = f'@{handle}'
                            break
                
                # Facebook
                fb_match = re.search(r'facebook\.com/([a-zA-Z0-9.]+)', html)
                if fb_match:
                    socials['facebook'] = f"fb.com/{fb_match.group(1)}"
                
                # LinkedIn
                li_match = re.search(r'linkedin\.com/company/([a-zA-Z0-9-]+)', html)
                if li_match:
                    socials['linkedin'] = f"linkedin.com/company/{li_match.group(1)}"
        except:
            pass
        
        return socials
    
    @staticmethod
    async def analyze_website(session: aiohttp.ClientSession, website: str) -> str:
        """Detect website platform"""
        if not website or website == 'No website':
            return 'No website'
        
        try:
            async with session.get(website, timeout=5, allow_redirects=True) as response:
                html = await response.text()
                html_lower = html.lower()
                
                # Platform detection
                if 'wix.com' in html or 'wixstatic.com' in html:
                    return 'Wix'
                elif 'wp-content' in html or 'wordpress' in html_lower:
                    return 'WordPress'
                elif 'squarespace' in html_lower:
                    return 'Squarespace'
                elif 'shopify' in html_lower or 'myshopify' in html:
                    return 'Shopify'
                elif 'weebly' in html_lower:
                    return 'Weebly'
                elif '_next' in html or 'nextjs' in html_lower:
                    return 'Next.js'
                elif 'react' in html_lower and '_app' in html:
                    return 'React'
                else:
                    return 'Custom'
        except:
            return 'Unable to load'
    
    @staticmethod
    def calculate_social_score(socials: Dict, website: str, has_reviews: bool) -> int:
        """Calculate social media presence score (0-10)"""
        score = 0
        
        if socials.get('instagram') and '@' in socials['instagram']:
            score += 3
        if socials.get('facebook') and 'fb.com/' in socials['facebook']:
            score += 2
        if socials.get('linkedin'):
            score += 2
        if website and website not in ['No website', 'Unable to load']:
            score += 2
        if has_reviews:
            score += 1
        
        return min(score, 10)
    
    @staticmethod
    def calculate_priority(lead_data: Dict, target_type: str) -> int:
        """Calculate priority score (1-10) based on multiple factors"""
        score = 5
        
        reviews = lead_data.get('total_reviews', 0)
        rating = lead_data.get('rating', 0)
        website = lead_data.get('website', '')
        tech_stack = lead_data.get('tech_stack', '')
        social_score = lead_data.get('social_score', 0)
        has_phone = bool(lead_data.get('phone') and lead_data['phone'] != 'Not listed')
        has_email = bool(lead_data.get('email') and lead_data['email'] not in ['None', 'Check website'])
        
        if target_type == 'voxmill':
            # High-ticket: More established = higher priority
            if reviews > 100:
                score += 2
            if rating > 4.5:
                score += 1
            if tech_stack not in ['No website', 'Wix', 'Weebly', 'Unable to load']:
                score += 1
            if social_score >= 6:
                score += 1
            if has_phone and has_email:
                score += 1
        else:
            # Agency: More struggling = higher priority
            if reviews < 20:
                score += 2
            if rating < 4.0:
                score += 1
            if tech_stack in ['No website', 'Wix', 'Weebly', 'Unable to load']:
                score += 2
            if social_score < 4:
                score += 1
            if not has_email:
                score += 1
        
        return min(max(score, 1), 10)
    
    @staticmethod
    def estimate_business_age(reviews: int, rating: float) -> str:
        """Estimate business age based on review count and rating"""
        if reviews < 10:
            return '<1 year'
        elif reviews < 30:
            return '1-2 years'
        elif reviews < 100:
            return '2-5 years'
        elif reviews < 500:
            return '5-10 years'
        else:
            return '10+ years'
    
    @staticmethod
    def generate_voxmill_intelligence(lead: Dict) -> Dict[str, str]:
        """Generate intelligence for high-ticket Voxmill targets"""
        name = lead.get('name', '')
        category = lead.get('category', '')
        city = lead.get('city', '')
        reviews = lead.get('total_reviews', 0)
        rating = lead.get('rating', 0)
        website = lead.get('website', '')
        tech_stack = lead.get('tech_stack', '')
        social_score = lead.get('social_score', 0)
        
        # Critical flaws
        flaws = []
        if reviews < 50:
            flaws.append('Limited market validation')
        if rating < 4.2:
            flaws.append('Reputation vulnerability')
        if website == 'No website':
            flaws.append('No digital presence')
        elif tech_stack in ['Wix', 'Weebly']:
            flaws.append('Unprofessional website platform')
        if social_score < 5:
            flaws.append('Weak social media footprint')
        if not lead.get('email') or lead['email'] in ['None', 'Check website']:
            flaws.append('No visible contact email')
        
        # Opportunity angles
        opportunities = []
        if reviews > 100 and rating > 4.5:
            opportunities.append('Strong market position - premium positioning opportunity')
        if reviews > 200:
            opportunities.append('High transaction volume - data-rich target')
        if lead.get('price_level') and len(lead.get('price_level', '')) >= 3:
            opportunities.append('Premium pricing power - high budget client')
        if len(flaws) >= 3:
            opportunities.append('Multiple improvement vectors - compelling value proposition')
        if social_score < 4:
            opportunities.append('Social media gap - immediate ROI opportunity')
        
        # Outreach hook
        if 'Weak social media footprint' in flaws or social_score < 4:
            hook = f"I've mapped the {category} market in {city} and noticed {name} is missing 60%+ of the digital touchpoints your top 3 competitors own. I can show you exactly where they're winning customers you're not reaching."
        elif 'Unprofessional website platform' in flaws:
            hook = f"I analyzed {name}'s digital presence against your {city} competitors. Your website platform is limiting your search visibility—I can show you the exact ranking gaps costing you leads."
        elif reviews < 50:
            hook = f"Your competitors in {city} are capturing 3x more online visibility than {name} with strategic market positioning. Want to see the 90-day plan to close that gap?"
        else:
            hook = f"I've built a competitive intelligence breakdown for the {category} market in {city}—{name} ranks in my data, but there are 3 specific moves your competitors are making that you're not. Worth a 5-minute look?"
        
        return {
            'critical_flaws': ' | '.join(flaws) if flaws else 'None detected',
            'opportunity_angles': ' | '.join(opportunities) if opportunities else 'Standard approach',
            'outreach_hook': hook
        }
    
    @staticmethod
    def generate_agency_intelligence(lead: Dict) -> Dict[str, str]:
        """Generate intelligence for struggling-SMB agency targets"""
        name = lead.get('name', '')
        category = lead.get('category', '')
        city = lead.get('city', '')
        reviews = lead.get('total_reviews', 0)
        rating = lead.get('rating', 0)
        website = lead.get('website', '')
        tech_stack = lead.get('tech_stack', '')
        social_score = lead.get('social_score', 0)
        
        # Red flags
        red_flags = []
        if reviews < 10:
            red_flags.append('New/unproven business')
        if rating < 3.5:
            red_flags.append('Reputation issues')
        if website == 'No website':
            red_flags.append('No online presence')
        elif tech_stack == 'Unable to load':
            red_flags.append('Broken/inaccessible website')
        if social_score < 3:
            red_flags.append('Invisible on social media')
        if not lead.get('phone') or lead['phone'] == 'Not listed':
            red_flags.append('Hard to contact')
        
        # Pain points
        pain_points = []
        if reviews < 20:
            pain_points.append('Customer acquisition challenge')
        if website in ['No website', 'Unable to load']:
            pain_points.append('Missing digital infrastructure')
        if social_score < 4:
            pain_points.append('Social media strategy gap')
        if rating < 4.0 and reviews > 10:
            pain_points.append('Reputation management needs')
        if reviews < 30:
            pain_points.append('Early-stage growth systems needed')
        
        # Solution angle
        if website == 'No website':
            solution = f"I build complete digital presence systems for {category} businesses in {city}—website + social automation + review generation. £2,000 setup, then £600/month. ROI in 60 days or free second month."
        elif social_score < 3:
            solution = f"Your {city} competitors are winning customers on Instagram and Facebook while {name} is invisible. I automate your entire social presence for £600/month—content, posting, engagement, growth. 90-day guarantee."
        elif reviews < 20:
            solution = f"I help {category} businesses get 50+ Google reviews in 90 days through automated campaigns. £800/month, proven system, typically 10-15 new reviews per month. First 10 reviews or refund."
        else:
            solution = f"I automate the entire marketing operation for {category} businesses in {city}—you focus on delivery, I handle all customer acquisition for £600-800/month. Performance-based pricing available."
        
        return {
            'red_flags': ' | '.join(red_flags) if red_flags else 'None detected',
            'pain_points': ' | '.join(pain_points) if pain_points else 'Standard needs',
            'solution_angle': solution
        }


# ============================================
# LEAD MINING ENGINE
# ============================================
class LeadMiner:
    """Main lead mining orchestrator"""
    
    def __init__(self):
        self.intel_processor = IntelligenceProcessor()
        self.google_client = GooglePlacesClient()
        self.yelp_client = YelpClient()
    
    async def mine_leads(
        self,
        queries: List[str],
        cities: List[str],
        country: str,
        target_type: str
    ) -> List[Dict]:
        """Mine leads with full intelligence processing"""
        all_leads = []
        seen_place_ids: Set[str] = set()
        
        async with aiohttp.ClientSession() as session:
            for city in cities:
                for query in queries:
                    search_query = f"{query} in {city} {country}"
                    logger.info(f"🔍 Mining: {search_query}")
                    
                    # Search Google Places
                    places = await self.google_client.search(session, search_query)
                    
                    for place in places:
                        place_id = place.get('place_id')
                        if not place_id or place_id in seen_place_ids:
                            continue
                        
                        seen_place_ids.add(place_id)
                        
                        # Get detailed information
                        details = await self.google_client.get_details(session, place_id)
                        if not details:
                            continue
                        
                        # Process lead with full intelligence
                        lead = await self.process_lead(
                            session, details, query, city, country, target_type
                        )
                        
                        if lead:
                            all_leads.append(lead)
                            logger.info(f"✅ Processed: {lead['name']} (Priority: {lead['priority_score']})")
                        
                        await asyncio.sleep(Config.REQUEST_DELAY)
                    
                    await asyncio.sleep(Config.BATCH_DELAY)
        
        logger.info(f"✅ Mining complete: {len(all_leads)} unique leads found")
        return all_leads
    
    async def process_lead(
        self,
        session: aiohttp.ClientSession,
        details: Dict,
        category: str,
        city: str,
        country: str,
        target_type: str
    ) -> Optional[Dict]:
        """Process single lead with full intelligence"""
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
            
            # Gather intelligence in parallel
            email_task = self.intel_processor.detect_email(session, website)
            socials_task = self.intel_processor.find_socials(session, website, name)
            tech_task = self.intel_processor.analyze_website(session, website)
            yelp_task = self.yelp_client.search(session, name, f"{city}, {country}")
            
            email, socials, tech_stack, yelp_data = await asyncio.gather(
                email_task, socials_task, tech_task, yelp_task,
                return_exceptions=True
            )
            
            # Handle any exceptions
            if isinstance(email, Exception):
                email = 'Check website'
            if isinstance(socials, Exception):
                socials = {'instagram': '', 'facebook': '', 'linkedin': ''}
            if isinstance(tech_stack, Exception):
                tech_stack = 'Unable to load'
            if isinstance(yelp_data, Exception):
                yelp_data = None
            
            # Build lead object
            lead = {
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
                'total_reviews': reviews,
                'yelp_rating': yelp_data.get('rating') if yelp_data else 'N/A',
                'price_level': price_level,
                'instagram': socials.get('instagram', ''),
                'facebook': socials.get('facebook', ''),
                'linkedin': socials.get('linkedin', ''),
                'tech_stack': tech_stack,
                'estimated_age': self.intel_processor.estimate_business_age(reviews, rating),
            }
            
            # Calculate scores
            social_score = self.intel_processor.calculate_social_score(socials, website, reviews > 0)
            lead['social_score'] = social_score
            
            priority = self.intel_processor.calculate_priority(lead, target_type)
            lead['priority_score'] = priority
            
            # Generate intelligence
            if target_type == 'voxmill':
                intelligence = self.intel_processor.generate_voxmill_intelligence(lead)
            else:
                intelligence = self.intel_processor.generate_agency_intelligence(lead)
            
            lead.update(intelligence)
            
            return lead
            
        except Exception as e:
            logger.error(f"❌ Error processing lead: {e}")
            return None


# ============================================
# MAIN EXECUTION
# ============================================
async def main():
    """Main execution flow"""
    start_time = datetime.now()
    logger.info("=" * 80)
    logger.info("🚀 VOXMILL ULTIMATE LEAD INTELLIGENCE MINER")
    logger.info("=" * 80)
    
    try:
        # Initialize miner
        miner = LeadMiner()
        
        # Mine Voxmill high-ticket leads (UK)
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
        
        # Combine results
        all_voxmill = voxmill_uk + voxmill_us
        all_agency = agency_uk + agency_us
        
        # Sort by priority (highest first)
        all_voxmill.sort(key=lambda x: x.get('priority_score', 0), reverse=True)
        all_agency.sort(key=lambda x: x.get('priority_score', 0), reverse=True)
        
        # Display results summary
        logger.info("\n" + "=" * 80)
        logger.info("📊 MINING RESULTS SUMMARY")
        logger.info("=" * 80)
        logger.info(f"✅ Voxmill High-Ticket Leads: {len(all_voxmill)}")
        logger.info(f"   - UK: {len(voxmill_uk)}")
        logger.info(f"   - US: {len(voxmill_us)}")
        logger.info(f"   - Priority 8-10: {len([l for l in all_voxmill if l['priority_score'] >= 8])}")
        logger.info(f"   - Priority 6-7: {len([l for l in all_voxmill if 6 <= l['priority_score'] < 8])}")
        logger.info(f"\n✅ Agency SMB Leads: {len(all_agency)}")
        logger.info(f"   - UK: {len(agency_uk)}")
        logger.info(f"   - US: {len(agency_us)}")
        logger.info(f"   - Priority 8-10: {len([l for l in all_agency if l['priority_score'] >= 8])}")
        logger.info(f"   - Priority 6-7: {len([l for l in all_agency if 6 <= l['priority_score'] < 8])}")
        
        # Connect to Google Sheets
        logger.info("\n📊 Connecting to Google Sheets...")
        sheets_manager = SheetsManager()
        sheet = sheets_manager.connect()
        
        # Write results to sheets
        if all_voxmill:
            sheets_manager.write_leads(sheet, all_voxmill, 'VOXMILL - High Ticket', 'voxmill')
        else:
            logger.warning("⚠️  No Voxmill leads to write")
        
        if all_agency:
            sheets_manager.write_leads(sheet, all_agency, 'AGENCY - Struggling SMBs', 'agency')
        else:
            logger.warning("⚠️  No Agency leads to write")
        
        # Final summary
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        logger.info("\n" + "=" * 80)
        logger.info("✨ MISSION COMPLETE")
        logger.info("=" * 80)
        logger.info(f"⏱️  Total execution time: {duration:.1f} seconds")
        logger.info(f"📊 Total leads generated: {len(all_voxmill) + len(all_agency)}")
        logger.info(f"🎯 High-priority targets (8-10): {len([l for l in all_voxmill + all_agency if l['priority_score'] >= 8])}")
        logger.info(f"📁 Google Sheet ID: {Config.SHEET_ID}")
        logger.info(f"🔗 View results: https://docs.google.com/spreadsheets/d/{Config.SHEET_ID}")
        logger.info("\n🚀 Next step: Sort by Priority Score and start outreach!")
        logger.info("=" * 80)
        
        return {
            'success': True,
            'voxmill_leads': len(all_voxmill),
            'agency_leads': len(all_agency),
            'duration': duration
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
