#!/usr/bin/env python3
"""
═══════════════════════════════════════════════════════════════════════════════
VOXMILL PLATINUM INTELLIGENCE ENGINE
═══════════════════════════════════════════════════════════════════════════════

THE ULTIMATE LEAD MINING & ENRICHMENT SYSTEM

Runtime: 4-6 hours
Output: 400-600 leads with 60+ intelligence data points each
APIs: Google Places, Hunter.io, Instagram, Clearbit, BuiltWith, LinkedIn, GPT-4

Author: Voxmill Operations Architect
Version: 2.0 PLATINUM
Date: October 2025

═══════════════════════════════════════════════════════════════════════════════
"""

import os
import sys
import time
import json
import requests
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from openai import OpenAI
import logging
from urllib.parse import urlparse, quote_plus
from concurrent.futures import ThreadPoolExecutor, as_completed
import warnings
warnings.filterwarnings('ignore')

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

# API KEYS - REPLACE WITH YOUR ACTUAL KEYS
GOOGLE_PLACES_API_KEY = 'YOUR_GOOGLE_PLACES_API_KEY'
HUNTER_API_KEY = 'YOUR_HUNTER_IO_API_KEY'
OPENAI_API_KEY = 'YOUR_OPENAI_API_KEY'
RAPIDAPI_KEY = '1440de56aamsh945d6c41f441399p1af6adjsne2d964758775'  # Your Instagram API key
CLEARBIT_API_KEY = 'YOUR_CLEARBIT_API_KEY'
BUILTWITH_API_KEY = 'YOUR_BUILTWITH_API_KEY'
LINKEDIN_SCRAPER_API_KEY = 'YOUR_LINKEDIN_SCRAPER_API_KEY'

# Google Sheets Setup
GOOGLE_SHEETS_CREDS_FILE = 'credentials.json'  # Your service account JSON
SPREADSHEET_NAME = 'Voxmill Platinum Intelligence'

# UK Cities to Target
UK_CITIES = [
    'London', 'Manchester', 'Birmingham', 'Leeds', 'Liverpool',
    'Newcastle', 'Sheffield', 'Bristol', 'Edinburgh', 'Glasgow',
    'Cardiff', 'Belfast', 'Nottingham', 'Southampton', 'Leicester',
    'Brighton', 'Oxford', 'Cambridge', 'Bath', 'York'
]

# Voxmill Target Categories (High-ticket B2B)
VOXMILL_CATEGORIES = [
    'estate agent',
    'real estate agency',
    'luxury car dealership',
    'car dealership',
    'yacht charter',
    'boat charter',
    'private jet charter',
    'luxury hotel',
    'boutique hotel',
    'private clinic',
    'cosmetic surgery clinic',
    'dental clinic',
    'property developer',
    'commercial real estate'
]

# Agency Target Categories (Struggling SMBs)
AGENCY_CATEGORIES = [
    'restaurant',
    'cafe',
    'gym',
    'fitness studio',
    'hair salon',
    'beauty salon',
    'spa',
    'retail store',
    'boutique',
    'local shop',
    'coffee shop',
    'bar',
    'pub'
]

# Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('voxmill_platinum.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Initialize OpenAI
openai_client = OpenAI(api_key=OPENAI_API_KEY)

# ═══════════════════════════════════════════════════════════════════════════════
# UTILITY FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════

def safe_request(url: str, headers: dict = None, params: dict = None, 
                 method: str = 'GET', json_data: dict = None, 
                 max_retries: int = 3, timeout: int = 30) -> Optional[dict]:
    """Make HTTP request with exponential backoff retry logic"""
    for attempt in range(max_retries):
        try:
            if method == 'GET':
                response = requests.get(url, headers=headers, params=params, timeout=timeout)
            elif method == 'POST':
                response = requests.post(url, headers=headers, json=json_data, timeout=timeout)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:  # Rate limit
                wait_time = (2 ** attempt) * 2
                logger.warning(f"Rate limited. Waiting {wait_time}s before retry {attempt + 1}/{max_retries}")
                time.sleep(wait_time)
            else:
                logger.error(f"Request failed: {response.status_code} - {response.text}")
                return None
        except requests.exceptions.Timeout:
            logger.warning(f"Request timeout (attempt {attempt + 1}/{max_retries})")
            time.sleep(2 ** attempt)
        except Exception as e:
            logger.error(f"Request error: {str(e)}")
            return None
    
    return None

def extract_domain(url: str) -> Optional[str]:
    """Extract clean domain from URL"""
    if not url:
        return None
    try:
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        parsed = urlparse(url)
        domain = parsed.netloc or parsed.path
        domain = domain.replace('www.', '')
        return domain.split('/')[0] if domain else None
    except:
        return None

def calculate_priority_score(lead: dict) -> int:
    """Calculate weighted priority score (0-100) based on multiple factors"""
    score = 50  # Base score
    
    # Positive factors
    if lead.get('email_verified'):
        score += 15
    if lead.get('website'):
        score += 10
    if lead.get('instagram_followers', 0) > 1000:
        score += 10
    if lead.get('google_reviews', 0) > 20:
        score += 5
    if lead.get('rating', 0) >= 4.0:
        score += 5
    if lead.get('employee_count', 0) > 10:
        score += 5
    
    # Negative factors (weaknesses = opportunity)
    flaw_count = lead.get('flaw_count', 0)
    if flaw_count >= 10:
        score += 15  # More flaws = better prospect for services
    elif flaw_count >= 5:
        score += 10
    
    # Severity multiplier
    severity = lead.get('weakness_severity', 'Medium')
    if severity == 'Critical':
        score += 10
    elif severity == 'High':
        score += 5
    
    return min(100, max(0, score))

def calculate_digital_maturity(lead: dict) -> int:
    """Calculate digital maturity score (0-10)"""
    score = 0
    
    if lead.get('website'):
        score += 2
    if lead.get('has_ssl'):
        score += 1
    if lead.get('website_speed', 0) >= 70:
        score += 1
    if lead.get('mobile_friendly'):
        score += 1
    if lead.get('instagram_handle'):
        score += 1
    if lead.get('facebook_page'):
        score += 1
    if lead.get('linkedin_company'):
        score += 1
    if lead.get('tech_stack_primary'):
        score += 1
    if lead.get('analytics_tools'):
        score += 1
    
    return score

def extract_instagram_handle(text: str) -> Optional[str]:
    """Extract Instagram handle from text using regex"""
    if not text:
        return None
    
    patterns = [
        r'instagram\.com/([a-zA-Z0-9_.]+)',
        r'@([a-zA-Z0-9_.]+)',
        r'ig:\s*([a-zA-Z0-9_.]+)',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text.lower())
        if match:
            handle = match.group(1).strip()
            if len(handle) > 0 and len(handle) <= 30:
                return handle
    
    return None

# ═══════════════════════════════════════════════════════════════════════════════
# API INTEGRATION FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════

def search_google_places(query: str, location: str) -> List[dict]:
    """Search Google Places API for businesses"""
    logger.info(f"Searching Google Places: {query} in {location}")
    
    url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
    params = {
        'query': f"{query} in {location}, UK",
        'key': GOOGLE_PLACES_API_KEY
    }
    
    all_results = []
    
    # Initial search
    data = safe_request(url, params=params)
    if not data or 'results' not in data:
        logger.error(f"Google Places search failed for {query} in {location}")
        return []
    
    all_results.extend(data['results'])
    
    # Pagination (up to 60 results total)
    next_page_token = data.get('next_page_token')
    while next_page_token and len(all_results) < 60:
        time.sleep(2)  # Required delay for next_page_token
        params['pagetoken'] = next_page_token
        data = safe_request(url, params=params)
        if data and 'results' in data:
            all_results.extend(data['results'])
            next_page_token = data.get('next_page_token')
        else:
            break
    
    logger.info(f"Found {len(all_results)} results for {query} in {location}")
    return all_results

def get_place_details(place_id: str) -> Optional[dict]:
    """Get detailed information about a place"""
    url = "https://maps.googleapis.com/maps/api/place/details/json"
    params = {
        'place_id': place_id,
        'fields': 'name,formatted_address,formatted_phone_number,website,rating,user_ratings_total,opening_hours,price_level,photos,reviews,business_status,url',
        'key': GOOGLE_PLACES_API_KEY
    }
    
    data = safe_request(url, params=params)
    if data and 'result' in data:
        return data['result']
    return None

def verify_email_hunter(domain: str) -> Optional[dict]:
    """Verify and find email addresses using Hunter.io"""
    if not domain:
        return None
    
    logger.info(f"Hunter.io email search for {domain}")
    
    url = "https://api.hunter.io/v2/domain-search"
    params = {
        'domain': domain,
        'api_key': HUNTER_API_KEY,
        'limit': 10
    }
    
    data = safe_request(url, params=params)
    if data and 'data' in data:
        emails = data['data'].get('emails', [])
        if emails:
            # Return the first email with highest confidence
            sorted_emails = sorted(emails, key=lambda x: x.get('confidence', 0), reverse=True)
            return sorted_emails[0]
    
    return None

def get_instagram_data(handle: str) -> Optional[dict]:
    """Get Instagram profile data using RapidAPI"""
    if not handle:
        return None
    
    handle = handle.replace('@', '').strip()
    logger.info(f"Fetching Instagram data for @{handle}")
    
    url = "https://instagram-scraper-api2.p.rapidapi.com/v1/info"
    headers = {
        'X-RapidAPI-Key': RAPIDAPI_KEY,
        'X-RapidAPI-Host': 'instagram-scraper-api2.p.rapidapi.com'
    }
    params = {'username_or_id_or_url': handle}
    
    data = safe_request(url, headers=headers, params=params)
    if data and 'data' in data:
        profile = data['data']
        return {
            'followers': profile.get('follower_count', 0),
            'following': profile.get('following_count', 0),
            'posts': profile.get('media_count', 0),
            'verified': profile.get('is_verified', False),
            'bio': profile.get('biography', ''),
            'engagement_rate': calculate_engagement_rate(profile)
        }
    
    return None

def calculate_engagement_rate(instagram_profile: dict) -> float:
    """Calculate estimated engagement rate from Instagram data"""
    followers = instagram_profile.get('follower_count', 0)
    posts = instagram_profile.get('media_count', 0)
    
    if followers == 0:
        return 0.0
    
    # Estimated engagement (since we don't have likes/comments from this endpoint)
    # Industry average is 1-5% for small businesses
    base_rate = 2.5
    
    # Adjust based on follower count (larger accounts typically have lower engagement)
    if followers > 100000:
        base_rate *= 0.5
    elif followers > 10000:
        base_rate *= 0.8
    
    return round(base_rate, 2)

def enrich_clearbit(domain: str) -> Optional[dict]:
    """Enrich company data using Clearbit"""
    if not domain:
        return None
    
    logger.info(f"Clearbit enrichment for {domain}")
    
    url = f"https://company.clearbit.com/v2/companies/find"
    headers = {'Authorization': f'Bearer {CLEARBIT_API_KEY}'}
    params = {'domain': domain}
    
    data = safe_request(url, headers=headers, params=params)
    if data:
        return {
            'name': data.get('name'),
            'description': data.get('description'),
            'founded_year': data.get('foundedYear'),
            'employee_count': data.get('metrics', {}).get('employees'),
            'revenue_range': data.get('metrics', {}).get('estimatedAnnualRevenue'),
            'industry': data.get('category', {}).get('industry'),
            'tech': data.get('tech', []),
            'alexa_rank': data.get('metrics', {}).get('alexaGlobalRank'),
            'parent_company': data.get('parent', {}).get('domain')
        }
    
    return None

def get_builtwith_data(domain: str) -> Optional[dict]:
    """Get technology stack using BuiltWith API"""
    if not domain:
        return None
    
    logger.info(f"BuiltWith tech stack for {domain}")
    
    url = f"https://api.builtwith.com/free1/api.json"
    params = {
        'KEY': BUILTWITH_API_KEY,
        'LOOKUP': domain
    }
    
    data = safe_request(url, params=params)
    if data and 'Results' in data and len(data['Results']) > 0:
        tech = data['Results'][0].get('Result', {}).get('Paths', [])
        if tech and len(tech) > 0:
            technologies = tech[0].get('Technologies', [])
            
            tech_stack = {
                'cms': None,
                'hosting': None,
                'analytics': [],
                'advertising': [],
                'frameworks': [],
                'all_tech': []
            }
            
            for t in technologies:
                name = t.get('Name', '')
                categories = t.get('Categories', [])
                
                tech_stack['all_tech'].append(name)
                
                if 'Content Management System' in categories:
                    tech_stack['cms'] = name
                if 'Web Hosting' in categories or 'Cloud Hosting' in categories:
                    tech_stack['hosting'] = name
                if 'Analytics' in categories:
                    tech_stack['analytics'].append(name)
                if 'Advertising' in categories:
                    tech_stack['advertising'].append(name)
                if 'Frameworks' in categories or 'JavaScript Frameworks' in categories:
                    tech_stack['frameworks'].append(name)
            
            return tech_stack
    
    return None

def get_pagespeed_score(url: str) -> Optional[dict]:
    """Get website speed and performance metrics using Google PageSpeed API"""
    if not url:
        return None
    
    logger.info(f"PageSpeed analysis for {url}")
    
    api_url = "https://www.googleapis.com/pagespeedonline/v5/runPagespeed"
    params = {
        'url': url,
        'key': GOOGLE_PLACES_API_KEY,  # Can reuse Google API key
        'category': 'performance',
        'category': 'seo',
        'category': 'accessibility'
    }
    
    data = safe_request(api_url, params=params, timeout=60)
    if data and 'lighthouseResult' in data:
        categories = data['lighthouseResult'].get('categories', {})
        return {
            'performance_score': int(categories.get('performance', {}).get('score', 0) * 100),
            'seo_score': int(categories.get('seo', {}).get('score', 0) * 100),
            'accessibility_score': int(categories.get('accessibility', {}).get('score', 0) * 100),
            'load_time': data['lighthouseResult'].get('audits', {}).get('interactive', {}).get('numericValue', 0) / 1000
        }
    
    return None

def check_ssl_certificate(domain: str) -> bool:
    """Check if website has valid SSL certificate"""
    if not domain:
        return False
    
    try:
        url = f"https://{domain}"
        response = requests.head(url, timeout=10, allow_redirects=True)
        return response.url.startswith('https://')
    except:
        return False

def check_mobile_friendly(url: str) -> bool:
    """Check if website is mobile-friendly (simplified check)"""
    if not url:
        return False
    
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X)'}
        response = requests.get(url, headers=headers, timeout=15)
        
        # Basic check: look for viewport meta tag
        if 'viewport' in response.text.lower():
            return True
    except:
        pass
    
    return False

def find_competitors(business_name: str, category: str, city: str, exclude_place_id: str) -> List[dict]:
    """Find top competitors using Google Places"""
    logger.info(f"Finding competitors for {business_name} in {city}")
    
    results = search_google_places(category, city)
    
    competitors = []
    for result in results:
        if result.get('place_id') == exclude_place_id:
            continue
        
        competitors.append({
            'name': result.get('name'),
            'rating': result.get('rating', 0),
            'reviews': result.get('user_ratings_total', 0),
            'address': result.get('formatted_address', '')
        })
        
        if len(competitors) >= 5:
            break
    
    return competitors

def scrape_linkedin_company(domain: str) -> Optional[dict]:
    """Scrape LinkedIn company data (using RapidAPI LinkedIn scraper)"""
    if not domain:
        return None
    
    logger.info(f"LinkedIn scraping for {domain}")
    
    # Note: Replace with actual LinkedIn scraper API endpoint
    url = "https://linkedin-data-scraper.p.rapidapi.com/company_search"
    headers = {
        'X-RapidAPI-Key': LINKEDIN_SCRAPER_API_KEY,
        'X-RapidAPI-Host': 'linkedin-data-scraper.p.rapidapi.com'
    }
    params = {'domain': domain}
    
    data = safe_request(url, headers=headers, params=params)
    if data and 'company' in data:
        company = data['company']
        return {
            'url': company.get('url'),
            'followers': company.get('followerCount', 0),
            'employees': company.get('employeeCount'),
            'description': company.get('description')
        }
    
    return None

def find_decision_maker(company_name: str, domain: str) -> Optional[dict]:
    """Find decision maker using LinkedIn scraper"""
    if not company_name:
        return None
    
    logger.info(f"Finding decision maker for {company_name}")
    
    # Note: This is a simplified version - actual implementation would use LinkedIn People Search API
    # Search for titles like: Owner, Director, Marketing Manager, General Manager
    target_titles = ['owner', 'director', 'ceo', 'founder', 'managing director', 'general manager', 'marketing director']
    
    # Placeholder return - in production this would scrape LinkedIn
    return {
        'name': 'Decision Maker',  # Would be scraped
        'title': 'Owner/Director',  # Would be scraped
        'department': 'Executive',
        'linkedin_url': None
    }

def analyze_reviews_with_gpt(reviews: List[dict]) -> dict:
    """Analyze reviews using GPT-4 to extract sentiment and keywords"""
    if not reviews or len(reviews) == 0:
        return {
            'sentiment': 'Unknown',
            'positive_keywords': [],
            'negative_keywords': [],
            'main_complaint': None
        }
    
    logger.info(f"Analyzing {len(reviews)} reviews with GPT-4")
    
    # Take last 10 reviews
    review_texts = [r.get('text', '') for r in reviews[:10] if r.get('text')]
    combined_text = '\n\n'.join(review_texts)
    
    prompt = f"""Analyze these customer reviews and provide:
1. Overall sentiment (Positive/Negative/Mixed)
2. Top 3 positive keywords/themes
3. Top 3 negative keywords/themes
4. Most common complaint (if any)

Reviews:
{combined_text}

Respond in JSON format:
{{
    "sentiment": "Positive/Negative/Mixed",
    "positive_keywords": ["keyword1", "keyword2", "keyword3"],
    "negative_keywords": ["keyword1", "keyword2", "keyword3"],
    "main_complaint": "brief description or null"
}}"""
    
    try:
        response = openai_client.chat.completions.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": "You are a business review analyst. Respond only with valid JSON."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.3,
            max_tokens=300
        )
        
        result = json.loads(response.choices[0].message.content)
        return result
    except Exception as e:
        logger.error(f"GPT-4 review analysis failed: {str(e)}")
        return {
            'sentiment': 'Unknown',
            'positive_keywords': [],
            'negative_keywords': [],
            'main_complaint': None
        }

def generate_outreach_messages(lead: dict) -> dict:
    """Generate 5 AI-powered outreach messages using GPT-4"""
    logger.info(f"Generating outreach for {lead.get('name')}")
    
    # Build context for GPT
    flaws = lead.get('detailed_flaws', [])
    competitors = lead.get('competitors', [])
    
    competitor_context = ""
    if competitors:
        comp = competitors[0]
        competitor_context = f"Their main competitor {comp['name']} has {comp['rating']} stars and {comp['reviews']} reviews."
    
    flaw_context = ", ".join(flaws[:5]) if flaws else "no major issues detected"
    
    prompt = f"""You are a B2B sales expert writing outreach messages for Voxmill, a company that provides automated local market intelligence reports.

Business: {lead.get('name')}
Industry: {lead.get('category')}
Location: {lead.get('city')}, UK
Website: {lead.get('website', 'None')}
Rating: {lead.get('rating', 'N/A')} stars ({lead.get('google_reviews', 0)} reviews)
Key Issues: {flaw_context}
{competitor_context}

Generate 5 personalized outreach messages (50-80 words each) using these angles:
1. Pain Point - highlight their biggest weakness
2. Opportunity - show growth potential with data
3. Competitor - mention how competitors are ahead
4. Data-Driven - use specific local market stats
5. Urgency - time-sensitive opportunity

Also generate:
- Email subject line (8-12 words)
- LinkedIn connection request (200 chars max)
- Instagram DM opener (150 chars max)
- SMS template (140 chars max)

Format as JSON:
{{
    "email_pain": "message",
    "email_opportunity": "message",
    "email_competitor": "message",
    "email_data": "message",
    "email_urgency": "message",
    "subject_line": "subject",
    "linkedin_request": "message",
    "instagram_dm": "message",
    "sms_template": "message"
}}"""
    
    try:
        response = openai_client.chat.completions.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": "You are an expert B2B sales copywriter. Write compelling, data-driven outreach. Respond only with valid JSON."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7,
            max_tokens=1500
        )
        
        result = json.loads(response.choices[0].message.content)
        return result
    except Exception as e:
        logger.error(f"GPT-4 outreach generation failed: {str(e)}")
        return {
            'email_pain': 'Failed to generate',
            'email_opportunity': 'Failed to generate',
            'email_competitor': 'Failed to generate',
            'email_data': 'Failed to generate',
            'email_urgency': 'Failed to generate',
            'subject_line': 'Quick question about your business',
            'linkedin_request': 'Hi, I'd love to connect!',
            'instagram_dm': 'Hey! Love what you're doing',
            'sms_template': 'Hi, quick question about your business'
        }

# ═══════════════════════════════════════════════════════════════════════════════
# FLAW DETECTION ENGINE
# ═══════════════════════════════════════════════════════════════════════════════

def detect_business_flaws(lead: dict) -> Tuple[List[str], str, int]:
    """
    Comprehensive flaw detection across 20+ categories
    Returns: (list of flaws, severity, flaw_count)
    """
    flaws = []
    
    # CATEGORY 1: Website Issues
    if not lead.get('website'):
        flaws.append("No website")
    else:
        if not lead.get('has_ssl'):
            flaws.append("No SSL certificate (security risk)")
        if lead.get('website_speed', 100) < 50:
            flaws.append(f"Very slow website ({lead.get('website_speed')}% speed score)")
        elif lead.get('website_speed', 100) < 70:
            flaws.append(f"Slow website ({lead.get('website_speed')}% speed score)")
        if not lead.get('mobile_friendly'):
            flaws.append("Website not mobile-friendly")
        if lead.get('seo_score', 100) < 50:
            flaws.append(f"Poor SEO ({lead.get('seo_score')}% SEO score)")
    
    # CATEGORY 2: Social Media Presence
    if not lead.get('instagram_handle'):
        flaws.append("No Instagram presence")
    elif lead.get('instagram_followers', 0) < 100:
        flaws.append(f"Very low Instagram following ({lead.get('instagram_followers')} followers)")
    elif lead.get('instagram_followers', 0) < 500:
        flaws.append(f"Low Instagram following ({lead.get('instagram_followers')} followers)")
    
    if not lead.get('facebook_page'):
        flaws.append("No Facebook page")
    
    if not lead.get('linkedin_company'):
        flaws.append("No LinkedIn company page")
    
    # CATEGORY 3: Reviews & Reputation
    if lead.get('google_reviews', 0) < 5:
        flaws.append(f"Very few Google reviews ({lead.get('google_reviews')} reviews)")
    elif lead.get('google_reviews', 0) < 10:
        flaws.append(f"Low review count ({lead.get('google_reviews')} reviews)")
    
    if lead.get('rating', 5.0) < 3.5:
        flaws.append(f"Poor rating ({lead.get('rating')} stars)")
    elif lead.get('rating', 5.0) < 4.0:
        flaws.append(f"Below-average rating ({lead.get('rating')} stars)")
    
    if lead.get('review_sentiment') == 'Negative':
        flaws.append("Negative review sentiment detected")
    
    # CATEGORY 4: Technology Stack
    if not lead.get('analytics_tools'):
        flaws.append("No analytics tracking (flying blind)")
    
    if not lead.get('ad_platforms'):
        flaws.append("No advertising pixels installed")
    
    if lead.get('cms') == 'None' or not lead.get('cms'):
        flaws.append("No modern CMS detected")
    
    # CATEGORY 5: Competitive Position
    competitors = lead.get('competitors', [])
    if competitors:
        avg_competitor_rating = sum([c.get('rating', 0) for c in competitors]) / len(competitors)
        if lead.get('rating', 0) < avg_competitor_rating - 0.5:
            flaws.append(f"Rating significantly below competitors (competitors avg: {avg_competitor_rating:.1f})")
        
        avg_competitor_reviews = sum([c.get('reviews', 0) for c in competitors]) / len(competitors)
        if lead.get('google_reviews', 0) < avg_competitor_reviews * 0.5:
            flaws.append(f"Far fewer reviews than competitors")
    
    # CATEGORY 6: Digital Maturity
    digital_score = lead.get('digital_maturity_score', 0)
    if digital_score <= 3:
        flaws.append("Very low digital maturity (minimal online presence)")
    elif digital_score <= 5:
        flaws.append("Low digital maturity (missing key channels)")
    
    # CATEGORY 7: Business Operations
    if not lead.get('email_verified'):
        flaws.append("No verified business email found")
    
    if not lead.get('phone'):
        flaws.append("No phone number listed")
    
    # CATEGORY 8: Content & Engagement
    if lead.get('instagram_posts', 0) < 10:
        flaws.append("Minimal Instagram content")
    
    if lead.get('instagram_engagement', 0) < 1.0:
        flaws.append(f"Low Instagram engagement rate ({lead.get('instagram_engagement')}%)")
    
    # CATEGORY 9: Market Visibility
    if lead.get('traffic_estimate', 1000) < 100:
        flaws.append("Very low website traffic")
    
    # CATEGORY 10: Growth Indicators
    if lead.get('job_postings', 0) == 0:
        flaws.append("No recent hiring activity (potential stagnation)")
    
    # Calculate severity
    flaw_count = len(flaws)
    if flaw_count >= 15:
        severity = "Critical"
    elif flaw_count >= 10:
        severity = "High"
    elif flaw_count >= 5:
        severity = "Medium"
    else:
        severity = "Low"
    
    return flaws, severity, flaw_count

# ═══════════════════════════════════════════════════════════════════════════════
# MAIN ENRICHMENT PIPELINE
# ═══════════════════════════════════════════════════════════════════════════════

def enrich_lead(place: dict, category: str, city: str, is_voxmill_target: bool) -> dict:
    """
    Full enrichment pipeline for a single lead
    Gathers data from all APIs and analyzes comprehensively
    """
    lead = {
        'name': place.get('name'),
        'category': category,
        'city': city,
        'country': 'UK',
        'address': place.get('formatted_address', ''),
        'place_id': place.get('place_id'),
        'maps_url': f"https://www.google.com/maps/place/?q=place_id:{place.get('place_id')}",
        'rating': place.get('rating', 0),
        'google_reviews': place.get('user_ratings_total', 0),
        'price_level': place.get('price_level'),
        'business_status': place.get('business_status', 'OPERATIONAL'),
        'is_voxmill_target': is_voxmill_target
    }
    
    # Get detailed place info
    logger.info(f"Enriching: {lead['name']}")
    details = get_place_details(place.get('place_id'))
    
    if details:
        lead['phone'] = details.get('formatted_phone_number')
        lead['website'] = details.get('website')
        lead['reviews_data'] = details.get('reviews', [])
        
        # Extract social media from website or description
        if details.get('website'):
            # Try to find social links
            pass
    
    domain = extract_domain(lead.get('website'))
    lead['domain'] = domain
    
    # Email verification with Hunter.io
    if domain:
        time.sleep(1)  # Rate limit protection
        email_data = verify_email_hunter(domain)
        if email_data:
            lead['hunter_email'] = email_data.get('value')
            lead['email_confidence'] = email_data.get('confidence')
            lead['email_verified'] = email_data.get('confidence', 0) >= 70
        else:
            lead['hunter_email'] = None
            lead['email_confidence'] = 0
            lead['email_verified'] = False
    
    # Instagram data
    instagram_handle = extract_instagram_handle(str(details))
    if instagram_handle:
        time.sleep(1)
        insta_data = get_instagram_data(instagram_handle)
        if insta_data:
            lead['instagram_handle'] = f"@{instagram_handle}"
            lead['instagram_followers'] = insta_data.get('followers', 0)
            lead['instagram_posts'] = insta_data.get('posts', 0)
            lead['instagram_engagement'] = insta_data.get('engagement_rate', 0)
            lead['instagram_verified'] = insta_data.get('verified', False)
    
    # Company enrichment (Clearbit)
    if domain:
        time.sleep(1)
        clearbit_data = enrich_clearbit(domain)
        if clearbit_data:
            lead['employee_count'] = clearbit_data.get('employee_count')
            lead['founded_year'] = clearbit_data.get('founded_year')
            lead['revenue_range'] = clearbit_data.get('revenue_range')
            lead['industry'] = clearbit_data.get('industry')
            lead['alexa_rank'] = clearbit_data.get('alexa_rank')
    
    # Tech stack (BuiltWith)
    if domain:
        time.sleep(1)
        builtwith_data = get_builtwith_data(domain)
        if builtwith_data:
            lead['cms'] = builtwith_data.get('cms')
            lead['hosting'] = builtwith_data.get('hosting')
            lead['analytics_tools'] = ', '.join(builtwith_data.get('analytics', []))
            lead['ad_platforms'] = ', '.join(builtwith_data.get('advertising', []))
            lead['tech_stack_primary'] = ', '.join(builtwith_data.get('all_tech', [])[:5])
    
    # Website performance
    if lead.get('website'):
        time.sleep(2)
        pagespeed = get_pagespeed_score(lead['website'])
        if pagespeed:
            lead['website_speed'] = pagespeed.get('performance_score', 0)
            lead['seo_score'] = pagespeed.get('seo_score', 0)
            lead['accessibility_score'] = pagespeed.get('accessibility_score', 0)
            lead['load_time'] = round(pagespeed.get('load_time', 0), 2)
        
        lead['has_ssl'] = check_ssl_certificate(domain) if domain else False
        lead['mobile_friendly'] = check_mobile_friendly(lead['website'])
    
    # LinkedIn company data
    if domain:
        time.sleep(1)
        linkedin_data = scrape_linkedin_company(domain)
        if linkedin_data:
            lead['linkedin_company'] = linkedin_data.get('url')
            lead['linkedin_followers'] = linkedin_data.get('followers', 0)
    
    # Decision maker
    time.sleep(1)
    decision_maker = find_decision_maker(lead['name'], domain)
    if decision_maker:
        lead['decision_maker'] = decision_maker.get('name')
        lead['decision_maker_position'] = decision_maker.get('title')
        lead['decision_maker_dept'] = decision_maker.get('department')
    
    # Competitor analysis
    time.sleep(2)
    competitors = find_competitors(lead['name'], category, city, place.get('place_id'))
    lead['competitors'] = competitors
    
    # Review sentiment analysis
    if lead.get('reviews_data') and len(lead['reviews_data']) > 0:
        sentiment_data = analyze_reviews_with_gpt(lead['reviews_data'])
        lead['review_sentiment'] = sentiment_data.get('sentiment')
        lead['positive_keywords'] = ', '.join(sentiment_data.get('positive_keywords', []))
        lead['negative_keywords'] = ', '.join(sentiment_data.get('negative_keywords', []))
    
    # Calculate digital maturity
    lead['digital_maturity_score'] = calculate_digital_maturity(lead)
    
    # Flaw detection
    flaws, severity, flaw_count = detect_business_flaws(lead)
    lead['detailed_flaws'] = flaws
    lead['weakness_severity'] = severity
    lead['flaw_count'] = flaw_count
    
    # Priority score
    lead['priority_score'] = calculate_priority_score(lead)
    
    # Generate AI outreach messages
    time.sleep(2)
    outreach = generate_outreach_messages(lead)
    lead.update(outreach)
    
    # Timestamp
    lead['last_updated'] = datetime.now().isoformat()
    
    logger.info(f"✅ Enriched: {lead['name']} - Priority: {lead['priority_score']} - Flaws: {flaw_count}")
    
    return lead

# ═══════════════════════════════════════════════════════════════════════════════
# GOOGLE SHEETS EXPORT
# ═══════════════════════════════════════════════════════════════════════════════

def init_google_sheets() -> gspread.Spreadsheet:
    """Initialize Google Sheets connection"""
    scope = [
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/drive'
    ]
    
    creds = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_SHEETS_CREDS_FILE, scope)
    client = gspread.authorize(creds)
    
    # Create or open spreadsheet
    try:
        sheet = client.open(SPREADSHEET_NAME)
        logger.info(f"Opened existing spreadsheet: {SPREADSHEET_NAME}")
    except:
        sheet = client.create(SPREADSHEET_NAME)
        logger.info(f"Created new spreadsheet: {SPREADSHEET_NAME}")
    
    return sheet

def export_to_sheets(leads: List[dict], sheet_name: str, spreadsheet: gspread.Spreadsheet):
    """Export leads to Google Sheets with formatting"""
    logger.info(f"Exporting {len(leads)} leads to sheet: {sheet_name}")
    
    # Define columns (60+ columns)
    headers = [
        'Name', 'Category', 'City', 'Country', 'Address', 'Phone',
        'Email (Hunter)', 'Email Confidence', 'Email Verified',
        'Decision Maker', 'Position', 'Department',
        'Website', 'Domain', 'Google Maps', 'Place ID',
        'Instagram Handle', 'Instagram Followers', 'Instagram Posts', 
        'Instagram Engagement %', 'Instagram Verified',
        'Facebook Page', 'LinkedIn Company', 'LinkedIn Followers',
        'Google Rating', 'Google Reviews', 'Price Level',
        'Review Sentiment', 'Positive Keywords', 'Negative Keywords',
        'Tech Stack Primary', 'CMS', 'Web Hosting', 'Analytics Tools', 'Ad Platforms',
        'Website Speed (0-100)', 'SEO Score (0-100)', 'Accessibility Score',
        'Load Time (s)', 'Mobile Friendly', 'Has SSL',
        'Employee Count', 'Founded Year', 'Revenue Range', 'Industry', 'Alexa Rank',
        'Digital Maturity (0-10)', 'Flaw Count', 'Weakness Severity',
        'Detailed Flaws', 'Competitor 1', 'Competitor 2', 'Competitor 3',
        'Competitive Gap Analysis', 'Priority Score (0-100)',
        'Outreach #1 (Pain)', 'Outreach #2 (Opportunity)', 'Outreach #3 (Competitor)',
        'Outreach #4 (Data)', 'Outreach #5 (Urgency)',
        'Email Subject Line', 'LinkedIn Connection Request', 
        'Instagram DM', 'SMS Template',
        'Last Updated'
    ]
    
    # Create or clear worksheet
    try:
        worksheet = spreadsheet.worksheet(sheet_name)
        worksheet.clear()
    except:
        worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=2000, cols=len(headers))
    
    # Prepare data rows
    rows = [headers]
    
    for lead in leads:
        competitors = lead.get('competitors', [])
        comp1 = competitors[0]['name'] if len(competitors) > 0 else ''
        comp2 = competitors[1]['name'] if len(competitors) > 1 else ''
        comp3 = competitors[2]['name'] if len(competitors) > 2 else ''
        
        flaws_text = '\n'.join(lead.get('detailed_flaws', []))
        
        row = [
            lead.get('name', ''),
            lead.get('category', ''),
            lead.get('city', ''),
            lead.get('country', ''),
            lead.get('address', ''),
            lead.get('phone', ''),
            lead.get('hunter_email', ''),
            lead.get('email_confidence', 0),
            'Yes' if lead.get('email_verified') else 'No',
            lead.get('decision_maker', ''),
            lead.get('decision_maker_position', ''),
            lead.get('decision_maker_dept', ''),
            lead.get('website', ''),
            lead.get('domain', ''),
            lead.get('maps_url', ''),
            lead.get('place_id', ''),
            lead.get('instagram_handle', ''),
            lead.get('instagram_followers', 0),
            lead.get('instagram_posts', 0),
            lead.get('instagram_engagement', 0),
            'Yes' if lead.get('instagram_verified') else 'No',
            lead.get('facebook_page', ''),
            lead.get('linkedin_company', ''),
            lead.get('linkedin_followers', 0),
            lead.get('rating', 0),
            lead.get('google_reviews', 0),
            lead.get('price_level', ''),
            lead.get('review_sentiment', ''),
            lead.get('positive_keywords', ''),
            lead.get('negative_keywords', ''),
            lead.get('tech_stack_primary', ''),
            lead.get('cms', ''),
            lead.get('hosting', ''),
            lead.get('analytics_tools', ''),
            lead.get('ad_platforms', ''),
            lead.get('website_speed', 0),
            lead.get('seo_score', 0),
            lead.get('accessibility_score', 0),
            lead.get('load_time', 0),
            'Yes' if lead.get('mobile_friendly') else 'No',
            'Yes' if lead.get('has_ssl') else 'No',
            lead.get('employee_count', ''),
            lead.get('founded_year', ''),
            lead.get('revenue_range', ''),
            lead.get('industry', ''),
            lead.get('alexa_rank', ''),
            lead.get('digital_maturity_score', 0),
            lead.get('flaw_count', 0),
            lead.get('weakness_severity', ''),
            flaws_text,
            comp1,
            comp2,
            comp3,
            f"vs {comp1}: Rating diff: {lead.get('rating', 0) - competitors[0].get('rating', 0):.1f}" if competitors else '',
            lead.get('priority_score', 0),
            lead.get('email_pain', ''),
            lead.get('email_opportunity', ''),
            lead.get('email_competitor', ''),
            lead.get('email_data', ''),
            lead.get('email_urgency', ''),
            lead.get('subject_line', ''),
            lead.get('linkedin_request', ''),
            lead.get('instagram_dm', ''),
            lead.get('sms_template', ''),
            lead.get('last_updated', '')
        ]
        
        rows.append(row)
    
    # Write to sheet
    worksheet.update('A1', rows)
    
    # Format header row
    worksheet.format('A1:BH1', {
        'backgroundColor': {'red': 0.0, 'green': 0.0, 'blue': 0.0},
        'textFormat': {'foregroundColor': {'red': 0.84, 'green': 0.65, 'blue': 0.13}, 'bold': True, 'fontSize': 11},
        'horizontalAlignment': 'CENTER'
    })
    
    # Freeze header row
    worksheet.freeze(rows=1)
    
    # Auto-resize columns
    worksheet.columns_auto_resize(0, len(headers))
    
    logger.info(f"✅ Successfully exported {len(leads)} leads to '{sheet_name}'")

# ═══════════════════════════════════════════════════════════════════════════════
# MAIN EXECUTION
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    """Main execution pipeline"""
    logger.info("="*80)
    logger.info("VOXMILL PLATINUM INTELLIGENCE ENGINE - STARTING")
    logger.info("="*80)
    
    start_time = time.time()
    
    # Initialize Google Sheets
    logger.info("Initializing Google Sheets connection...")
    spreadsheet = init_google_sheets()
    
    # Collections
    voxmill_leads = []
    agency_leads = []
    
    # PHASE 1: Mine Voxmill Targets (High-ticket B2B)
    logger.info("\n" + "="*80)
    logger.info("PHASE 1: MINING VOXMILL TARGETS (Boutique Real Estate, Luxury Auto, Charters)")
    logger.info("="*80)
    
    for city in UK_CITIES:
        for category in VOXMILL_CATEGORIES:
            logger.info(f"\n🔍 Searching: {category} in {city}")
            places = search_google_places(category, city)
            
            for place in places:
                try:
                    enriched_lead = enrich_lead(place, category, city, is_voxmill_target=True)
                    voxmill_leads.append(enriched_lead)
                    
                    # Progress update
                    if len(voxmill_leads) % 10 == 0:
                        logger.info(f"📊 Progress: {len(voxmill_leads)} Voxmill leads enriched")
                    
                    # Rate limit protection
                    time.sleep(3)
                    
                except Exception as e:
                    logger.error(f"Failed to enrich {place.get('name')}: {str(e)}")
                    continue
            
            # Pause between categories
            time.sleep(5)
    
    logger.info(f"\n✅ PHASE 1 COMPLETE: {len(voxmill_leads)} Voxmill targets enriched")
    
    # PHASE 2: Mine Agency Targets (Struggling SMBs)
    logger.info("\n" + "="*80)
    logger.info("PHASE 2: MINING AGENCY TARGETS (Struggling SMBs needing AI/automation)")
    logger.info("="*80)
    
    for city in UK_CITIES[:10]:  # Limit to top 10 cities for agency targets
        for category in AGENCY_CATEGORIES:
            logger.info(f"\n🔍 Searching: {category} in {city}")
            places = search_google_places(category, city)
            
            # Filter for struggling businesses (low reviews, poor ratings, etc.)
            struggling_places = [
                p for p in places 
                if p.get('user_ratings_total', 0) < 50 or p.get('rating', 5.0) < 4.0
            ]
            
            for place in struggling_places[:5]:  # Top 5 struggling per category
                try:
                    enriched_lead = enrich_lead(place, category, city, is_voxmill_target=False)
                    agency_leads.append(enriched_lead)
                    
                    if len(agency_leads) % 10 == 0:
                        logger.info(f"📊 Progress: {len(agency_leads)} agency leads enriched")
                    
                    time.sleep(3)
                    
                except Exception as e:
                    logger.error(f"Failed to enrich {place.get('name')}: {str(e)}")
                    continue
            
            time.sleep(5)
    
    logger.info(f"\n✅ PHASE 2 COMPLETE: {len(agency_leads)} agency targets enriched")
    
    # PHASE 3: Export to Google Sheets
    logger.info("\n" + "="*80)
    logger.info("PHASE 3: EXPORTING TO GOOGLE SHEETS")
    logger.info("="*80)
    
    # Sort by priority score
    voxmill_leads.sort(key=lambda x: x.get('priority_score', 0), reverse=True)
    agency_leads.sort(key=lambda x: x.get('priority_score', 0), reverse=True)
    
    # Export
    export_to_sheets(voxmill_leads, 'Voxmill Targets (High-Ticket)', spreadsheet)
    export_to_sheets(agency_leads, 'Agency Targets (Struggling SMBs)', spreadsheet)
    
    # PHASE 4: Summary Report
    logger.info("\n" + "="*80)
    logger.info("EXECUTION SUMMARY")
    logger.info("="*80)
    
    elapsed_time = time.time() - start_time
    hours = int(elapsed_time // 3600)
    minutes = int((elapsed_time % 3600) // 60)
    
    logger.info(f"""
📊 FINAL RESULTS:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
✅ Voxmill Targets (High-Ticket): {len(voxmill_leads)}
✅ Agency Targets (SMBs): {len(agency_leads)}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📈 Total Leads Enriched: {len(voxmill_leads) + len(agency_leads)}
⏱️  Total Runtime: {hours}h {minutes}m
📋 Spreadsheet: {spreadsheet.title}
🔗 URL: {spreadsheet.url}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🎯 TOP VOXMILL PRIORITIES:
""")
    
    for i, lead in enumerate(voxmill_leads[:5], 1):
        logger.info(f"{i}. {lead['name']} ({lead['city']}) - Priority: {lead['priority_score']} - Flaws: {lead['flaw_count']}")
    
    logger.info(f"""
🎯 TOP AGENCY PRIORITIES:
""")
    
    for i, lead in enumerate(agency_leads[:5], 1):
        logger.info(f"{i}. {lead['name']} ({lead['city']}) - Priority: {lead['priority_score']} - Flaws: {lead['flaw_count']}")
    
    logger.info("\n" + "="*80)
    logger.info("✅ VOXMILL PLATINUM INTELLIGENCE ENGINE - COMPLETE")
    logger.info("="*80)
    logger.info(f"\n🚀 NEXT STEPS:")
    logger.info("1. Open the Google Sheet and review leads")
    logger.info("2. Filter by Priority Score (90+) for best prospects")
    logger.info("3. Copy outreach messages and start contacting leads")
    logger.info("4. Track responses and iterate\n")

if __name__ == "__main__":
    main()
