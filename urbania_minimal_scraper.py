#!/usr/bin/env python3
"""
Urbania.pe Minimal Scraper - Maximum Compatibility
=================================================

This scraper uses the most basic Chrome options for maximum compatibility
and reliable data extraction from Urbania.pe.

Author: Minimal Scraper Bot
Date: September 2024
"""

import logging
import time
import random
import json
import csv
import re
from datetime import datetime
from typing import Dict, List, Optional, Any
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import os
from urllib.parse import urlparse, unquote
import requests
import argparse
from pathlib import Path

try:
    from google.cloud import storage
except Exception:
    storage = None

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('urbania_minimal_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class MinimalUrbaniaScraper:
    """
    Minimal Urbania.pe scraper with maximum compatibility
    """
    
    def __init__(self):
        self.base_url = "https://urbania.pe/buscar/alquiler-de-departamentos-en-lima"
        self.driver = None
        self.properties = []
        self.current_site_page = 1
        self.global_index_counter = 0
        # image downloading options
        self.download_images = False
        self.images_dir = "images"
        # GCS options
        self.gcs_bucket_name = None
        self.gcs_prefix = "raw_data"
        self.gcp_keyfile = None
        self._gcs_client = None
        self._gcs_bucket = None

    def setup_gcs(self) -> bool:
        """Initialize GCS client and bucket if configured."""
        try:
            if not self.gcs_bucket_name:
                return False
            if storage is None:
                logger.warning("google-cloud-storage not installed. Skipping GCS setup.")
                return False
            if self.gcp_keyfile and Path(self.gcp_keyfile).exists():
                self._gcs_client = storage.Client.from_service_account_json(self.gcp_keyfile)
            else:
                # Fallback to default credentials if available
                self._gcs_client = storage.Client()
            self._gcs_bucket = self._gcs_client.bucket(self.gcs_bucket_name)
            # Lazy existence check
            _ = self._gcs_bucket.exists()
            logger.info(f"☁️ GCS configured for bucket: gs://{self.gcs_bucket_name}/{self.gcs_prefix}")
            return True
        except Exception as e:
            logger.warning(f"⚠️ Could not initialize GCS client: {e}")
            return False

    def upload_to_gcs(self, local_path: str, dest_name: str | None = None) -> bool:
        """Upload a local file to GCS under prefix. Returns True on success."""
        try:
            if not self._gcs_bucket:
                return False
            if not local_path or not os.path.exists(local_path):
                return False
            fname = dest_name or os.path.basename(local_path)
            key = f"{self.gcs_prefix.rstrip('/')}/{fname}" if self.gcs_prefix else fname
            blob = self._gcs_bucket.blob(key)
            blob.upload_from_filename(local_path)
            logger.info(f"☁️ Uploaded to gs://{self.gcs_bucket_name}/{key}")
            return True
        except Exception as e:
            logger.warning(f"⚠️ GCS upload failed for {local_path}: {e}")
            return False

    def setup_minimal_driver(self) -> bool:
        """Setup Chrome driver with absolute minimal options"""
        try:
            logger.info("🚀 Setting up minimal Chrome driver...")
            
            # Absolute minimal options
            options = uc.ChromeOptions()
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            
            # Create driver with minimal configuration
            self.driver = uc.Chrome(options=options)
            self.driver.maximize_window()
            time.sleep(3)
            
            logger.info("✅ Minimal driver setup completed")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to setup driver: {e}")
            return False

    def wait_for_cloudflare(self, timeout: int = 120) -> bool:
        """Simple Cloudflare wait with patience"""
        try:
            logger.info("🛡️ Navigating to Urbania and waiting for Cloudflare...")
            
            self.driver.get(self.base_url)
            time.sleep(10)  # Initial wait
            
            start_time = time.time()
            while time.time() - start_time < timeout:
                try:
                    page_source = self.driver.page_source.lower()
                    
                    # Check if we have property content
                    if ('departamento' in page_source and 
                        'alquiler' in page_source and 
                        len(page_source) > 50000):
                        logger.info("✅ Page loaded successfully!")
                        return True
                    
                    # Check for Cloudflare
                    if 'cloudflare' in page_source or 'just a moment' in page_source:
                        elapsed = int(time.time() - start_time)
                        if elapsed % 20 == 0:  # Log every 20 seconds
                            logger.info(f"⏳ Waiting for Cloudflare... ({elapsed}s/{timeout}s)")
                    
                    time.sleep(5)
                    
                except Exception as e:
                    logger.warning(f"⚠️ Error checking page: {e}")
                    time.sleep(5)
            
            logger.error(f"❌ Timeout after {timeout} seconds")
            return False
            
        except Exception as e:
            logger.error(f"❌ Error in Cloudflare wait: {e}")
            return False

    def extract_properties_simple(self) -> List[Dict[str, Any]]:
        """Simple property extraction"""
        try:
            logger.info("🔍 Starting simple property extraction...")
            
            # Scroll page to load content
            logger.info("📜 Scrolling to load content...")
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(3)
            self.driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(2)
            
            # Upload page source to GCS (cloud-only)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            if self._gcs_bucket:
                try:
                    key = f"{self.gcs_prefix.rstrip('/')}/minimal_page_source_{timestamp}.html"
                    blob = self._gcs_bucket.blob(key)
                    blob.upload_from_string(self.driver.page_source, content_type="text/html")
                    logger.info(f"☁️ Page source uploaded: gs://{self.gcs_bucket_name}/{key}")
                except Exception as e:
                    logger.warning(f"⚠️ Failed to upload page source to GCS: {e}")
            else:
                logger.info("ℹ️ GCS not configured; skipping page source upload.")
            
            # Try multiple selectors to find property elements
            selectors = [
                'div[class*="postingsList-module__card-container"]',
                'div[class*="card-container"]',
                'div[class*="posting-card"]',
                'div[class*="property"]',
                'div[class*="listing"]',
                'article'
            ]
            
            all_elements = []
            for selector in selectors:
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    if elements:
                        all_elements.extend(elements)
                        logger.info(f"📊 Found {len(elements)} elements with: {selector}")
                except Exception as e:
                    logger.warning(f"⚠️ Error with selector {selector}: {e}")
            
            if not all_elements:
                logger.error("❌ No property elements found")
                return []
            
            logger.info(f"📊 Total elements found: {len(all_elements)}")
            
            # Process elements
            properties = []
            processed_texts = set()
            
            for i, element in enumerate(all_elements):
                try:
                    text = element.text.strip()
                    
                    # Skip if empty or duplicate
                    if not text or len(text) < 50 or text in processed_texts:
                        continue
                    
                    # Check if looks like property
                    if self.looks_like_property(text):
                        property_data = self.create_property_data(element, text, i + 1)
                        if property_data:
                            properties.append(property_data)
                            processed_texts.add(text)
                            
                            if len(properties) % 5 == 0:
                                logger.info(f"📈 Extracted {len(properties)} properties so far...")
                
                except Exception as e:
                    logger.warning(f"⚠️ Error processing element {i}: {e}")
                    continue
            
            logger.info(f"✅ Extraction complete: {len(properties)} properties")
            return properties
            
        except Exception as e:
            logger.error(f"❌ Error in extraction: {e}")
            return []

    def looks_like_property(self, text: str) -> bool:
        """Check if text looks like a property listing"""
        text_lower = text.lower()
        
        # Must have property keywords
        property_words = ['alquiler', 'departamento', 'casa', 'm²', 'm2', 'dormitorio', 'baño']
        word_count = sum(1 for word in property_words if word in text_lower)
        
        return word_count >= 2 and len(text) > 100

    def create_property_data(self, element, text: str, index: int) -> Dict[str, Any]:
        """Create property data from element and text"""
        try:
            property_data = {
                'index': index,
                'scraped_at': datetime.now().isoformat(),
                'element_class': element.get_attribute('class') or "",
                'element_tag': element.tag_name,
                'full_text': text,
                'property_type': 'property'
            }
            
            # Basic parsing
            text_lower = text.lower()
            
            # Property type
            if 'casa' in text_lower:
                property_data['property_type'] = 'house'
            elif 'departamento' in text_lower:
                property_data['property_type'] = 'apartment'
            
            # Title (first line with good content)
            lines = [line.strip() for line in text.split('\n') if len(line.strip()) > 10]
            property_data['title'] = lines[0] if lines else "N/A"
            
            # URL
            url_match = re.search(r'https://urbania\.pe/[^\s]+', text)
            property_data['url'] = url_match.group(0) if url_match else "N/A"
            property_data['has_url'] = property_data['url'] != "N/A"
            
            # Price
            price_match = re.search(r'S/\s*([0-9,]+)', text)
            if price_match:
                property_data['price_raw'] = price_match.group(0)
                try:
                    property_data['price_numeric'] = int(price_match.group(1).replace(',', ''))
                    property_data['has_price'] = True
                except:
                    property_data['price_numeric'] = None
                    property_data['has_price'] = False
            else:
                property_data['price_raw'] = "N/A"
                property_data['price_numeric'] = None
                property_data['has_price'] = False
            
            property_data['currency'] = "PEN"
            
            # Location - check for Lima districts
            districts = [
                'miraflores', 'san isidro', 'barranco', 'surco', 'la molina',
                'san borja', 'magdalena', 'pueblo libre', 'jesús maría',
                'lince', 'la victoria', 'chorrillos', 'san miguel'
            ]
            
            property_data['location'] = "Lima"
            property_data['has_location'] = True
            
            for district in districts:
                if district in text_lower:
                    property_data['location'] = district.title()
                    break
            
            # Area
            area_match = re.search(r'(\d+)\s*m[²2]', text_lower)
            if area_match:
                property_data['area_raw'] = area_match.group(0)
                property_data['area_numeric'] = int(area_match.group(1))
            else:
                property_data['area_raw'] = "N/A"
                property_data['area_numeric'] = None
            
            # Bedrooms
            bedroom_match = re.search(r'(\d+)\s*(?:dormitorio|habitacion|dorm)', text_lower)
            property_data['bedrooms'] = int(bedroom_match.group(1)) if bedroom_match else None
            
            # Bathrooms
            bathroom_match = re.search(r'(\d+)\s*baño', text_lower)
            property_data['bathrooms'] = int(bathroom_match.group(1)) if bathroom_match else None
            
            # Simple features
            property_data['has_parking'] = any(word in text_lower for word in ['estacionamiento', 'cochera', 'garage'])
            property_data['parking_count'] = 1 if property_data['has_parking'] else 0
            property_data['has_pool'] = 'piscina' in text_lower
            property_data['has_garden'] = any(word in text_lower for word in ['jardín', 'jardin', 'área verde'])
            property_data['has_balcony'] = any(word in text_lower for word in ['balcón', 'terraza'])
            property_data['has_elevator'] = 'ascensor' in text_lower
            property_data['has_security'] = any(word in text_lower for word in ['seguridad', 'vigilancia'])
            property_data['has_gym'] = 'gimnasio' in text_lower
            property_data['is_furnished'] = 'amoblado' in text_lower
            property_data['allows_pets'] = 'mascota' in text_lower
            property_data['is_new'] = any(word in text_lower for word in ['nuevo', 'estreno'])
            property_data['has_terrace'] = 'terraza' in text_lower
            property_data['has_laundry'] = 'lavandería' in text_lower
            property_data['has_air_conditioning'] = any(word in text_lower for word in ['aire acondicionado', 'climatizado'])
            
            # Phone
            phone_match = re.search(r'(\+51\s*[0-9\s\-]{8,})', text)
            property_data['phone'] = phone_match.group(1) if phone_match else "N/A"
            
            # Additional fields - image URLs
            image_urls = self.extract_image_urls_from_element(element)
            property_data['image_urls'] = image_urls
            property_data['image_count'] = len(image_urls)
            
            # Price per sqm
            if property_data['price_numeric'] and property_data['area_numeric']:
                property_data['price_per_sqm'] = round(property_data['price_numeric'] / property_data['area_numeric'], 2)
            else:
                property_data['price_per_sqm'] = None
            
            # Data completeness
            key_fields = [
                property_data['has_price'],
                property_data['location'] != "Lima",
                property_data['area_numeric'] is not None,
                property_data['bedrooms'] is not None,
                property_data['bathrooms'] is not None,
                property_data['has_url']
            ]
            property_data['data_completeness'] = sum(key_fields) / len(key_fields) * 100
            
            # Feature count
            boolean_features = [
                'has_parking', 'has_pool', 'has_garden', 'has_balcony',
                'has_elevator', 'has_security', 'has_gym', 'is_furnished',
                'allows_pets', 'is_new', 'has_terrace', 'has_laundry',
                'has_air_conditioning'
            ]
            property_data['feature_count'] = sum(property_data.get(f, False) for f in boolean_features)
            
            # Page info
            property_data['page'] = 1
            property_data['site_page'] = 1
            property_data['global_index'] = index
            
            return property_data
            
        except Exception as e:
            logger.warning(f"⚠️ Error creating property data: {e}")
            return None

    def save_simple_results(self, properties: List[Dict[str, Any]]):
        """Upload results to GCS in CSV and JSON (no local files)."""
        try:
            if not properties:
                logger.warning("⚠️ No properties to save")
                return
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            if not self._gcs_bucket:
                logger.warning("⚠️ GCS not configured; cannot upload results.")
                return

            # CSV (in-memory)
            try:
                if properties:
                    fieldnames = list(properties[0].keys())
                else:
                    fieldnames = []
                from io import StringIO
                csv_buf = StringIO()
                writer = csv.DictWriter(csv_buf, fieldnames=fieldnames)
                writer.writeheader()
                for prop in properties:
                    writer.writerow(prop)
                csv_key = f"{self.gcs_prefix.rstrip('/')}/urbania_minimal_results_{timestamp}.csv"
                self._gcs_bucket.blob(csv_key).upload_from_string(csv_buf.getvalue(), content_type="text/csv")
                logger.info(f"☁️ CSV uploaded: gs://{self.gcs_bucket_name}/{csv_key}")
            except Exception as e:
                logger.warning(f"⚠️ Failed to upload CSV to GCS: {e}")

            # JSON (in-memory)
            try:
                json_key = f"{self.gcs_prefix.rstrip('/')}/urbania_minimal_results_{timestamp}.json"
                json_str = json.dumps(properties, ensure_ascii=False, indent=2)
                self._gcs_bucket.blob(json_key).upload_from_string(json_str, content_type="application/json; charset=utf-8")
                logger.info(f"☁️ JSON uploaded: gs://{self.gcs_bucket_name}/{json_key}")
            except Exception as e:
                logger.warning(f"⚠️ Failed to upload JSON to GCS: {e}")
            
            # Print summary
            self.print_summary(properties)
            
        except Exception as e:
            logger.error(f"❌ Error saving: {e}")

    def print_summary(self, properties: List[Dict[str, Any]]):
        """Print results summary"""
        total = len(properties)
        if total == 0:
            return
        
        with_prices = sum(1 for p in properties if p.get('has_price', False))
        with_locations = sum(1 for p in properties if p.get('location', 'Lima') != 'Lima')
        with_areas = sum(1 for p in properties if p.get('area_numeric'))
        with_urls = sum(1 for p in properties if p.get('has_url', False))
        
        logger.info("📊 EXTRACTION SUMMARY")
        logger.info(f"Total properties: {total}")
        logger.info(f"With prices: {with_prices} ({with_prices/total*100:.1f}%)")
        logger.info(f"With specific locations: {with_locations} ({with_locations/total*100:.1f}%)")
        logger.info(f"With areas: {with_areas} ({with_areas/total*100:.1f}%)")
        logger.info(f"With URLs: {with_urls} ({with_urls/total*100:.1f}%)")
        
        if with_prices > 0:
            prices = [p['price_numeric'] for p in properties if p.get('price_numeric')]
            if prices:
                avg_price = sum(prices) / len(prices)
                logger.info(f"Average price: S/ {avg_price:,.0f}")

    def run_minimal_scraper(self):
        """Run the minimal scraper"""
        try:
            logger.info("🚀 Starting Minimal Urbania.pe Scraper...")
            
            # Setup driver
            if not self.setup_minimal_driver():
                return
            
            # Wait for Cloudflare
            if not self.wait_for_cloudflare():
                return
            
            # Extract across multiple pages
            max_pages = 50
            if getattr(self, '_override_max_pages', None):
                max_pages = int(self._override_max_pages)
            all_properties: List[Dict[str, Any]] = []
            for page_num in range(1, max_pages + 1):
                logger.info(f"🧭 Extracting page {page_num}/{max_pages}")
                self.current_site_page = page_num
                page_properties = self.extract_properties_simple()

                # Attach pagination metadata and global indices
                for idx, prop in enumerate(page_properties, start=1):
                    self.global_index_counter += 1
                    prop['page'] = page_num
                    prop['site_page'] = page_num
                    prop['global_index'] = self.global_index_counter
                
                all_properties.extend(page_properties)

                # Try to go to next page unless last iteration
                if page_num < max_pages:
                    if not self.go_to_next_page():
                        logger.info("ℹ️ No next page found. Stopping pagination.")
                        break
                
            if all_properties:
                # Optionally download images
                if self.download_images:
                    self.download_images_for_properties(all_properties)
                self.save_simple_results(all_properties)
                logger.info(f"✅ Success! Extracted {len(all_properties)} properties from {self.current_site_page} page(s)")
            else:
                logger.warning("⚠️ No properties extracted")
                
        except Exception as e:
            logger.error(f"❌ Scraper error: {e}")
        
        finally:
            if self.driver:
                logger.info("🔒 Closing browser...")
                try:
                    self.driver.quit()
                except:
                    pass

    def go_to_next_page(self, timeout: int = 60) -> bool:
        """Click the next page control and wait for content to change.
        Returns True if navigation succeeds, False if no next page or timeout."""
        try:
            logger.info("➡️  Attempting to navigate to next page...")
            previous_signature = self._page_signature()

            # Common next-page selectors on Urbania
            next_selectors = [
                "a.paging-module__page-arrow[data-qa='PAGING_NEXT']",
                "[data-qa='PAGING_NEXT']",
                "a[rel='next']",
                "a[aria-label*='Siguiente']",
                "button[aria-label*='Siguiente']",
                "a[aria-label*='Next']",
                "li.paging-module__page-arrow a"
            ]

            next_elem = None
            for sel in next_selectors:
                try:
                    elems = self.driver.find_elements(By.CSS_SELECTOR, sel)
                    if elems:
                        next_elem = elems[-1]
                        logger.info(f"🔗 Next button found with selector: {sel}")
                        break
                except Exception:
                    continue

            if not next_elem:
                logger.info("⛔ Next page control not found.")
                return False

            # Scroll into view and click via JS to avoid intercept issues
            try:
                self.driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", next_elem)
                time.sleep(1)
                self.driver.execute_script("arguments[0].click();", next_elem)
            except Exception as e:
                logger.warning(f"⚠️ Error clicking next: {e}")
                try:
                    next_elem.click()
                except Exception as e2:
                    logger.error(f"❌ Fallback click failed: {e2}")
                    return False

            # Wait for page content to change
            start = time.time()
            while time.time() - start < timeout:
                time.sleep(2)
                current_signature = self._page_signature()
                if current_signature != previous_signature and len(self.driver.page_source) > 50000:
                    logger.info("✅ Page navigation detected.")
                    return True
            logger.warning("⌛ Timeout waiting for next page content.")
            return False
        except Exception as e:
            logger.error(f"❌ Error during pagination: {e}")
            return False

    def _page_signature(self) -> str:
        """Generate a lightweight signature for current page content."""
        try:
            src = self.driver.page_source
            return f"{len(src)}::{src[:3000]}"
        except Exception:
            return ""

    def extract_image_urls_from_element(self, element) -> List[str]:
        """Extract image URLs from a property element.
        Looks into <img> tags (src, data-src, srcset, etc.), anchors to images, and CSS background images.
        """
        urls: List[str] = []
        seen = set()

        def _add(url: Optional[str]):
            if not url:
                return
            u = url.strip()
            if not u:
                return
            # Normalize protocol-relative URLs
            if u.startswith('//'):
                u = 'https:' + u
            # Filter obvious non-image trackers
            if any(token in u.lower() for token in [
                'sprite', 'icon', 'logo', 'placeholder', 'tracking', 'analytics']):
                pass  # still allow; filtering can be adjusted if needed
            if u not in seen:
                seen.add(u)
                urls.append(u)

        try:
            # <img> tags
            img_elems = element.find_elements(By.CSS_SELECTOR, 'img')
            for img in img_elems:
                for attr in ['src', 'data-src', 'data-lazy', 'data-original']:
                    _add(img.get_attribute(attr))
                # srcset may contain multiple URLs
                srcset = img.get_attribute('srcset') or img.get_attribute('data-srcset')
                if srcset:
                    for part in srcset.split(','):
                        u = part.strip().split(' ')[0]
                        _add(u)
        except Exception:
            pass

        try:
            # Anchors linking directly to images
            a_elems = element.find_elements(By.CSS_SELECTOR, 'a[href]')
            for a in a_elems:
                href = a.get_attribute('href')
                if href and any(href.lower().endswith(ext) for ext in ['.jpg', '.jpeg', '.png', '.webp']):
                    _add(href)
        except Exception:
            pass

        try:
            # Background images in style attributes
            styled = element.find_elements(By.CSS_SELECTOR, '*[style*="background"]')
            bg_url_pattern = re.compile(r'url\((\"|\')?(?P<u>[^)\"\']+)', re.IGNORECASE)
            for el in styled:
                style = el.get_attribute('style') or ''
                for m in bg_url_pattern.finditer(style):
                    _add(m.group('u'))
        except Exception:
            pass

        return urls

    def download_images_for_properties(self, properties: List[Dict[str, Any]]):
        """Download images for each property into a structured directory."""
        try:
            os.makedirs(self.images_dir, exist_ok=True)
        except Exception as e:
            logger.warning(f"⚠️ Could not create images directory: {e}")
            return

        session = requests.Session()
        # Try to mirror the browser headers & cookies to avoid CDN 403
        try:
            ua = self.driver.execute_script("return navigator.userAgent") or "Mozilla/5.0"
        except Exception:
            ua = "Mozilla/5.0"
        referer = self.driver.current_url if self.driver else self.base_url
        session.headers.update({
            "User-Agent": ua,
            "Referer": referer,
            "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
            "Accept-Language": "es-PE,es;q=0.9,en;q=0.8",
            "Connection": "keep-alive"
        })
        try:
            for c in self.driver.get_cookies():
                try:
                    session.cookies.set(c.get('name'), c.get('value'), domain=c.get('domain'))
                except Exception:
                    continue
        except Exception:
            pass

        def _safe_filename_from_url(url: str) -> str:
            try:
                path = urlparse(url).path
                name = os.path.basename(path) or "image"
                return unquote(name.split('?')[0])
            except Exception:
                return "image"

        total = len(properties)
        for i, prop in enumerate(properties, start=1):
            try:
                prop_id = prop.get('global_index') or prop.get('index') or i
                prop_dir = os.path.join(self.images_dir, str(prop_id))
                os.makedirs(prop_dir, exist_ok=True)
                urls = prop.get('image_urls') or []
                saved = 0
                for j, url in enumerate(urls, start=1):
                    try:
                        if not url or not url.startswith('http'):
                            continue
                        fname = _safe_filename_from_url(url)
                        # ensure unique name
                        target_path = os.path.join(prop_dir, f"{j:02d}_{fname}")
                        # skip if exists
                        if os.path.exists(target_path):
                            saved += 1
                            continue
                        resp = session.get(url, timeout=25)
                        ctype = (resp.headers.get('Content-Type') or '').lower()
                        if resp.status_code == 200 and resp.content and ('image' in ctype or any(fname.lower().endswith(ext) for ext in ['.jpg','.jpeg','.png','.webp'])):
                            with open(target_path, 'wb') as out:
                                out.write(resp.content)
                            saved += 1
                        else:
                            # One retry tweaking headers
                            retry_headers = {"Referer": self.base_url}
                            resp2 = session.get(url, timeout=25, headers=retry_headers)
                            ctype2 = (resp2.headers.get('Content-Type') or '').lower()
                            if resp2.status_code == 200 and resp2.content and ('image' in ctype2 or any(fname.lower().endswith(ext) for ext in ['.jpg','.jpeg','.png','.webp'])):
                                with open(target_path, 'wb') as out:
                                    out.write(resp2.content)
                                saved += 1
                    except Exception:
                        continue
                logger.info(f"🖼️  [{i}/{total}] Property {prop_id}: saved {saved}/{len(urls)} images")
            except Exception as e:
                logger.warning(f"⚠️ Error downloading images for property {i}: {e}")

    def download_image_via_browser(self, url: str, target_path: str) -> bool:
        """Fallback: open the image URL in a new tab via window.open (keeps referrer),
        then capture a full-page screenshot as PNG.
        """
        try:
            if not url.startswith('http'):
                return False
            original = self.driver.current_window_handle
            # Open in a real tab so referrer is preserved
            self.driver.execute_script("window.open(arguments[0], '_blank');", url)
            WebDriverWait(self.driver, 10).until(lambda d: len(d.window_handles) > 1)
            new_handle = [h for h in self.driver.window_handles if h != original][-1]
            self.driver.switch_to.window(new_handle)
            # Wait a bit for image to render
            time.sleep(2)
            # Take full tab screenshot
            png = self.driver.get_screenshot_as_png()
            os.makedirs(os.path.dirname(target_path), exist_ok=True)
            with open(target_path, 'wb') as f:
                f.write(png)
            # Close the tab and return
            self.driver.close()
            self.driver.switch_to.window(original)
            return True
        except Exception:
            try:
                if len(self.driver.window_handles) > 0:
                    self.driver.switch_to.window(self.driver.window_handles[0])
            except Exception:
                pass
            return False

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Urbania Minimal Scraper')
    parser.add_argument('--download-images', action='store_true', help='Download property images to disk')
    parser.add_argument('--images-dir', type=str, default='images', help='Directory to save downloaded images')
    parser.add_argument('--max-pages', type=int, default=None, help='Override number of pages to traverse')
    parser.add_argument('--gcs-bucket', type=str, default='urbania_scrapper', help='GCS bucket name')
    parser.add_argument('--gcs-prefix', type=str, default='raw_data', help='GCS prefix (folder)')
    parser.add_argument('--gcp-keyfile', type=str, default=None, help='Path to GCP service account JSON key')
    args = parser.parse_args()

    scraper = MinimalUrbaniaScraper()
    scraper.download_images = bool(args.download_images)
    scraper.images_dir = args.images_dir or scraper.images_dir
    scraper.gcs_bucket_name = args.gcs_bucket
    scraper.gcs_prefix = args.gcs_prefix or scraper.gcs_prefix
    scraper.gcp_keyfile = args.gcp_keyfile
    scraper.setup_gcs()

    if args.max_pages is not None and args.max_pages > 0:
        # Monkey-patch max_pages by setting an attribute and reading it in run
        scraper._override_max_pages = args.max_pages
    else:
        scraper._override_max_pages = None

    scraper.run_minimal_scraper()

if __name__ == "__main__":
    main()
