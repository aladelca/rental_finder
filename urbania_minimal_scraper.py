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
            
            # Save page source for debugging
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            with open(f'minimal_page_source_{timestamp}.html', 'w', encoding='utf-8') as f:
                f.write(self.driver.page_source)
            logger.info(f"💾 Page source saved for debugging")
            
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
            
            # Additional fields
            property_data['image_urls'] = []
            property_data['image_count'] = 0
            
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
        """Save results in CSV and JSON"""
        try:
            if not properties:
                logger.warning("⚠️ No properties to save")
                return
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # CSV
            csv_filename = f'urbania_minimal_results_{timestamp}.csv'
            with open(csv_filename, 'w', newline='', encoding='utf-8') as f:
                if properties:
                    fieldnames = list(properties[0].keys())
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    for prop in properties:
                        writer.writerow(prop)
            logger.info(f"💾 CSV saved: {csv_filename}")
            
            # JSON
            json_filename = f'urbania_minimal_results_{timestamp}.json'
            with open(json_filename, 'w', encoding='utf-8') as f:
                json.dump(properties, f, ensure_ascii=False, indent=2)
            logger.info(f"💾 JSON saved: {json_filename}")
            
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

def main():
    """Main function"""
    scraper = MinimalUrbaniaScraper()
    scraper.run_minimal_scraper()

if __name__ == "__main__":
    main()
