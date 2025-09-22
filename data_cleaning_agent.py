#!/usr/bin/env python3
"""
Data Cleaning Agent for Urbania Property Data
===========================================

This agent analyzes property data from the JSON file and uses OpenAI's GPT API 
to correct and complete missing information based on the full_text field.

Author: Data Cleaning Agent
Date: September 2024
"""

import json
import os
import re
import time
from typing import Dict, List, Optional, Any
from datetime import datetime
import openai
from openai import OpenAI
from dotenv import load_dotenv
from pathlib import Path

try:
    from google.cloud import storage
except Exception:
    storage = None

# Load environment variables from .env file
load_dotenv()

# Lima Districts List for location validation
LIMA_DISTRICTS = [
    "Ancón", "Ate", "Barranco", "Breña", "Carabayllo", "Chaclacayo", "Chorrillos",
    "Cieneguilla", "Comas", "El Agustino", "Independencia", "Jesús María",
    "La Molina", "La Victoria", "Lima", "Lince", "Los Olivos", "Lurigancho",
    "Lurín", "Magdalena del Mar", "Miraflores", "Pachacámac", "Pucusana",
    "Pueblo Libre", "Puente Piedra", "Punta Hermosa", "Punta Negra",
    "Rímac", "San Bartolo", "San Borja", "San Isidro", "San Juan de Lurigancho",
    "San Juan de Miraflores", "San Luis", "San Martín de Porres", "San Miguel",
    "Santa Anita", "Santa María del Mar", "Santa Rosa", "Santiago de Surco",
    "Surquillo", "Villa El Salvador", "Villa María del Triunfo"
]

class PropertyDataCleaner:
    """Agent that cleans and completes property data using OpenAI API."""
    
    def __init__(self, api_key: str, model: str = "gpt-3.5-turbo"):
        """Initialize the cleaning agent with OpenAI API key."""
        self.client = OpenAI(api_key=api_key)
        self.model = model
        self.processed_count = 0
        self.errors = []
        # GCS config (defaults can be overridden from main)
        self.gcs_bucket_name: str | None = os.getenv('GCS_BUCKET', 'urbania_scrapper')
        self.gcs_prefix: str = os.getenv('GCS_PREFIX', 'clean_data')
        self.gcp_keyfile: str | None = os.getenv('GCP_KEYFILE')
        self.cloud_only: bool = str(os.getenv('CLOUD_ONLY', 'false')).lower() in ('1', 'true', 'yes')
        self._gcs_client = None
        self._gcs_bucket = None

    def setup_gcs(self) -> bool:
        """Initialize GCS client and bucket if library and config are available."""
        try:
            if not self.gcs_bucket_name:
                return False
            if storage is None:
                print("WARNING: google-cloud-storage not installed. Skipping GCS setup.")
                return False
            if self.gcp_keyfile and Path(self.gcp_keyfile).exists():
                self._gcs_client = storage.Client.from_service_account_json(self.gcp_keyfile)
            else:
                self._gcs_client = storage.Client()
            self._gcs_bucket = self._gcs_client.bucket(self.gcs_bucket_name)
            _ = self._gcs_bucket.exists()
            print(f"GCS configured: gs://{self.gcs_bucket_name}/{self.gcs_prefix}")
            return True
        except Exception as e:
            print(f"WARNING: GCS setup failed: {e}")
            return False

    def _gcs_upload_json_string(self, json_text: str, key_name: str) -> bool:
        try:
            if not self._gcs_bucket:
                return False
            key = f"{self.gcs_prefix.rstrip('/')}/{key_name}"
            blob = self._gcs_bucket.blob(key)
            blob.upload_from_string(json_text, content_type='application/json; charset=utf-8')
            print(f"Uploaded to gs://{self.gcs_bucket_name}/{key}")
            return True
        except Exception as e:
            print(f"WARNING: GCS upload failed for {key_name}: {e}")
            return False

    def _load_properties_from_source(self, source: str):
        """Load JSON list/dict from local path or GCS gs://bucket/path URI."""
        try:
            if source.startswith('gs://'):
                if storage is None:
                    raise RuntimeError("google-cloud-storage not installed for GCS input")
                # Parse gs://bucket/key
                without = source[len('gs://'):]
                parts = without.split('/', 1)
                if len(parts) != 2:
                    raise ValueError(f"Invalid GCS URI: {source}")
                bkt, key = parts[0], parts[1]
                # Ensure client
                client = self._gcs_client
                if client is None:
                    if self.gcp_keyfile and Path(self.gcp_keyfile).exists():
                        client = storage.Client.from_service_account_json(self.gcp_keyfile)
                    else:
                        client = storage.Client()
                bucket = client.bucket(bkt)
                blob = bucket.blob(key)
                text = blob.download_as_text()
                data = json.loads(text)
            else:
                with open(source, 'r', encoding='utf-8') as f:
                    data = json.load(f)
            # Normalize to list
            if isinstance(data, list):
                return data
            elif isinstance(data, dict):
                return [data]
            else:
                raise ValueError("Input JSON must be a list or object")
        except Exception as e:
            print(f"Error loading input from {source}: {e}")
            return []
        
    def create_analysis_prompt(self, property_data: Dict[str, Any]) -> str:
        """Create a detailed prompt for GPT to analyze property data."""
        
        full_text = property_data.get('full_text', '')
        current_location = property_data.get('location', '')
        
        prompt = f"""
Analiza el siguiente texto de una propiedad inmobiliaria en Lima, Perú y extrae/corrige la información solicitada.

TEXTO DE LA PROPIEDAD:
{full_text}

UBICACIÓN ACTUAL: {current_location}

DISTRITOS DE LIMA VÁLIDOS:
{', '.join(LIMA_DISTRICTS)}

Necesito que analices el texto y me proporciones la siguiente información en formato JSON EXACTO:

{{
    "price_raw": "precio como aparece en el texto (ej: 'S/ 250,000', 'USD 180,000', 'Consultar precio') o 'N/A'",
    "price_numeric": número sin comas ni símbolos o null, si hay precio en dolares y soles, usar el precio en soles. Tipicamente el precio debería estar entre 10 y 40000, si excede, revisa el formato y corrige los errores
    "property_type": identifica si es casa o departamento (solamente puede tener esos valores),
    "currency": "PEN", "USD" o "EUR",
    "has_price": true/false,
    "location": "distrito específico de Lima encontrado en el texto o 'Lima' si no se especifica",
    "has_location": true/false,
    "area_raw": "área como aparece en el texto (ej: '120 m²', '80 m² tot.') o 'N/A'",
    "area_numeric": número del área en m² o null,
    "bedrooms": número de dormitorios o null,
    "bathrooms": número de baños o null,
    "has_parking": true/false (buscar palabras como 'cochera', 'estacionamiento', 'garage'),
    "parking_count": número de estacionamientos o 0,
    "has_pool": true/false (buscar 'piscina', 'pool'),
    "has_garden": true/false (buscar 'jardín', 'garden', 'área verde'),
    "has_balcony": true/false (buscar 'balcón', 'balcony'),
    "has_elevator": true/false (buscar 'ascensor', 'elevator'),
    "has_security": true/false (buscar 'seguridad', 'security', 'portero', 'vigilancia'),
    "has_gym": true/false (buscar 'gimnasio', 'gym'),
    "is_furnished": true/false (buscar 'amoblado', 'furnished', 'amueblado'),
    "allows_pets": true/false (buscar 'mascotas', 'pets'),
    "is_new": true/false (buscar 'estreno', 'nuevo', 'new', 'en planos'),
    "has_terrace": true/false (buscar 'terraza', 'terrace'),
    "has_laundry": true/false (buscar 'lavandería', 'laundry'),
    "has_air_conditioning": true/false (buscar 'aire acondicionado', 'A/C', 'AC', 'climatizado')
    "full_text": texto completo de la propiedad, tal como lo recibiste, no se debe modificar.
}}

IMPORTANTE:
- Para location, usa EXACTAMENTE uno de los distritos de la lista si lo encuentras en el texto
- Si encuentras "Santa Catalina, La Victoria", el distrito es "La Victoria"
- Para precios, extrae el número completo (ej: si dice "S/ 250,000" → price_numeric: 250000)
- Para áreas, extrae solo el número en m² (ej: si dice "120 m²" → area_numeric: 120)
- Busca sinónimos y variaciones de las características (ej: "cochera" = parking)
- Si no encuentras información específica, usa null para números y false para booleanos

Responde SOLO con el JSON, sin explicaciones adicionales.
"""
        return prompt

    def analyze_property_with_gpt(self, property_data: Dict[str, Any]) -> Dict[str, Any]:
        """Use GPT to analyze and correct property data."""
        
        try:
            prompt = self.create_analysis_prompt(property_data)
            
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "Eres un experto analista de datos inmobiliarios. Analiza textos de propiedades y extrae información estructurada con precisión."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=1000,
                temperature=0.1
            )
            
            # Extract JSON from response
            content = response.choices[0].message.content.strip()
            
            # Try to parse JSON from response
            try:
                # Clean the response to extract JSON
                json_match = re.search(r'\{.*\}', content, re.DOTALL)
                if json_match:
                    corrected_data = json.loads(json_match.group())
                else:
                    raise ValueError("No JSON found in response")
                    
            except json.JSONDecodeError as e:
                print(f"Error parsing JSON response: {e}")
                print(f"Response content: {content}")
                return property_data  # Return original if parsing fails
            
            return corrected_data
            
        except Exception as e:
            error_msg = f"Error analyzing property {property_data.get('index', 'unknown')}: {str(e)}"
            print(error_msg)
            self.errors.append(error_msg)
            return property_data  # Return original data if API call fails

    def update_property_data(self, original: Dict[str, Any], corrected: Dict[str, Any]) -> Dict[str, Any]:
        """Update original property data with corrected information."""
        
        # Keep all original fields
        updated = original.copy()
        
        # Update with corrected data
        fields_to_update = [
            'price_raw', 'price_numeric', 'currency', 'has_price',
            'location', 'has_location', 'area_raw', 'area_numeric',
            'bedrooms', 'bathrooms', 'has_parking', 'parking_count',
            'has_pool', 'has_garden', 'has_balcony', 'has_elevator',
            'has_security', 'has_gym', 'is_furnished', 'allows_pets',
            'is_new', 'has_terrace', 'has_laundry', 'has_air_conditioning'
        ]
        
        for field in fields_to_update:
            if field in corrected and corrected[field] is not None:
                updated[field] = corrected[field]
        
        # Recalculate data completeness
        total_fields = len(fields_to_update)
        complete_fields = sum(1 for field in fields_to_update 
                            if updated.get(field) not in [None, "N/A", "", False])
        updated['data_completeness'] = (complete_fields / total_fields) * 100
        
        # Recalculate feature count (boolean features that are True)
        boolean_features = [
            'has_parking', 'has_pool', 'has_garden', 'has_balcony',
            'has_elevator', 'has_security', 'has_gym', 'is_furnished',
            'allows_pets', 'is_new', 'has_terrace', 'has_laundry',
            'has_air_conditioning'
        ]
        updated['feature_count'] = sum(1 for feature in boolean_features 
                                     if updated.get(feature, False))
        
        return updated

    def process_batch(self, properties: List[Dict[str, Any]], 
                     start_index: int = 0, batch_size: int = 10,
                     delay: float = 1.0) -> List[Dict[str, Any]]:
        """Process a batch of properties with rate limiting."""
        
        cleaned_properties = []
        total = len(properties)
        
        for i, property_data in enumerate(properties[start_index:], start_index):
            try:
                print(f"Processing property {i+1}/{total} (Index: {property_data.get('index', 'N/A')})")
                
                # Skip if no full_text or empty
                if not property_data.get('full_text', '').strip():
                    print(f"  Skipping - no full_text content")
                    cleaned_properties.append(property_data)
                    continue
                
                # Analyze with GPT
                corrected_data = self.analyze_property_with_gpt(property_data)
                
                # Update property with corrected data
                updated_property = self.update_property_data(property_data, corrected_data)
                cleaned_properties.append(updated_property)
                
                self.processed_count += 1
                
                # Rate limiting
                if delay > 0:
                    time.sleep(delay)
                
                # Save progress periodically
                if (i + 1) % batch_size == 0:
                    self.save_progress(cleaned_properties, i + 1)
                    
            except Exception as e:
                error_msg = f"Error processing property {i}: {str(e)}"
                print(error_msg)
                self.errors.append(error_msg)
                cleaned_properties.append(property_data)  # Keep original on error
        
        return cleaned_properties

    def save_progress(self, properties: List[Dict[str, Any]], processed_count: int):
        """Save progress to a temporary file."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"cleaned_data/cleaned_data_progress_{timestamp}_batch_{processed_count}.json"
        
        try:
            # Build JSON once
            json_text = json.dumps(properties, ensure_ascii=False, indent=2)
            # Local save (unless cloud-only)
            if not self.cloud_only:
                with open(filename, 'w', encoding='utf-8') as f:
                    f.write(json_text)
                print(f"  Progress saved: {filename}")
            # GCS upload under progress/
            gcs_name = f"progress/cleaned_data_progress_{timestamp}_batch_{processed_count}.json"
            self._gcs_upload_json_string(json_text, gcs_name)
        except Exception as e:
            print(f"  Error saving progress: {e}")

    def clean_data(self, input_file: str, output_file: str = None, 
                   start_index: int = 0, max_properties: int = None,
                   batch_size: int = 10, delay: float = 1.0):
        """Main method to clean the property data."""
        
        print(f"=== Data Cleaning Agent Started ===")
        print(f"Input file: {input_file}")
        print(f"Model: {self.model}")
        print(f"Start index: {start_index}")
        print(f"Max properties: {max_properties or 'All'}")
        print(f"Batch size: {batch_size}")
        print(f"Delay: {delay}s")
        print(f"=================================")
        
        # Load data (supports local path or gs:// URI)
        properties = self._load_properties_from_source(input_file)
        if not properties:
            print("No input properties loaded. Aborting.")
            return
        print(f"Loaded {len(properties)} properties from {input_file}")
        
        # Limit properties if specified
        if max_properties:
            properties = properties[:max_properties]
            print(f"Limited to first {max_properties} properties")
        
        # Process properties
        start_time = time.time()
        cleaned_properties = self.process_batch(
            properties, start_index, batch_size, delay
        )
        end_time = time.time()
        
        # Generate output filename if not provided
        if output_file is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"cleaned_urbania_data_{timestamp}.json"
        
        # Save results (local optional) and upload to GCS
        try:
            json_text = json.dumps(cleaned_properties, ensure_ascii=False, indent=2)
            if not self.cloud_only:
                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write(json_text)
                print(f"Results saved to: {output_file}")
            # Upload to GCS at root prefix
            self._gcs_upload_json_string(json_text, Path(output_file).name)
        except Exception as e:
            print(f"Error saving/uploading results: {e}")
            return
        
        # Print summary
        print(f"\n=== Cleaning Summary ===")
        print(f"Total properties: {len(properties)}")
        print(f"Successfully processed: {self.processed_count}")
        print(f"Errors: {len(self.errors)}")
        print(f"Processing time: {end_time - start_time:.2f} seconds")
        print(f"Output file: {output_file}")
        
        if self.errors:
            print(f"\nErrors encountered:")
            for error in self.errors[:5]:  # Show first 5 errors
                print(f"  - {error}")
            if len(self.errors) > 5:
                print(f"  ... and {len(self.errors) - 5} more errors")


def main():
    """Main function to run the data cleaning agent."""
    
    # Configuration
    API_KEY = os.getenv('OPENAI_API_KEY')
    if not API_KEY:
        print("ERROR: Please set your OPENAI_API_KEY environment variable")
        print("\nOptions to set your API key:")
        print("1. Create a .env file:")
        print("   cp env.example .env")
        print("   # Then edit .env with your actual API key")
        print("\n2. Set environment variable in your shell:")
        print("   export OPENAI_API_KEY='your_api_key_here'")
        print("\n3. Set it temporarily for this session:")
        print("   OPENAI_API_KEY='your_api_key_here' python data_cleaning_agent.py")
        return
    
    MODEL = os.getenv('OPENAI_MODEL', 'gpt-3.5-turbo')
    # Input can be local path or GCS URI (gs://bucket/path). If INPUT_GCS_URI is set, it overrides.
    INPUT_FILE = os.getenv('INPUT_GCS_URI') or os.getenv('INPUT_FILE', "urbania_minimal_results_20250920_190749.json")
    
    # Initialize cleaning agent
    cleaner = PropertyDataCleaner(api_key=API_KEY, model=MODEL)
    cleaner.setup_gcs()
    
    # Configuration for testing - start with a small batch
    START_INDEX = 0
    MAX_PROPERTIES = 100000  # Start with 20 properties for testing
    BATCH_SIZE = 5
    DELAY = 1.0  # 1 second delay between API calls to avoid rate limits
    
    print("Starting with a test batch of 20 properties...")
    print("You can modify MAX_PROPERTIES in the script to process more data.")
    
    # Clean the data
    cleaner.clean_data(
        input_file=INPUT_FILE,
        start_index=START_INDEX,
        max_properties=MAX_PROPERTIES,
        batch_size=BATCH_SIZE,
        delay=DELAY
    )


if __name__ == "__main__":
    main()
