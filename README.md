# Urbania.pe Data Pipeline (Scrape → Clean → Predict)

End-to-end project to extract property data from Urbania.pe, clean it with an AI data-cleaning agent, and (WIP) generate predictions.

## Features

- **Cloudflare Protection Handling**: Detects and handles Cloudflare challenges
- **Comprehensive Data Extraction**: Extracts property details including price, location, amenities
- **Pagination**: Clicks "Siguiente" and aggregates listings across multiple pages
- **Detail Page Scraping**: Optional deep scraping of individual property pages
- **Cloud-first Storage**: Scraper and agents can read/write directly to Google Cloud Storage (GCS)
- **Multiple Export Formats**: Saves data as CSV and JSON (locally or in GCS)
- **Robust Error Handling**: Continues scraping even if individual listings fail
- **Stealth Mode**: Configured to avoid detection as automated browser
- **Logging**: Comprehensive logging for debugging and monitoring
- **AI Data Cleaning Agent**: Uses OpenAI to complete/correct fields from `full_text`
- **Predictions (WIP)**: Placeholder to build modeling on the cleaned dataset

## Installation

1. **Prerequisites**:
   - Python 3.8+
   - Google Chrome browser
   - Internet connection

2. **Install dependencies** (recommended):
   ```bash
   pip install -r requirements_full.txt
   ```
   If you see Python 3.12/3.13 compatibility errors, run:
   ```bash
   python3 fix_python313_compatibility.py
   ```

## Usage

### End-to-End Pipeline (Scrape → Clean → Predict)

1) Scrape (con paginación) → guarda en GCS:
```bash
python urbania_minimal_scraper.py \
  --gcs-bucket urbania_scrapper \
  --gcs-prefix raw_data \
  --gcp-keyfile /ruta/a/tu_service_account.json \
  --max-pages 50
```
El scraper sube los resultados (CSV/JSON) y snapshots de página a `gs://urbania_scrapper/raw_data/` (no escribe localmente).

2) Clean (agente de limpieza con OpenAI) → lee de GCS y sube a GCS:
```bash
# Configura tu API key (usa env.example como plantilla)
cp env.example .env
# edita .env y coloca OPENAI_API_KEY=sk-xxxx

# Variables opcionales en .env (o export):
# GCP_KEYFILE=/ruta/a/tu_service_account.json
# GCS_BUCKET=urbania_scrapper
# GCS_PREFIX=clean_data
# INPUT_GCS_URI=gs://urbania_scrapper/raw_data/urbania_minimal_results_YYYYMMDD_HHMMSS.json
# CLOUD_ONLY=true  # para no escribir localmente

# Ejecuta el agente (por defecto procesa un lote inicial, lee INPUT_GCS_URI si está definido)
python data_cleaning_agent.py
```
Sube `cleaned_urbania_data_YYYYMMDD_HHMMSS.json` a `gs://urbania_scrapper/clean_data/` y progreso a `gs://urbania_scrapper/clean_data/progress/`.

3) Predict (WIP):
- Sección por construir. La idea es entrenar modelos sobre el dataset limpio para estimar precios/atributos.

### Quick Start (Minimal, with Pagination)

```bash
python urbania_minimal_scraper.py --gcs-bucket urbania_scrapper --gcs-prefix raw_data --gcp-keyfile /ruta/a/key.json
```

The minimal scraper waits for Cloudflare, extracts the first page, then clicks "Siguiente" to traverse multiple pages and saves a single CSV/JSON with all properties.

### Configure Pagination

To control how many pages to traverse, edit `urbania_minimal_scraper.py`:

```python
# inside run_minimal_scraper()
max_pages = 50  # number of pages to visit
```

## Configuration

### Scraper Options

- `headless`: Run browser without GUI (default: False)
- `delay_range`: Random delay between requests (default: (2, 5) seconds)
- `scrape_details`: Extract additional data from detail pages (default: False)

### GCS Options (Scraper)

- `--gcs-bucket`: nombre del bucket (por defecto `urbania_scrapper`)
- `--gcs-prefix`: prefijo/carpeta (por defecto `raw_data`)
- `--gcp-keyfile`: ruta a JSON de service account
- `--max-pages`: número de páginas a recorrer (por defecto 50)

### Data Fields Extracted

#### Basic Listing Data
- `title`: Property title/description
- `price`: Rental price (with currency)
- `location`: Full address/location
- `district`: Specific district/neighborhood
- `area`: Property area in m²
- `bedrooms`: Number of bedrooms
- `bathrooms`: Number of bathrooms
- `parking`: Number of parking spaces
- `url`: Link to property detail page
- `scraped_at`: Timestamp of data extraction

#### Detail Page Data (if enabled)
- `full_description`: Complete property description
- `maintenance_fee`: Monthly maintenance costs
- `total_area`: Total property area
- `built_area`: Built/constructed area
- `property_age`: Age of the property
- `amenities`: List of property amenities
- `property_id`: Unique property identifier

## Cloudflare Handling

The scraper is designed to handle Cloudflare protection:

1. **Detection**: Automatically detects Cloudflare challenge pages
2. **Waiting**: Allows time for challenges to complete
3. **Validation**: Confirms successful bypass before proceeding
4. **Fallback**: Provides debug information if bypass fails

### Manual Cloudflare Bypass

If automatic bypass fails:

1. Run `manual_inspect.py`
2. Manually solve any Cloudflare challenges
3. Inspect page structure to identify correct selectors
4. Update selectors in `urbania_scraper.py`

## Selector Discovery

The scraper uses multiple fallback selectors for robustness. To find exact selectors:

### JavaScript Snippets (run in browser console)

```javascript
// Find price elements
document.querySelectorAll('*').forEach(el => {
  if (el.innerText && (el.innerText.includes('S/') || el.innerText.includes('$'))) {
    console.log('Price element:', el.className, el.innerText.substring(0,50));
  }
});

// Find property links
document.querySelectorAll('a[href]').forEach(el => {
  if (el.href.includes('propiedad') || el.href.includes('inmueble')) {
    console.log('Property link:', el.className, el.href);
  }
});

// Find amenity elements
document.querySelectorAll('*').forEach(el => {
  if (el.innerText && (el.innerText.includes('dormitorio') || el.innerText.includes('baño'))) {
    console.log('Amenity element:', el.className, el.innerText.substring(0,50));
  }
});
```

## Output Files

El scraper y los agentes generan varios archivos (en local o en GCS según configuración):

- `urbania_minimal_results_YYYYMMDD_HHMMSS.csv`: Aggregated data (all pages)
- `urbania_minimal_results_YYYYMMDD_HHMMSS.json`: Aggregated data (all pages)
- `urbania_minimal_scraper.log`: Execution log for the minimal scraper
- `minimal_page_source_YYYYMMDD_HHMMSS.html`: Saved page source snapshots
- `debug_cloudflare_attempt_*.html`: Cloudflare debug pages (when applicable)

- `cleaned_urbania_data_YYYYMMDD_HHMMSS.json`: Datos ya limpiados por el agente
- `cleaned_data_progress_YYYYMMDD_HHMMSS_batch_N.json`: Progreso de limpieza por lotes

## Cleaning → Parquet Agent (GCS support)

Convierte JSONs limpios a Parquet, con lectura/escritura desde GCS opcional.

```bash
python cleaning_to_parquet_agent.py \
  --input-gcs-prefix gs://urbania_scrapper/clean_data \
  --output-gcs-prefix gs://urbania_scrapper/processed_data \
  --gcp-keyfile /ruta/a/tu_service_account.json \
  --cloud-only
```

Flags clave:
- `--input-dir` o `--input-gcs-prefix` (prioritario)
- `--output-dir` y/o `--output-gcs-prefix`
- `--gcp-keyfile` para autenticación
- `--cloud-only` para evitar archivos locales

Other scripts in the repo may still generate legacy files like `urbania_rentals_*.csv/json` and `urbania_scraper.log`.

## Error Handling

The scraper includes comprehensive error handling:

- **Individual Listing Failures**: Continues with next listing
- **Network Issues**: Automatic retries with delays
- **Element Not Found**: Multiple fallback selectors
- **Page Load Issues**: Timeout handling and retries

## Best Practices

1. **Respect Rate Limits**: Use appropriate delays between requests
2. **Monitor Logs**: Check logs for errors and warnings
3. **Update Selectors**: Selectors may change as site updates
4. **Legal Compliance**: Ensure compliance with site ToS and robots.txt
5. **Data Validation**: Always validate extracted data

## Troubleshooting

### Common Issues

1. **Cloudflare Blocks**: 
   - Use manual inspection script
   - Increase delays between requests
   - Check if IP is blocked

2. **No Data Extracted**:
   - Run manual inspection to verify selectors
   - Check page source files for actual structure
   - Update CSS selectors in code

3. **Browser Issues**:
   - Update Chrome browser
   - Clear browser cache
   - Check chromedriver compatibility

### Debug Mode

Enable detailed logging by setting log level to DEBUG:

```python
import logging
logging.getLogger().setLevel(logging.DEBUG)
```

## Legal Considerations

- **Robots.txt**: Check site's robots.txt for scraping permissions
- **Terms of Service**: Ensure compliance with site ToS
- **Rate Limiting**: Implement appropriate delays
- **Data Usage**: Use data responsibly and ethically

## Contributing

1. Fork the repository
2. Create feature branch
3. Add tests for new functionality
4. Submit pull request

## License

This project is for educational purposes only. Users are responsible for complying with applicable laws and website terms of service.