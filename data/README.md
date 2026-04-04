# Data Directory

## Dataset Information

This project uses global air quality data from **OpenAQ** (Open Air Quality).

### Sample Data

The sample dataset (`openaq_sample.parquet`) contains:
- **27,759 rows** of air quality measurements
- **10 columns** including timestamps, location, pollutant type, and values
- **Coverage**: Multiple countries worldwide (2014-2024)
- **Pollutants**: PM2.5, NO₂, CO
- **Format**: Parquet (columnar storage)

### Schema

| Column | Type | Description |
|--------|------|-------------|
| Country Code | string | ISO country code |
| City | string | City name |
| Location | string | Monitoring station name |
| Coordinates | string | Latitude, longitude |
| Pollutant | string | Pollutant type (PM2.5, NO2, CO) |
| Source Name | string | Data source agency |
| Unit | string | Measurement unit (µg/m³) |
| Value | float64 | Pollutant concentration |
| Last Updated | string | ISO datetime |
| Country Label | string | Full country name |

## Download Instructions

### Option 1: Use Sample Data

Place `openaq_sample.parquet` in this directory:

```bash
# From project root
cp /path/to/openaq_sample.parquet data/
```

### Option 2: Download from OpenAQ

Visit [OpenAQ](https://openaq.org) to download recent data:

```bash
# Example: Download recent data via OpenAQ API
# (Requires OpenAQ API key)
```

## Data Processing Notes

- **File size**: ~1.5 MB compressed (Parquet)
- **Memory usage**: ~5 MB when loaded
- **Processing time**: <10 seconds for sample dataset
- **Scalability**: Pipeline designed to handle 10TB+ with Dask

## Data Citation

If using OpenAQ data in publications:

```
OpenAQ (2024). Global Air Quality Data. 
Retrieved from https://openaq.org
```
