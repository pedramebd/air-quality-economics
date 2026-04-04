# Setup and Deployment Guide

Complete guide for setting up and deploying the Air Quality Economics Pipeline.

## Table of Contents

1. [Local Setup](#local-setup)
2. [Running the Pipeline](#running-the-pipeline)
3. [Jupyter Notebook Demo](#jupyter-notebook-demo)
4. [GitHub Deployment](#github-deployment)
5. [Troubleshooting](#troubleshooting)

---

## Local Setup

### Prerequisites

- Python 3.8 or higher
- pip or conda package manager
- 2GB free disk space
- Internet connection (for downloading dependencies)

### Installation Steps

```bash
# 1. Clone repository (or download ZIP)
git clone https://github.com/yourusername/air-quality-economics.git
cd air-quality-economics

# 2. Create virtual environment (recommended)
python -m venv venv

# Activate virtual environment
# On Mac/Linux:
source venv/bin/activate
# On Windows:
venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Verify installation
python -c "import dask, pandas, plotly; print('✓ All dependencies installed')"
```

### Data Setup

Place `openaq_sample.parquet` in the `data/` directory:

```bash
cp /path/to/openaq_sample.parquet data/
```

---

## Running the Pipeline

### Method 1: Python Script

```bash
# From project root
python src/air_quality_pipeline.py
```

This will:
- Load data from `data/openaq_sample.parquet`
- Clean and process data
- Generate statistics
- Create interactive visualizations
- Save outputs to `results/`

### Method 2: Interactive Python

```python
from src.air_quality_pipeline import AirQualityPipeline

# Initialize
pipeline = AirQualityPipeline('data/openaq_sample.parquet')

# Run analysis
results = pipeline.run_analysis()

# Access outputs
df_cleaned = results['cleaned_data']
stats = results['stats']
```

### Method 3: Step-by-Step

```python
pipeline = AirQualityPipeline('data/openaq_sample.parquet')

# Step 1: Load
pipeline.load_data()

# Step 2: Clean
df_cleaned = pipeline.clean_data()

# Step 3: Analyze
stats = pipeline.compute_statistics()

# Step 4: Visualize
figs = pipeline.create_visualizations()

# Step 5: Save
pipeline.save_cleaned_data()
```

---

## Jupyter Notebook Demo

### Launch Notebook

```bash
# Start Jupyter
jupyter notebook

# Navigate to notebooks/demo_analysis.ipynb
# Run all cells: Cell > Run All
```

### Expected Output

The notebook will display:
- Pipeline execution logs
- Statistical summaries
- Interactive charts (inline)
- Data quality metrics

---

## GitHub Deployment

### First-Time Setup

```bash
# 1. Create repository on GitHub
# (via web interface)

# 2. Initialize git (if not cloned)
git init
git add .
git commit -m "Initial commit: Air Quality Economics Pipeline"

# 3. Link to remote
git remote add origin https://github.com/yourusername/air-quality-economics.git

# 4. Push
git branch -M main
git push -u origin main
```

### Updating Repository

```bash
# After making changes
git add .
git commit -m "Update: description of changes"
git push
```

### Before Pushing

**Check .gitignore** to ensure large files aren't committed:

```bash
# These should NOT be in Git:
data/*.parquet      # Data files (too large)
results/*.html      # Generated files (regenerate locally)
results/*.parquet   # Cleaned data (regenerate locally)

# These SHOULD be in Git:
src/                # Source code
notebooks/          # Notebooks (without outputs)
requirements.txt    # Dependencies
README.md          # Documentation
```

### Clean Notebook Outputs (Before Pushing)

```bash
# Clear outputs from notebook
jupyter nbconvert --clear-output --inplace notebooks/demo_analysis.ipynb
```

---

## Troubleshooting

### Common Issues

#### 1. Import Error: No module named 'dask'

**Solution**:
```bash
pip install -r requirements.txt
```

#### 2. FileNotFoundError: data/openaq_sample.parquet

**Solution**: Ensure data file is in correct location:
```bash
ls data/openaq_sample.parquet
# Should show the file
```

#### 3. Charts Not Displaying in Notebook

**Solution**: Ensure Plotly is installed and restart kernel:
```bash
pip install plotly
# Then: Kernel > Restart in Jupyter
```

#### 4. Memory Error with Dask

**Solution**: Reduce dataset size or increase swap:
```python
# Process in chunks
df = dd.read_parquet('data/openaq_sample.parquet', blocksize='10MB')
```

#### 5. Timezone Warnings

**Solution**: Already handled in code with `utc=True`, but if you see warnings:
```python
# Explicitly set timezone
df['Last Updated'] = dd.to_datetime(df['Last Updated'], utc=True)
```

---

## Performance Notes

### Expected Runtime

- **Sample dataset (27K rows)**: ~10 seconds
- **Medium dataset (500K rows)**: ~2 minutes
- **Large dataset (10M+ rows)**: Requires Dask distributed cluster

### Memory Requirements

- **Sample**: ~50 MB RAM
- **Medium**: ~500 MB RAM
- **Large**: Requires distributed processing

---

## Advanced Usage

### Custom Data Source

```python
pipeline = AirQualityPipeline('path/to/your/data.parquet')
results = pipeline.run_analysis()
```

### Selective Analysis

```python
# Only load specific columns
pipeline.load_data(columns=['Last Updated', 'City', 'Pollutant', 'Value'])

# Only create specific charts
pipeline.create_visualizations(output_dir='custom_results')
```

### Export for External Use

```python
# Export to CSV
df_cleaned.to_csv('results/cleaned_data.csv', index=False)

# Export statistics to JSON
import json
stats_dict = {
    'top_cities': stats['avg_pm25_by_city'].head(10).to_dict()
}
with open('results/stats.json', 'w') as f:
    json.dump(stats_dict, f, indent=2)
```

---

## Contact & Support

For issues or questions:
- **GitHub Issues**: https://github.com/yourusername/air-quality-economics/issues
- **Email**: your.email@example.com

---

**Last Updated**: November 2024
