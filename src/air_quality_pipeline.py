"""
Air Quality Economics Pipeline
===============================

A production-ready pipeline for analyzing global air quality data from OpenAQ.

Author: Pedram Ebadollahyvahed
Date: November 2024
"""

import dask.dataframe as dd
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sklearn.linear_model import LinearRegression
import numpy as np


class AirQualityPipeline:
    """
    Complete pipeline for OpenAQ air quality data analysis.
    
    Features:
    - Efficient Parquet reading with column projection
    - Timezone-aware datetime handling
    - Comprehensive data cleaning
    - Statistical aggregations
    - Interactive visualizations
    """
    
    def __init__(self, file_path='data/openaq_sample.parquet'):
        """
        Initialize pipeline with data source.
        
        Args:
            file_path (str): Path to input Parquet file
        """
        self.file_path = file_path
        self.df_raw = None
        self.df_cleaned = None
        self.stats = {}
        
    def load_data(self, columns=None):
        """
        Load data with column projection for I/O optimization.
        
        Args:
            columns (list): Specific columns to load. If None, loads essential columns.
            
        Returns:
            dd.DataFrame: Loaded data
        """
        if columns is None:
            # Default essential columns for analysis
            columns = [
                'Last Updated',
                'Location',
                'City',
                'Country Label',
                'Pollutant',
                'Value'
            ]
        
        print("Loading data with column projection...")
        df = dd.read_parquet(
            self.file_path,
            columns=columns,
            engine='pyarrow'
        )
        
        # Rename for clarity
        df = df.rename(columns={'Location': 'station_id'})
        
        print(f"Loaded {len(columns)} columns")
        print(f"Columns: {df.columns.tolist()}")
        
        self.df_raw = df
        return df
    
    def clean_data(self, df=None):
        """
        Clean and preprocess air quality data.
        
        Steps:
        1. Parse datetime with timezone handling
        2. Filter for PM2.5 and NO₂ pollutants
        3. Remove negative values (sensor errors)
        4. Pivot to wide format
        5. Count and report missing values
        
        Args:
            df (dd.DataFrame): Input data. Uses self.df_raw if None.
            
        Returns:
            pd.DataFrame: Cleaned data in wide format
        """
        if df is None:
            df = self.df_raw
        
        print("\n" + "="*60)
        print("DATA CLEANING PIPELINE")
        print("="*60)
        
        # Step 1: Parse datetime
        print("\n1. Parsing timestamps...")
        df['Last Updated'] = dd.to_datetime(df['Last Updated'], utc=True, errors='coerce')
        print(" Datetime parsed with UTC timezone")
        
        # Step 2: Filter pollutants
        print("\n2. Filtering for PM2.5 and NO₂...")
        original_count = df.shape[0].compute()
        df = df[df['Pollutant'].isin(['PM2.5', 'NO2'])]
        filtered_count = df.shape[0].compute()
        print(f"Filtered: {original_count:,} → {filtered_count:,} rows")
        
        # Step 3: Remove negative values
        print("\n3. Removing invalid negative values...")
        negative_count = (df['Value'] < 0).sum().compute()
        df = df[df['Value'] >= 0]
        clean_count = df.shape[0].compute()
        print(f"Removed {negative_count} negative values")
        print(f"Clean rows: {clean_count:,}")
        
        # Step 4: Convert to Pandas for pivot
        print("\n4. Converting to Pandas for pivot operation...")
        df_pandas = df.compute()
        print(f"Converted: {len(df_pandas):,} rows")
        
        # Step 5: Pivot to wide format
        print("\n5. Pivoting to wide format...")
        df_wide = df_pandas.pivot_table(
            index=['Last Updated', 'station_id', 'City', 'Country Label'],
            columns='Pollutant',
            values='Value',
            aggfunc='mean'
        ).reset_index()
        
        df_wide.columns.name = None
        df_wide['Last Updated'] = pd.to_datetime(df_wide['Last Updated'], utc=True)
        
        print(f"Pivoted: {df_wide.shape[0]:,} rows × {df_wide.shape[1]} columns")
        
        # Step 6: Count missing values
        print("\n6. Analyzing data quality...")
        missing_pm25 = df_wide['PM2.5'].isna().sum()
        missing_no2 = df_wide['NO2'].isna().sum()
        print(f"Missing PM2.5: {missing_pm25:,}")
        print(f"Missing NO₂: {missing_no2:,}")
        
        # Summary
        print("\n" + "="*60)
        print("CLEANING SUMMARY")
        print("="*60)
        print(f"Original rows:        {original_count:,}")
        print(f"After filtering:      {filtered_count:,}")
        print(f"Negative removed:     {negative_count}")
        print(f"Final clean rows:     {df_wide.shape[0]:,}")
        print(f"Missing PM2.5:        {missing_pm25:,}")
        print(f"Missing NO₂:          {missing_no2:,}")
        print("="*60 + "\n")
        
        # Store stats
        self.stats['cleaning'] = {
            'original_rows': original_count,
            'filtered_rows': filtered_count,
            'negative_removed': negative_count,
            'final_rows': df_wide.shape[0],
            'missing_pm25': missing_pm25,
            'missing_no2': missing_no2
        }
        
        self.df_cleaned = df_wide
        return df_wide
    
    def compute_statistics(self, df=None):
        """
        Compute descriptive statistics.
        
        Returns:
            dict: Statistics including city averages, temporal trends, missing data
        """
        if df is None:
            df = self.df_cleaned
        
        print("\n" + "="*60)
        print("STATISTICAL ANALYSIS")
        print("="*60)
        
        # 1. Average PM2.5 by City
        print("\n1. Computing PM2.5 by city...")
        avg_pm25_by_city = df.groupby('City')['PM2.5'].mean().sort_values(ascending=False).dropna()
        print(f"Analyzed {len(avg_pm25_by_city)} cities")
        
        # 2. Monthly NO₂ trends
        print("\n2. Computing monthly NO₂ trends...")
        df['Year-Month'] = df['Last Updated'].dt.tz_localize(None).dt.to_period('M')
        avg_no2_by_month = df.groupby('Year-Month')['NO2'].mean().dropna()
        avg_no2_by_month.index = avg_no2_by_month.index.to_timestamp()
        print(f"Analyzed {len(avg_no2_by_month)} months with data")
        
        # 3. Missing PM2.5 by country
        print("\n3. Computing data quality by country...")
        country_stats = df.groupby('Country Label').agg({
            'PM2.5': [lambda x: len(x), lambda x: x.isna().sum()]
        })
        country_stats.columns = ['total_records', 'missing_pm25']
        country_stats['percent_missing'] = (
            country_stats['missing_pm25'] / country_stats['total_records']
        ) * 100
        missing_pm25_per_country = country_stats['percent_missing'].sort_values(ascending=False)
        print(f"Analyzed {len(missing_pm25_per_country)} countries")
        
        # Summary
        print("\n" + "="*60)
        print("STATISTICS SUMMARY")
        print("="*60)
        print(f"Cities analyzed:       {len(avg_pm25_by_city)}")
        print(f"Months with NO₂:       {len(avg_no2_by_month)}")
        print(f"Countries analyzed:    {len(missing_pm25_per_country)}")
        print(f"Top polluted city:     {avg_pm25_by_city.index[0]} ({avg_pm25_by_city.iloc[0]:.2f} µg/m³)")
        print(f"Overall avg NO₂:       {avg_no2_by_month.mean():.2f} µg/m³")
        print("="*60 + "\n")
        
        # Store results
        self.stats['analysis'] = {
            'avg_pm25_by_city': avg_pm25_by_city,
            'avg_no2_by_month': avg_no2_by_month,
            'missing_pm25_per_country': missing_pm25_per_country
        }
        
        return self.stats['analysis']
    
    def create_visualizations(self, output_dir='results'):
        """
        Create interactive Plotly visualizations.
        
        Args:
            output_dir (str): Directory to save HTML charts
            
        Returns:
            dict: Figure objects
        """
        stats = self.stats['analysis']
        df = self.df_cleaned
        
        print("\n" + "="*60)
        print("VISUALIZATION GENERATION")
        print("="*60)
        
        # Chart 1: Top 10 Cities Bar Chart
        print("\n1. Creating bar chart: Top 10 PM2.5 cities...")
        top10_cities = stats['avg_pm25_by_city'].head(10)
        
        fig1 = px.bar(
            x=top10_cities.index,
            y=top10_cities.values,
            labels={'x': 'City', 'y': 'Average PM2.5 (µg/m³)'},
            title='Top 10 Most Polluted Cities by PM2.5 Concentration',
            color=top10_cities.values,
            color_continuous_scale='Reds'
        )
        fig1.update_layout(
            xaxis_tickangle=-45,
            showlegend=False,
            height=500,
            template='plotly_white'
        )
        fig1.show()
        fig1.write_html(f'{output_dir}/chart1_pm25_cities.html')
        print(f"Saved: {output_dir}/chart1_pm25_cities.html")
        
        # Chart 2: Monthly NO₂ Line Chart
        print("\n2. Creating line chart: Monthly NO₂ trends...")
        avg_no2_by_month = stats['avg_no2_by_month']
        
        fig2 = px.line(
            x=avg_no2_by_month.index,
            y=avg_no2_by_month.values,
            labels={'x': 'Month', 'y': 'Average NO₂ (µg/m³)'},
            title='Monthly Average NO₂ Trend (2016-2024)',
            markers=True
        )
        fig2.update_traces(line_color='#1f77b4', marker=dict(size=4))
        fig2.update_layout(
            height=500,
            template='plotly_white',
            hovermode='x unified'
        )
        fig2.show()
        fig2.write_html(f'{output_dir}/chart2_no2_trends.html')
        print(f"Saved: {output_dir}/chart2_no2_trends.html")
        
        # Chart 3: PM2.5 vs NO₂ Scatter with Regression
        print("\n3. Creating scatter plot: PM2.5 vs NO₂ correlation...")
        
        # Find city with best correlation
        city_correlations = []
        for city in df['City'].unique():
            city_df = df[df['City'] == city][['PM2.5', 'NO2']].dropna()
            if len(city_df) >= 20:
                corr = city_df['PM2.5'].corr(city_df['NO2'])
                city_correlations.append({
                    'city': city,
                    'correlation': corr,
                    'abs_correlation': abs(corr),
                    'n_points': len(city_df)
                })
        
        corr_df = pd.DataFrame(city_correlations).sort_values('abs_correlation', ascending=False)
        selected_city = corr_df.iloc[0]['city']
        selected_corr = corr_df.iloc[0]['correlation']
        
        print(f"Selected: {selected_city} (r={selected_corr:.3f})")
        
        city_df = df[df['City'] == selected_city][['PM2.5', 'NO2']].dropna()
        
        # Regression
        X = city_df['PM2.5'].values.reshape(-1, 1)
        y = city_df['NO2'].values
        model = LinearRegression()
        model.fit(X, y)
        r2_score = model.score(X, y)
        
        x_range = np.linspace(city_df['PM2.5'].min(), city_df['PM2.5'].max(), 100)
        y_pred = model.predict(x_range.reshape(-1, 1))
        
        fig3 = go.Figure()
        fig3.add_trace(go.Scatter(
            x=city_df['PM2.5'], y=city_df['NO2'],
            mode='markers', name='Observations',
            marker=dict(size=8, color='steelblue', opacity=0.6)
        ))
        fig3.add_trace(go.Scatter(
            x=x_range, y=y_pred,
            mode='lines', name=f'Linear Fit (R²={r2_score:.3f})',
            line=dict(color='red', width=2)
        ))
        fig3.update_layout(
            title=f'PM2.5 vs NO₂ Correlation: {selected_city}<br><sub>R²={r2_score:.3f}, r={selected_corr:.3f}, n={len(city_df)}</sub>',
            xaxis_title='PM2.5 (µg/m³)',
            yaxis_title='NO₂ (µg/m³)',
            height=500,
            template='plotly_white'
        )
        fig3.show()
        fig3.write_html(f'{output_dir}/chart3_correlation.html')
        print(f" Saved: {output_dir}/chart3_correlation.html")
        print(f"R² score: {r2_score:.3f}")
        
        print("\n" + "="*60)
        print("✓ All visualizations generated successfully!")
        print("="*60 + "\n")
        
        return {'fig1': fig1, 'fig2': fig2, 'fig3': fig3}
    
    def save_cleaned_data(self, output_path='results/cleaned_openaq.parquet'):
        """Save cleaned data to Parquet for reuse."""
        self.df_cleaned.to_parquet(output_path, engine='pyarrow', index=False)
        print(f"Cleaned data saved: {output_path}")
    
    def run_analysis(self):
        """
        Run complete pipeline: load → clean → analyze → visualize.
        
        Returns:
            dict: All results and statistics
        """
        print("\n" + "="*60)
        print("AIR QUALITY ECONOMICS PIPELINE")
        print("="*60)
        
        # Load
        self.load_data()
        
        # Clean
        df_cleaned = self.clean_data()
        
        # Analyze
        stats = self.compute_statistics()
        
        # Visualize
        figs = self.create_visualizations()
        
        # Save
        self.save_cleaned_data()
        
        print("\n" + "="*60)
        print("PIPELINE COMPLETED SUCCESSFULLY")
        print("="*60)
        
        return {
            'cleaned_data': df_cleaned,
            'stats': stats,
            'figures': figs
        }


# Example usage
if __name__ == "__main__":
    # Initialize and run pipeline
    pipeline = AirQualityPipeline('data/openaq_sample.parquet')
    results = pipeline.run_analysis()
    
    # Access results
    df_cleaned = results['cleaned_data']
    avg_pm25_by_city = results['stats']['avg_pm25_by_city']
    avg_no2_by_month = results['stats']['avg_no2_by_month']
    missing_pm25_per_country = results['stats']['missing_pm25_per_country']
