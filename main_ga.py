import streamlit as st
import autogen
import json
import re
from typing import Dict, List, Optional, Tuple
import logging
import time
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import os
from dotenv import load_dotenv
import numpy as np
from io import BytesIO
import base64

# GA4 API imports
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest, FilterExpression, Filter
from google.oauth2 import service_account
import tempfile

# Load environment variables
load_dotenv()

# Enhanced logging configuration
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('ga4_app.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Enhanced configuration with retry logic
llm_config = {
    "config_list": [{
        "model": "gpt-4o-mini",
        "api_key": os.getenv("OPENAI_API_KEY"),
        "base_url": "https://api.openai.com/v1",
        "api_type": "openai"
    }],
    "temperature": 0.1,  # Lower temperature for more consistent results
    "timeout": 120,  # Increased timeout
    "seed": 42,
    "cache_seed": 42,
    "retry_wait_time": 1,
    "max_retry_period": 30
}

# Professional color scheme
BRAND_COLORS = {
    'primary': '#1f77b4',
    'secondary': '#ff7f0e', 
    'success': '#2ca02c',
    'warning': '#ff9500',
    'danger': '#d62728',
    'info': '#17a2b8',
    'dark': '#343a40',
    'light': '#f8f9fa'
}

class GA4DataManager:
    """Enhanced GA4 data management with better error handling and validation"""
    
    def __init__(self, property_id: str, service_account_file: str = "service-account.json"):
        self.property_id = property_id
        self.service_account_file = service_account_file
        self.client = None
        self._rate_limit_delay = 1  # Start with 1 second delay
        
    def authenticate(self) -> Dict:
        """Enhanced authentication with better error handling"""
        try:
            if not os.path.exists(self.service_account_file):
                return {
                    "success": False,
                    "error": f"Service account file not found: {self.service_account_file}",
                    "suggestion": "Please ensure the service account JSON file exists in the project directory"
                }
            
            # Validate JSON structure
            with open(self.service_account_file, 'r') as f:
                service_account_info = json.load(f)
            
            # Validate required fields
            required_fields = ["type", "project_id", "private_key", "client_email"]
            missing_fields = [field for field in required_fields if field not in service_account_info]
            
            if missing_fields:
                return {
                    "success": False,
                    "error": f"Missing required fields in service account: {missing_fields}",
                    "suggestion": "Please check your service account JSON file"
                }
            
            # Create credentials
            credentials = service_account.Credentials.from_service_account_info(
                service_account_info,
                scopes=['https://www.googleapis.com/auth/analytics.readonly']
            )
            
            self.client = BetaAnalyticsDataClient(credentials=credentials)
            
            # Test connection
            test_request = RunReportRequest(
                property=f"properties/{self.property_id}",
                date_ranges=[DateRange(start_date="yesterday", end_date="today")],
                metrics=[Metric(name="sessions")],
                limit=1
            )
            
            response = self.client.run_report(test_request)
            
            return {
                "success": True,
                "message": f"Successfully authenticated with GA4 property {self.property_id}",
                "test_rows": len(response.rows)
            }
            
        except Exception as e:
            logger.error(f"Authentication failed: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "suggestion": "Check your service account permissions and property ID"
            }
    
    def get_data_with_retry(self, dimensions: List[str], metrics: List[str], 
                           start_date: str, end_date: str, limit: int = 10000) -> Dict:
        """Enhanced data retrieval with retry logic and rate limiting"""
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                # Rate limiting
                if attempt > 0:
                    time.sleep(self._rate_limit_delay * (2 ** attempt))  # Exponential backoff
                
                if not self.client:
                    auth_result = self.authenticate()
                    if not auth_result["success"]:
                        return auth_result
                
                # Validate inputs
                validation_result = self._validate_query_inputs(dimensions, metrics, start_date, end_date)
                if not validation_result["valid"]:
                    return {
                        "success": False,
                        "error": validation_result["error"],
                        "suggestion": validation_result["suggestion"]
                    }
                
                # Build and execute request
                request = RunReportRequest(
                    property=f"properties/{self.property_id}",
                    date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
                    dimensions=[Dimension(name=dim) for dim in dimensions],
                    metrics=[Metric(name=metric) for metric in metrics],
                    limit=min(limit, 100000)  # GA4 API limit
                )
                
                response = self.client.run_report(request)
                
                # Process response
                data = self._process_response(response, dimensions, metrics)
                
                return {
                    "success": True,
                    "data": data,
                    "row_count": len(data),
                    "metadata": {
                        "dimensions": dimensions,
                        "metrics": metrics,
                        "date_range": f"{start_date} to {end_date}",
                        "property_id": self.property_id
                    }
                }
                
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1} failed: {str(e)}")
                if attempt == max_retries - 1:
                    return {
                        "success": False,
                        "error": str(e),
                        "suggestion": "Try reducing the date range or number of dimensions/metrics"
                    }
                
        return {"success": False, "error": "Max retries exceeded"}
    
    def _validate_query_inputs(self, dimensions: List[str], metrics: List[str], 
                              start_date: str, end_date: str) -> Dict:
        """Validate query inputs against GA4 limits and compatibility"""
        
        # Check limits
        if len(dimensions) > 9:
            return {
                "valid": False,
                "error": f"Too many dimensions ({len(dimensions)}). Maximum is 9.",
                "suggestion": "Reduce the number of dimensions in your query"
            }
        
        if len(metrics) > 10:
            return {
                "valid": False,
                "error": f"Too many metrics ({len(metrics)}). Maximum is 10.",
                "suggestion": "Reduce the number of metrics in your query"
            }
        
        # Validate metric names
        valid_metrics = {
            "sessions", "totalUsers", "newUsers", "activeUsers",
            "screenPageViews", "eventCount", "bounceRate", "engagementRate",
            "averageSessionDuration", "sessionsPerUser", "conversions", 
            "totalRevenue", "transactions", "purchaseRevenue"
        }
        
        invalid_metrics = [m for m in metrics if m not in valid_metrics]
        if invalid_metrics:
            return {
                "valid": False,
                "error": f"Invalid metrics: {invalid_metrics}",
                "suggestion": f"Use valid metrics: {', '.join(sorted(valid_metrics))}"
            }
        
        # Check date format
        date_patterns = [
            r'^\d{4}-\d{2}-\d{2}$',  # YYYY-MM-DD
            r'^\d+daysAgo$',         # NdaysAgo
            r'^(today|yesterday)$'   # today, yesterday
        ]
        
        for date_val in [start_date, end_date]:
            if not any(re.match(pattern, date_val) for pattern in date_patterns):
                return {
                    "valid": False,
                    "error": f"Invalid date format: {date_val}",
                    "suggestion": "Use formats like '2024-01-01', '30daysAgo', 'today', or 'yesterday'"
                }
        
        return {"valid": True}
    
    def _process_response(self, response, dimensions: List[str], metrics: List[str]) -> List[Dict]:
        """Process GA4 API response into structured data"""
        data = []
        
        for row in response.rows:
            row_data = {}
            
            # Process dimensions
            for i, dim in enumerate(dimensions):
                value = row.dimension_values[i].value
                if dim == "date" and len(value) == 8:  # YYYYMMDD format
                    try:
                        row_data[dim] = datetime.strptime(value, '%Y%m%d').strftime('%Y-%m-%d')
                    except:
                        row_data[dim] = value
                elif dim == "pagePath":
                    # Clean up page path for better readability
                    row_data[dim] = self._clean_page_path(value)
                    row_data[dim + "_raw"] = value  # Keep original for reference
                elif dim == "pageTitle":
                    # Clean up page title
                    row_data[dim] = self._clean_page_title(value)
                else:
                    row_data[dim] = value
            
            # Process metrics
            for i, metric in enumerate(metrics):
                value = row.metric_values[i].value
                try:
                    # Convert to appropriate numeric type
                    if '.' in value or 'e' in value.lower():
                        row_data[metric] = float(value)
                    else:
                        row_data[metric] = int(value)
                except (ValueError, TypeError):
                    row_data[metric] = value
            
            data.append(row_data)
        
        return data
    
    def _clean_page_path(self, path: str) -> str:
        """Clean and make page paths more readable"""
        if not path or path.strip() == "":
            return "Unknown Page"
        
        # Handle homepage
        if path == "/" or path == "":
            return "Homepage"
        
        # Remove query parameters for cleaner display
        if "?" in path:
            path = path.split("?")[0]
        
        # Remove trailing slashes
        path = path.rstrip("/")
        
        # If still empty after cleaning, it's homepage
        if not path:
            return "Homepage"
        
        # Limit length for display
        if len(path) > 50:
            return path[:47] + "..."
        
        return path
    
    def _clean_page_title(self, title: str) -> str:
        """Clean and make page titles more readable"""
        if not title or title.strip() == "" or title == "(not set)":
            return "Untitled Page"
        
        # Limit length for display
        if len(title) > 60:
            return title[:57] + "..."
        
        return title

class QueryIntelligence:
    """Enhanced query parsing and intelligence"""
    
    @staticmethod
    def parse_user_request(user_request: str) -> Dict:
        """Enhanced natural language query parsing"""
        request_lower = user_request.lower()
        
        # Advanced query patterns with confidence scoring
        query_patterns = [
            # Traffic analysis
            {
                "patterns": ["traffic", "visitors", "sessions", "users", "visits"],
                "dimensions": ["date"],
                "metrics": ["sessions", "totalUsers", "screenPageViews"],
                "type": "traffic",
                "confidence": 0.9
            },
            # Page performance
            {
                "patterns": ["pages", "page views", "content", "top pages", "popular pages"],
                "dimensions": ["pagePath", "pageTitle"],
                "metrics": ["screenPageViews", "sessions"],
                "type": "pages",
                "confidence": 0.95
            },
            # Traffic sources
            {
                "patterns": ["source", "referral", "campaign", "channel", "where", "how users found"],
                "dimensions": ["source", "medium", "sessionDefaultChannelGroup"],
                "metrics": ["sessions", "totalUsers"],
                "type": "acquisition",
                "confidence": 0.9
            },
            # Device analysis
            {
                "patterns": ["device", "mobile", "desktop", "tablet", "platform"],
                "dimensions": ["deviceCategory", "operatingSystem"],
                "metrics": ["sessions", "totalUsers"],
                "type": "technology",
                "confidence": 0.85
            },
            # Geographic analysis
            {
                "patterns": ["country", "location", "geographic", "region", "city"],
                "dimensions": ["country", "city"],
                "metrics": ["sessions", "totalUsers"],
                "type": "geography",
                "confidence": 0.9
            },
            # Conversion analysis
            {
                "patterns": ["conversion", "goal", "revenue", "purchase", "transaction"],
                "dimensions": ["date"],
                "metrics": ["conversions", "totalRevenue", "transactions"],
                "type": "ecommerce",
                "confidence": 0.95
            },
            # Engagement analysis
            {
                "patterns": ["engagement", "bounce", "duration", "time", "interaction"],
                "dimensions": ["date"],
                "metrics": ["engagementRate", "bounceRate", "averageSessionDuration"],
                "type": "engagement",
                "confidence": 0.9
            }
        ]
        
        # Find best matching pattern
        best_match = None
        highest_score = 0
        
        for pattern in query_patterns:
            score = 0
            for keyword in pattern["patterns"]:
                if keyword in request_lower:
                    score += pattern["confidence"]
            
            if score > highest_score:
                highest_score = score
                best_match = pattern
        
        if best_match:
            return {
                "dimensions": best_match["dimensions"],
                "metrics": best_match["metrics"],
                "type": best_match["type"],
                "confidence": highest_score
            }
        
        # Default fallback
        return {
            "dimensions": ["date"],
            "metrics": ["sessions", "totalUsers"],
            "type": "traffic",
            "confidence": 0.5
        }
    
    @staticmethod
    def parse_date_range(user_request: str) -> Tuple[str, str]:
        """Enhanced date range parsing"""
        request_lower = user_request.lower()
        
        date_patterns = [
            (["last 90 days", "past 3 months", "quarterly"], ("90daysAgo", "today")),
            (["last 60 days", "past 2 months"], ("60daysAgo", "today")),
            (["last 30 days", "past month", "monthly"], ("30daysAgo", "today")),
            (["last 14 days", "past 2 weeks"], ("14daysAgo", "today")),
            (["last 7 days", "past week", "weekly"], ("7daysAgo", "today")),
            (["yesterday"], ("yesterday", "yesterday")),
            (["today"], ("today", "today")),
            (["this week"], ("7daysAgo", "today")),
            (["this month"], ("30daysAgo", "today")),
        ]
        
        for patterns, date_range in date_patterns:
            if any(pattern in request_lower for pattern in patterns):
                return date_range
        
        return "30daysAgo", "today"  # Default to last 30 days

class ProfessionalVisualizer:
    """Enhanced visualization with professional styling"""
    
    def __init__(self):
        self.brand_colors = BRAND_COLORS
        self.color_sequence = [
            self.brand_colors['primary'],
            self.brand_colors['secondary'],
            self.brand_colors['success'],
            self.brand_colors['warning'],
            self.brand_colors['info']
        ]
    
    def create_executive_summary(self, data: List[Dict], query_type: str) -> str:
        """Create executive summary with key insights"""
        if not data:
            return "**Executive Summary**: No data available for analysis."
        
        df = pd.DataFrame(data)
        summary_parts = []
        
        # Time period analysis
        if 'date' in df.columns:
            date_range = f"{df['date'].min()} to {df['date'].max()}"
            summary_parts.append(f"**Analysis Period**: {date_range}")
        
        # Key metrics overview
        if 'totalUsers' in df.columns:
            total_users = df['totalUsers'].sum()
            summary_parts.append(f"**Total Users**: {total_users:,}")
        
        if 'sessions' in df.columns:
            total_sessions = df['sessions'].sum()
            summary_parts.append(f"**Total Sessions**: {total_sessions:,}")
            
            if 'totalUsers' in df.columns and total_users > 0:
                sessions_per_user = total_sessions / total_users
                summary_parts.append(f"**Sessions per User**: {sessions_per_user:.2f}")
        
        if 'screenPageViews' in df.columns:
            total_pageviews = df['screenPageViews'].sum()
            summary_parts.append(f"**Total Page Views**: {total_pageviews:,}")
        
        if 'conversions' in df.columns:
            total_conversions = df['conversions'].sum()
            summary_parts.append(f"**Total Conversions**: {total_conversions:,}")
            
            if 'totalUsers' in df.columns and total_users > 0:
                conversion_rate = (total_conversions / total_users) * 100
                summary_parts.append(f"**Conversion Rate**: {conversion_rate:.2f}%")
        
        return " | ".join(summary_parts)
    
    def create_traffic_chart(self, df: pd.DataFrame) -> go.Figure:
        """Create professional traffic trend chart"""
        fig = go.Figure()
        
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values('date')
            
            # Add multiple metrics if available
            metrics = [
                ('sessions', 'Sessions', self.brand_colors['primary']),
                ('totalUsers', 'Users', self.brand_colors['secondary']),
                ('screenPageViews', 'Page Views', self.brand_colors['success'])
            ]
            
            for metric, label, color in metrics:
                if metric in df.columns:
                    fig.add_trace(go.Scatter(
                        x=df['date'],
                        y=df[metric],
                        mode='lines+markers',
                        name=label,
                        line=dict(color=color, width=3),
                        marker=dict(size=6, color=color),
                        hovertemplate=f'<b>{label}</b><br>Date: %{{x}}<br>Count: %{{y:,}}<extra></extra>'
                    ))
            
            fig.update_layout(
                title=dict(
                    text="Website Traffic Trends",
                    font=dict(size=16, color='#1e293b')
                ),
                xaxis_title="Date",
                yaxis_title="Count",
                hovermode='x unified',
                template='simple_white',
                height=500,
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                font=dict(size=12, color='#374151'),
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=1.02,
                    xanchor="right",
                    x=1,
                    font=dict(size=11)
                ),
                margin=dict(t=60, l=60, r=40, b=60)
            )
            
            # Update axes styling
            fig.update_xaxes(
                gridcolor='#f1f5f9',
                linecolor='#e2e8f0',
                tickfont=dict(size=11, color='#6b7280')
            )
            fig.update_yaxes(
                gridcolor='#f1f5f9',
                linecolor='#e2e8f0',
                tickfont=dict(size=11, color='#6b7280')
            )
        
        return fig
    
    def create_comparison_chart(self, df: pd.DataFrame, dimension: str, metric: str) -> go.Figure:
        """Create professional comparison charts"""
        # Take top 10 for readability
        df_sorted = df.nlargest(10, metric)
        
        # Create hover template with raw data if available
        if dimension == "pagePath" and f"{dimension}_raw" in df_sorted.columns:
            hover_template = '<b>%{y}</b><br>' + f'{metric.title()}: %{{x:,}}<br>' + 'Raw Path: %{customdata}<extra></extra>'
            customdata = df_sorted[f"{dimension}_raw"]
        else:
            hover_template = f'<b>%{{y}}</b><br>{metric.title()}: %{{x:,}}<extra></extra>'
            customdata = None
        
        fig = go.Figure(data=[
            go.Bar(
                x=df_sorted[metric],
                y=df_sorted[dimension],
                orientation='h',
                marker=dict(
                    color=df_sorted[metric],
                    colorscale='viridis',
                    colorbar=dict(title=metric.title())
                ),
                text=df_sorted[metric],
                texttemplate='%{text:,}',
                textposition='auto',
                hovertemplate=hover_template,
                customdata=customdata
            )
        ])
        
        fig.update_layout(
            title=dict(
                text=f"Top 10 {dimension.title()} by {metric.title()}",
                font=dict(size=16, color='#1e293b')
            ),
            xaxis_title=metric.title(),
            yaxis_title=dimension.title(),
            template='simple_white',
            height=500,
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            font=dict(size=12, color='#374151'),
            margin=dict(l=150, t=60, r=40, b=60)
        )
        
        # Update axes styling
        fig.update_xaxes(
            gridcolor='#f1f5f9',
            linecolor='#e2e8f0',
            tickfont=dict(size=11, color='#6b7280')
        )
        fig.update_yaxes(
            gridcolor='#f1f5f9',
            linecolor='#e2e8f0',
            tickfont=dict(size=11, color='#6b7280')
        )
        
        return fig
    
    def create_pie_chart(self, df: pd.DataFrame, dimension: str, metric: str) -> go.Figure:
        """Create professional pie chart"""
        # Group small segments into "Others"
        df_sorted = df.sort_values(metric, ascending=False)
        
        if len(df_sorted) > 8:
            top_segments = df_sorted.head(7)
            others_value = df_sorted.tail(len(df_sorted) - 7)[metric].sum()
            
            # Add "Others" segment
            others_row = {dimension: 'Others', metric: others_value}
            if f"{dimension}_raw" in df_sorted.columns:
                others_row[f"{dimension}_raw"] = 'Others'
            top_segments = pd.concat([top_segments, pd.DataFrame([others_row])], ignore_index=True)
        else:
            top_segments = df_sorted
        
        # Create hover template with raw data if available
        if dimension == "pagePath" and f"{dimension}_raw" in top_segments.columns:
            hover_template = '<b>%{label}</b><br>Count: %{value:,}<br>Percentage: %{percent}<br>Raw Path: %{customdata}<extra></extra>'
            customdata = top_segments[f"{dimension}_raw"]
        else:
            hover_template = '<b>%{label}</b><br>Count: %{value:,}<br>Percentage: %{percent}<extra></extra>'
            customdata = None
        
        fig = go.Figure(data=[
            go.Pie(
                labels=top_segments[dimension],
                values=top_segments[metric],
                hole=0.4,  # Donut chart
                textinfo='label+percent',
                textposition='auto',
                marker=dict(colors=self.color_sequence * 3),  # Repeat colors if needed
                hovertemplate=hover_template,
                customdata=customdata
            )
        ])
        
        fig.update_layout(
            title=dict(
                text=f"{dimension.title()} Distribution by {metric.title()}",
                font=dict(size=16, color='#1e293b')
            ),
            template='simple_white',
            height=500,
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            font=dict(size=12, color='#374151'),
            showlegend=True,
            legend=dict(
                orientation="v",
                yanchor="middle",
                y=0.5,
                xanchor="left",
                x=1.05,
                font=dict(size=11)
            ),
            margin=dict(t=60, l=40, r=150, b=60)
        )
        
        return fig

class ReportGenerator:
    """Professional report generation"""
    
    def __init__(self, ga_manager: GA4DataManager):
        self.ga_manager = ga_manager
        self.visualizer = ProfessionalVisualizer()
    
    def generate_comprehensive_report(self, user_request: str) -> Dict:
        """Generate a comprehensive professional report"""
        try:
            # Parse the request
            query_config = QueryIntelligence.parse_user_request(user_request)
            date_range = QueryIntelligence.parse_date_range(user_request)
            
            # Get data
            data_result = self.ga_manager.get_data_with_retry(
                dimensions=query_config['dimensions'],
                metrics=query_config['metrics'],
                start_date=date_range[0],
                end_date=date_range[1],
                limit=10000
            )
            
            if not data_result['success']:
                return {
                    "success": False,
                    "error": data_result['error'],
                    "suggestion": data_result.get('suggestion', 'Please try a different query')
                }
            
            df = pd.DataFrame(data_result['data'])
            
            # Generate insights
            insights = self._generate_insights(df, query_config['type'])
            
            # Create visualizations
            visualizations = self._create_visualizations(df, query_config)
            
            return {
                "success": True,
                "executive_summary": self.visualizer.create_executive_summary(data_result['data'], query_config['type']),
                "insights": insights,
                "visualizations": visualizations,
                "data": data_result['data'],
                "metadata": data_result['metadata']
            }
            
        except Exception as e:
            logger.error(f"Report generation failed: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "suggestion": "Please try simplifying your request or check your data access"
            }
    
    def _generate_insights(self, df: pd.DataFrame, query_type: str) -> List[str]:
        """Generate data-driven insights"""
        insights = []
        
        if query_type == "traffic" and 'date' in df.columns:
            # Trend analysis
            if len(df) > 7:
                recent_week = df.tail(7)['sessions'].mean() if 'sessions' in df.columns else 0
                previous_week = df.iloc[-14:-7]['sessions'].mean() if len(df) >= 14 and 'sessions' in df.columns else recent_week
                
                if previous_week > 0:
                    change = ((recent_week - previous_week) / previous_week) * 100
                    trend = "increased" if change > 0 else "decreased"
                    insights.append(f"📈 Traffic has {trend} by {abs(change):.1f}% week-over-week")
            
            # Peak performance
            if 'sessions' in df.columns:
                peak_day = df.loc[df['sessions'].idxmax()]
                insights.append(f"🔥 Peak traffic day: {peak_day.get('date', 'N/A')} with {peak_day['sessions']:,} sessions")
        
        elif query_type == "acquisition" and 'source' in df.columns:
            top_source = df.loc[df['sessions'].idxmax()] if 'sessions' in df.columns else None
            if top_source is not None:
                insights.append(f"🎯 Top traffic source: {top_source['source']} ({top_source['sessions']:,} sessions)")
        
        elif query_type == "pages" and 'pagePath' in df.columns:
            if 'screenPageViews' in df.columns:
                top_page = df.loc[df['screenPageViews'].idxmax()]
                page_name = top_page['pagePath']
                insights.append(f"📄 Most viewed page: {page_name} ({top_page['screenPageViews']:,} views)")
                
                # Additional homepage insights
                homepage_data = df[df['pagePath'] == 'Homepage']
                if not homepage_data.empty and len(df) > 1:
                    homepage_views = homepage_data['screenPageViews'].sum()
                    total_views = df['screenPageViews'].sum()
                    homepage_percentage = (homepage_views / total_views) * 100
                    insights.append(f"🏠 Homepage accounts for {homepage_percentage:.1f}% of all page views")
        
        return insights
    
    def _create_visualizations(self, df: pd.DataFrame, query_config: Dict) -> List[Dict]:
        """Create appropriate visualizations based on query type"""
        visualizations = []
        
        if query_config['type'] == "traffic" and 'date' in df.columns:
            fig = self.visualizer.create_traffic_chart(df)
            visualizations.append({"type": "traffic_trend", "figure": fig})
        
        elif query_config['type'] == "acquisition":
            if 'source' in df.columns and 'sessions' in df.columns:
                fig = self.visualizer.create_comparison_chart(df, 'source', 'sessions')
                visualizations.append({"type": "source_comparison", "figure": fig})
        
        elif query_config['type'] == "technology":
            if 'deviceCategory' in df.columns and 'sessions' in df.columns:
                fig = self.visualizer.create_pie_chart(df, 'deviceCategory', 'sessions')
                visualizations.append({"type": "device_distribution", "figure": fig})
        
        elif query_config['type'] == "pages":
            if 'pagePath' in df.columns and 'screenPageViews' in df.columns:
                fig = self.visualizer.create_comparison_chart(df, 'pagePath', 'screenPageViews')
                visualizations.append({"type": "page_performance", "figure": fig})
        
        return visualizations

# Enhanced AutoGen Integration
def create_enhanced_analyst_agent():
    """Create enhanced analyst agent with better prompting"""
    return autogen.AssistantAgent(
        name="ga_analyst",
        llm_config=llm_config,
        system_message="""You are a senior Google Analytics consultant providing professional analysis for business clients.

CORE RESPONSIBILITIES:
1. Analyze GA4 data with business context and strategic insights
2. Identify actionable opportunities and potential issues  
3. Provide clear, executive-level recommendations
4. Structure analysis professionally for client presentation

ANALYSIS FRAMEWORK:
- Executive Summary (key metrics and timeframe)
- Key Findings (data-driven insights)
- Performance Analysis (trends, patterns, anomalies)
- Strategic Recommendations (specific, actionable steps)
- Next Steps (suggested follow-up analysis)

PROFESSIONAL STANDARDS:
- Use business language, not technical jargon
- Focus on ROI and business impact
- Provide context for all metrics
- Include confidence levels for predictions
- Suggest specific optimization opportunities

CRITICAL: Always structure your response with clear sections and actionable insights."""
    )

# Streamlit App Configuration
def configure_streamlit():
    """Configure Streamlit for professional appearance"""
    st.set_page_config(
        page_title="GA4 Analytics Intelligence Platform",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # Custom CSS for professional styling
    st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Source+Sans+Pro:wght@300;400;500;600;700&display=swap');
    
    html, body, [class*="css"] {
        font-family: 'Source Sans Pro', -apple-system, BlinkMacSystemFont, 'Segoe UI', Arial, sans-serif;
    }
    
    .main-header {
        font-size: 1.1rem;
        font-weight: 600;
        color: #1a202c;
        text-align: center;
        margin-bottom: 0.4rem;
        letter-spacing: -0.01em;
    }
    
    .subtitle {
        font-size: 0.9rem;
        font-weight: 400;
        color: #718096;
        text-align: center;
        margin-bottom: 1.25rem;
    }
    
    .metric-card {
        background: #ffffff;
        padding: 0.8rem;
        border-radius: 4px;
        border: 1px solid #d1d5db;
        margin: 0.4rem 0;
        font-size: 0.8rem;
        line-height: 1.3;
        color: #374151;
        font-weight: 500;
    }
    
    .insight-box {
        background: #f9fafb;
        padding: 0.7rem;
        border-radius: 4px;
        border-left: 1px solid #d1d5db;
        margin: 0.3rem 0;
        font-size: 0.8rem;
        line-height: 1.3;
        color: #6b7280;
    }
    
    .stButton > button {
        background-color: #4299e1;
        color: white;
        border: none;
        border-radius: 6px;
        padding: 0.45rem 0.9rem;
        font-weight: 500;
        font-size: 0.85rem;
        max-width: 280px;
        margin: 0 auto;
        display: block;
        font-family: 'Source Sans Pro', sans-serif;
    }
    
    .stButton > button:hover {
        background-color: #3182ce;
        border: none;
    }
    
    .sidebar .stButton > button {
        max-width: none;
        width: 100%;
    }
    
    /* Sidebar styling */
    .css-1d391kg {
        padding-top: 1rem;
    }
    
    /* Input styling */
    .stTextInput > div > div > input {
        font-size: 0.85rem;
        font-family: 'Source Sans Pro', sans-serif;
    }
    
    .stTextArea > div > div > textarea {
        font-size: 0.85rem;
        line-height: 1.4;
        font-family: 'Source Sans Pro', sans-serif;
    }
    
    /* Headers - Professional and compact */
    h1, h2, h3 {
        font-family: 'Source Sans Pro', sans-serif;
        font-weight: 600;
        letter-spacing: 0;
    }
    
    h2 {
        font-size: 0.65rem;
        color: #4a5568;
        margin-top: 0.5rem;
        margin-bottom: 0.2rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.1em;
    }
    
    h3 {
        font-size: 0.9rem;
        color: #4a5568;
        margin-bottom: 0.4rem;
        font-weight: 500;
    }
    
    /* Selectbox and other inputs */
    .stSelectbox > div > div {
        font-size: 0.85rem;
        font-family: 'Source Sans Pro', sans-serif;
    }
    
    /* Metrics */
    [data-testid="metric-container"] {
        background: white;
        border: 1px solid #e2e8f0;
        padding: 0.75rem;
        border-radius: 6px;
        box-shadow: 0 1px 2px rgba(0,0,0,0.03);
    }
    
    /* Tips section - More subtle */
    .tips-section {
        background: #f7fafc;
        padding: 0.85rem;
        border-radius: 6px;
        border-left: 2px solid #cbd5e0;
        font-size: 0.8rem;
        line-height: 1.3;
        color: #4a5568;
    }
    
    /* Chart containers with borders */
    .js-plotly-plot {
        border: 1px solid #e2e8f0 !important;
        border-radius: 6px !important;
        overflow: hidden !important;
    }
    
    /* Footer */
    .footer {
        text-align: center;
        color: #718096;
        font-size: 0.75rem;
        margin-top: 2.5rem;
        padding: 0.75rem;
        border-top: 1px solid #e2e8f0;
    }
    
    /* Remove default margins */
    .block-container {
        padding-top: 1rem;
        padding-bottom: 1rem;
        max-width: 1200px;
    }
    
    /* General text improvements */
    p, div, span {
        font-family: 'Source Sans Pro', sans-serif;
    }
    </style>
    """, unsafe_allow_html=True)

def main():
    """Main Streamlit application"""
    configure_streamlit()
    
    # Header
    st.markdown('<h1 class="main-header">GA4 Analytics Intelligence Platform</h1>', unsafe_allow_html=True)
    st.markdown('<div class="subtitle">Transform your Google Analytics data into actionable business insights</div>', unsafe_allow_html=True)
    
    # Sidebar configuration
    with st.sidebar:
        st.header("🔧 Configuration")
        
        # GA4 Property ID
        property_id = st.text_input(
            "GA4 Property ID",
            value=os.getenv("GA4_PROPERTY_ID", ""),
            help="Your Google Analytics 4 Property ID (numbers only)"
        )
        
        # Service Account Status
        service_account_path = "service-account.json"
        if os.path.exists(service_account_path):
            st.success("✅ Service Account: Connected")
        else:
            st.error("❌ Service Account: Not Found")
            st.info("Add your service-account.json file to the project directory")
        
        # Connection Test
        if st.button("🔍 Test Connection", use_container_width=True):
            if property_id and os.path.exists(service_account_path):
                ga_manager = GA4DataManager(property_id)
                with st.spinner("Testing connection..."):
                    result = ga_manager.authenticate()
                    if result["success"]:
                        st.success("✅ " + result["message"])
                    else:
                        st.error("❌ " + result["error"])
                        if "suggestion" in result:
                            st.info("💡 " + result["suggestion"])
            else:
                st.warning("⚠️ Please configure Property ID and service account")
        
        st.divider()
        
        # Quick Query Examples
        st.subheader("💡 Example Queries")
        example_queries = [
            "Show me traffic trends for the last 30 days",
            "Analyze top performing pages this month", 
            "Compare mobile vs desktop users",
            "What are my main traffic sources?",
            "Show conversion performance last week",
            "Analyze user engagement metrics"
        ]
        
        selected_example = st.selectbox(
            "Quick Examples:",
            [""] + example_queries,
            help="Select an example query to get started"
        )
    
    # Main content area
    col1, col2 = st.columns([3, 2])
    
    with col1:
        # User input with default prompt
        default_prompt = "Show me comprehensive website analytics for the last 30 days including traffic trends, top pages, traffic sources, and device usage"
        
        user_query = st.text_area(
            "What would you like to analyze?",
            value=selected_example if selected_example else default_prompt,
            height=120,
            placeholder="Example: 'Show me traffic trends for the last 30 days and identify my top traffic sources'"
        )
    
    with col2:
        st.markdown("### 🎯 Query Tips")
        st.markdown("""
        <div class="tips-section">
        <strong>Time Periods:</strong><br>
        • "last 30 days", "this month"<br>
        • "past week", "yesterday"<br><br>
        
        <strong>Analysis Types:</strong><br>
        • Traffic trends<br>
        • Page performance<br>
        • Traffic sources<br>
        • Device analysis<br>
        • Conversions
        </div>
        """, unsafe_allow_html=True)
    
    # Initialize session state for report data
    if 'report_data' not in st.session_state:
        st.session_state.report_data = None
    if 'show_raw_data' not in st.session_state:
        st.session_state.show_raw_data = False
    
    # Generate Report Button - centered and appropriately sized
    col1, col2, col3 = st.columns([1, 1, 1])
    with col2:
        generate_report = st.button("📊 Create Report", type="primary")
    
    if generate_report:
        if not property_id:
            st.error("❌ Please enter your GA4 Property ID in the sidebar")
        elif not os.path.exists(service_account_path):
            st.error("❌ Service account file not found. Please add service-account.json to your project directory")
        elif not user_query.strip():
            st.error("❌ Please enter your analysis request")
        else:
            # Initialize components
            ga_manager = GA4DataManager(property_id)
            report_generator = ReportGenerator(ga_manager)
            
            # Generate report
            with st.spinner("🔄 Analyzing your data... This may take a moment"):
                report = report_generator.generate_comprehensive_report(user_query)
                st.session_state.report_data = report
                st.session_state.show_raw_data = False  # Reset raw data view
    
    # Display report if we have data (either from current generation or session state)
    if st.session_state.report_data and st.session_state.report_data["success"]:
        report = st.session_state.report_data
        
        # Display Executive Summary
        st.markdown("## Executive Summary")
        st.markdown('<div class="metric-card">' + report["executive_summary"] + '</div>', unsafe_allow_html=True)
        
        # Display Key Insights
        if report.get("insights"):
            st.markdown("## Key Insights")
            for insight in report["insights"]:
                st.markdown('<div class="insight-box">' + insight + '</div>', unsafe_allow_html=True)
        
        # Display Visualizations
        if report.get("visualizations"):
            st.markdown("## Charts & Analysis")
            for viz in report["visualizations"]:
                st.plotly_chart(viz["figure"], use_container_width=True)
        
        # Data Export Section
        if report.get("data"):
            st.markdown("## Data Export")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                # CSV Export
                df = pd.DataFrame(report["data"])
                csv = df.to_csv(index=False)
                st.download_button(
                    "📄 Download CSV",
                    csv,
                    f"ga4_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    "text/csv",
                    use_container_width=True
                )
            
            with col2:
                # JSON Export
                json_data = json.dumps(report["data"], indent=2)
                st.download_button(
                    "📝 Download JSON",
                    json_data,
                    f"ga4_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                    "application/json",
                    use_container_width=True
                )
            
            with col3:
                # Toggle for raw data view
                if st.button("👁️ View Raw Data", use_container_width=True):
                    st.session_state.show_raw_data = not st.session_state.show_raw_data
            
            # Show raw data if toggled
            if st.session_state.show_raw_data:
                st.markdown("### Raw Data")
                st.dataframe(df, use_container_width=True)
        

    
    elif st.session_state.report_data and not st.session_state.report_data["success"]:
        st.error(f"❌ Report generation failed: {st.session_state.report_data['error']}")
        if st.session_state.report_data.get("suggestion"):
            st.info(f"💡 Suggestion: {st.session_state.report_data['suggestion']}")
    
    # Footer
    st.markdown("---")
    st.markdown(
        '<div class="footer"><strong>GA4 Analytics Intelligence Platform</strong> | Powered by Google Analytics 4 API & AI Analysis</div>',
        unsafe_allow_html=True
    )

if __name__ == "__main__":
    main()