import streamlit as st
import autogen
import json
import re
from typing import Dict, List, Optional
import logging
import time
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import os
from dotenv import load_dotenv

# GA4 API imports
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest
from google.oauth2 import service_account
import tempfile

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration for the language model
llm_config = {
    "config_list": [{
        "model": "gpt-4o-mini",
        "api_key": os.getenv("OPENAI_API_KEY"),
        "base_url": "https://api.openai.com/v1",
        "api_type": "openai"
    }],
    "temperature": 0,
    "timeout": 60,  # Reduced timeout to prevent hanging
    "seed": 42,
    "cache_seed": 42
}

# GA4 Functions
def authenticate_ga_service_account(property_id: str, service_account_file: str = "service-account.json") -> Dict:
    """Authenticate with GA4 using service account file"""
    try:
        # Read service account file directly
        if not os.path.exists(service_account_file):
            return {
                "success": False,
                "error": f"Service account file not found: {service_account_file}",
                "message": f"Service account file not found: {service_account_file}"
            }
        
        try:
            with open(service_account_file, 'r') as f:
                service_account_info = json.load(f)
        except json.JSONDecodeError as json_error:
            return {
                "success": False,
                "error": f"JSON parsing error: {str(json_error)}",
                "message": f"Failed to parse service account JSON: {str(json_error)}"
            }
        except Exception as file_error:
            return {
                "success": False,
                "error": f"File reading error: {str(file_error)}",
                "message": f"Failed to read service account file: {str(file_error)}"
            }
        
        # Create credentials directly from parsed JSON (no temp file needed)
        credentials = service_account.Credentials.from_service_account_info(
            service_account_info,
            scopes=['https://www.googleapis.com/auth/analytics.readonly']
        )
        
        # Create GA4 client
        client = BetaAnalyticsDataClient(credentials=credentials)
        
        # Test connection with a simple query
        request = RunReportRequest(
            property=f"properties/{property_id}",
            date_ranges=[DateRange(start_date="7daysAgo", end_date="today")],
            metrics=[Metric(name="sessions")],
            limit=1
        )
        
        response = client.run_report(request)
        
        return {
            "success": True,
            "client": client,
            "property_id": property_id,
            "message": f"Successfully authenticated with GA4 property {property_id}. Test query returned {len(response.rows)} rows."
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "message": f"Authentication failed: {str(e)}"
        }

def get_analytics_data(property_id: str, dimensions: List[str], metrics: List[str], 
                      start_date: str, end_date: str, limit: int = 10000, service_account_file: str = "service-account.json") -> Dict:
    """Query GA4 data"""
    try:
        # Get authenticated client (improved error handling)
        auth_result = authenticate_ga_service_account(property_id, service_account_file)
        if not auth_result["success"]:
            logger.error(f"Authentication failed in get_analytics_data: {auth_result['message']}")
            return auth_result
        
        client = auth_result["client"]
        
        # Build request
        request = RunReportRequest(
            property=f"properties/{property_id}",
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            dimensions=[Dimension(name=dim) for dim in dimensions],
            metrics=[Metric(name=metric) for metric in metrics],
            limit=limit
        )
        
        response = client.run_report(request)
        
        # Convert to list of dictionaries with proper type conversion
        data = []
        for row in response.rows:
            row_data = {}
            for i, dim in enumerate(dimensions):
                row_data[dim] = row.dimension_values[i].value
            for i, metric in enumerate(metrics):
                value = row.metric_values[i].value
                # Convert to appropriate type
                try:
                    row_data[metric] = float(value) if '.' in value else int(value)
                except ValueError:
                    row_data[metric] = value
            data.append(row_data)
        
        return {
            "success": True,
            "data": data,
            "row_count": len(data),
            "message": f"Successfully retrieved {len(data)} rows of data"
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "message": f"Data retrieval failed: {str(e)}"
        }

@st.cache_data(ttl=3600)  # Cache for 1 hour
def cached_get_analytics_data(property_id: str, dimensions: List[str], metrics: List[str], 
                             start_date: str, end_date: str, limit: int = 10000, service_account_file: str = "service-account.json") -> Dict:
    """Cached version of get_analytics_data for better performance"""
    return get_analytics_data(property_id, dimensions, metrics, start_date, end_date, limit, service_account_file)

def get_available_dimensions() -> Dict:
    """Get available GA4 dimensions"""
    try:
        # Common GA4 dimensions
        dimensions = [
            "date", "dateHour", "dateHourMinute",
            "pagePath", "pageTitle", "pageReferrer",
            "source", "medium", "campaign",
            "deviceCategory", "operatingSystem", "browser",
            "country", "city", "region",
            "userType", "sessionDefaultChannelGroup",
            "eventName", "customEvent:event_name"
        ]
        
        return {
            "success": True,
            "dimensions": dimensions,
            "message": f"Available {len(dimensions)} common GA4 dimensions"
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "message": f"Failed to get dimensions: {str(e)}"
        }

def get_available_metrics() -> Dict:
    """Get available GA4 metrics"""
    try:
        # Common GA4 metrics (corrected names)
        metrics = [
            "sessions", "totalUsers", "newUsers", "activeUsers",
            "screenPageViews", "eventCount",
            "bounceRate", "engagementRate",
            "averageSessionDuration", "sessionsPerUser",
            "conversions", "totalRevenue",
            "transactions", "purchaseRevenue"
        ]
        
        return {
            "success": True,
            "metrics": metrics,
            "message": f"Available {len(metrics)} common GA4 metrics"
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "message": f"Failed to get metrics: {str(e)}"
        }

def test_connection(property_id: str, service_account_file: str = "service-account.json") -> Dict:
    """Test GA4 connection"""
    try:
        result = get_analytics_data(
            property_id=property_id,
            dimensions=["date"],
            metrics=["sessions"],
            start_date="yesterday",
            end_date="today",
            limit=1,
            service_account_file=service_account_file
        )
        
        if result["success"]:
            return {
                "success": True,
                "message": f"Connection test successful! Property ID: {property_id}, Sessions yesterday: {result['data'][0]['sessions'] if result['data'] else 'No data'}"
            }
        else:
            return result
            
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "message": f"Connection test failed: {str(e)}"
        }

def validate_service_account_json(json_content: str) -> Dict:
    """Validate service account JSON structure and content"""
    try:
        # Parse JSON
        if isinstance(json_content, str):
            try:
                service_account_info = json.loads(json_content)
            except json.JSONDecodeError as e:
                return {
                    "valid": False,
                    "error": f"JSON parsing error: {str(e)}",
                    "message": "Invalid JSON format. Check for unterminated strings, missing quotes, or trailing commas."
                }
        else:
            service_account_info = json_content
        
        # Check required fields
        required_fields = [
            "type", "project_id", "private_key_id", "private_key", 
            "client_email", "client_id", "auth_uri", "token_uri"
        ]
        
        missing_fields = []
        for field in required_fields:
            if field not in service_account_info:
                missing_fields.append(field)
        
        if missing_fields:
            return {
                "valid": False,
                "error": f"Missing required fields: {', '.join(missing_fields)}",
                "message": "Service account JSON is missing required fields"
            }
        
        # Validate field contents
        if service_account_info.get("type") != "service_account":
            return {
                "valid": False,
                "error": "Invalid type field",
                "message": "Type field must be 'service_account'"
            }
        
        if not service_account_info.get("private_key", "").startswith("-----BEGIN PRIVATE KEY-----"):
            return {
                "valid": False,
                "error": "Invalid private key format",
                "message": "Private key must start with '-----BEGIN PRIVATE KEY-----'"
            }
        
        if "@" not in service_account_info.get("client_email", ""):
            return {
                "valid": False,
                "error": "Invalid client email",
                "message": "Client email must be a valid email address"
            }
        
        return {
            "valid": True,
            "message": f"Service account JSON is valid. Project: {service_account_info.get('project_id')}, Email: {service_account_info.get('client_email')}"
        }
        
    except Exception as e:
        return {
            "valid": False,
            "error": str(e),
            "message": f"Validation error: {str(e)}"
        }

# Remove global variable - will use Streamlit session state instead

# Initialize agents
user_proxy = autogen.UserProxyAgent(
    name="user_proxy",
    system_message="A human user interacting with the Google Analytics analysis system.",
    code_execution_config=False,
    human_input_mode="NEVER",
    llm_config=llm_config
)

ga_analyst = autogen.AssistantAgent(
    name="ga_analyst",
    llm_config=llm_config,
    system_message="""You are a Google Analytics 4 expert focused on data analysis. You will receive GA4 data and provide insights.

    Your role:
    1. Analyze Google Analytics data provided to you
    2. Identify trends and patterns
    3. Provide actionable insights and recommendations
    4. Structure your analysis clearly
    
    AVAILABLE FUNCTIONS:
    - authenticate_ga_service_account(property_id): Authenticate using service-account.json file
    - test_connection(property_id): Test GA4 connection
    - get_analytics_data(property_id, dimensions, metrics, start_date, end_date): Query GA4 data
    - get_available_dimensions(): List available dimensions
    - get_available_metrics(): List available metrics
    
    IMPORTANT: 
    - The service account JSON is automatically loaded from service-account.json file. 
    - Do NOT try to pass JSON content as parameters - just use the property_id.
    - Use current date ranges like "30daysAgo", "7daysAgo", "yesterday", "today" - NOT fixed dates like "2023-09-01"
    - Always use "totalUsers" metric, never "users" (which is invalid in GA4 API)
    
    QUERY INTELLIGENCE:
    - Focus on common GA4 metrics: sessions, totalUsers, newUsers, screenPageViews, conversions
    - Use common dimensions: date, source, medium, pagePath, deviceCategory
    - Consider data volume and sampling when analyzing trends
    - IMPORTANT: Use 'totalUsers' not 'users', 'newUsers' for new users
    
    CRITICAL RULES:
    - Focus ONLY on the analysis task
    - Do not engage in casual conversation or pleasantries
    - Complete the analysis and provide a final summary
    - Stop after providing the analysis results
    
    Always structure your analysis with:
    1. Key Findings
    2. Trends Analysis  
    3. Recommendations
    4. Next Steps"""
)

data_visualizer = autogen.AssistantAgent(
    name="data_visualizer",
    llm_config=llm_config,
    system_message="""You create compelling visualizations from Google Analytics data.
    Focus on:
    - Time series for trends
    - Bar charts for comparisons
    - Pie charts for distributions
    - Heatmaps for patterns
    Always provide context and insights with visualizations."""
)

# Register GA4 functions with agents to prevent hanging
ga_analyst.register_for_execution(name="authenticate_ga_service_account")(authenticate_ga_service_account)
ga_analyst.register_for_execution(name="get_analytics_data")(get_analytics_data)
ga_analyst.register_for_execution(name="test_connection")(test_connection)
ga_analyst.register_for_execution(name="get_available_dimensions")(get_available_dimensions)
ga_analyst.register_for_execution(name="get_available_metrics")(get_available_metrics)

user_proxy.register_for_llm(name="authenticate_ga_service_account", description="Authenticate with GA4 using service account file (property_id required)")(authenticate_ga_service_account)
user_proxy.register_for_llm(name="get_analytics_data", description="Query GA4 data with dimensions and metrics (property_id, dimensions, metrics, start_date, end_date required)")(get_analytics_data)
user_proxy.register_for_llm(name="test_connection", description="Test GA4 connection (property_id required)")(test_connection)
user_proxy.register_for_llm(name="get_available_dimensions", description="Get available GA4 dimensions")(get_available_dimensions)
user_proxy.register_for_llm(name="get_available_metrics", description="Get available GA4 metrics")(get_available_metrics)

print("✅ GA4 functions registered with AutoGen agents")

def parse_natural_language_query(user_request: str) -> Dict:
    """Parse user request into GA4 dimensions/metrics"""
    user_request_lower = user_request.lower()
    
    # Page analysis patterns - check these first as they're more specific
    if any(phrase in user_request_lower for phrase in ['top pages', 'top performing pages', 'best pages', 'most viewed pages', 'page views', 'popular pages']):
        return {'dimensions': ['pagePath'], 'metrics': ['screenPageViews']}
    
    # Traffic source patterns
    if any(phrase in user_request_lower for phrase in ['traffic source', 'referral', 'where traffic', 'how users found']):
        return {'dimensions': ['source', 'medium'], 'metrics': ['sessions']}
    
    # Device patterns
    if any(phrase in user_request_lower for phrase in ['device', 'mobile', 'desktop', 'tablet']):
        return {'dimensions': ['deviceCategory'], 'metrics': ['sessions']}
    
    # Geographic patterns
    if any(phrase in user_request_lower for phrase in ['country', 'location', 'geographic', 'where users', 'city']):
        return {'dimensions': ['country'], 'metrics': ['sessions']}
    
    # Conversion patterns
    if any(phrase in user_request_lower for phrase in ['conversion', 'goal', 'revenue', 'purchase']):
        return {'dimensions': ['date'], 'metrics': ['conversions', 'totalRevenue']}
    
    # Engagement patterns
    if any(phrase in user_request_lower for phrase in ['engagement', 'session duration', 'time on site']):
        return {'dimensions': ['date'], 'metrics': ['engagementRate', 'averageSessionDuration']}
    
    # Channel patterns
    if any(phrase in user_request_lower for phrase in ['channel', 'organic', 'paid', 'social', 'direct']):
        return {'dimensions': ['sessionDefaultChannelGroup'], 'metrics': ['sessions', 'conversions']}
    
    # Default to traffic trends for general queries
    return {'dimensions': ['date'], 'metrics': ['sessions', 'totalUsers']}

def parse_date_range(user_request: str) -> tuple:
    """Extract date range from natural language"""
    user_request_lower = user_request.lower()
    
    if "last 30 days" in user_request_lower or "past month" in user_request_lower:
        return "30daysAgo", "today"
    elif "last 7 days" in user_request_lower or "past week" in user_request_lower:
        return "7daysAgo", "today"
    elif "last 90 days" in user_request_lower or "past 3 months" in user_request_lower:
        return "90daysAgo", "today"
    elif "yesterday" in user_request_lower:
        return "yesterday", "yesterday"
    elif "today" in user_request_lower:
        return "today", "today"
    elif "this month" in user_request_lower:
        return "30daysAgo", "today"
    elif "this week" in user_request_lower:
        return "7daysAgo", "today"
    
    return "7daysAgo", "today"  # default

def validate_date_range(start_date: str, end_date: str, user_request: str) -> tuple:
    """Validate and adjust date ranges to prevent quota issues"""
    # If asking for long periods, suggest shorter ranges or sampling
    if "90daysAgo" in start_date and "complex" in user_request.lower():
        return "30daysAgo", "today"  # Reduce scope
    elif "90daysAgo" in start_date and "trend" in user_request.lower():
        return "30daysAgo", "today"  # Reduce scope for trend analysis
    elif "365daysAgo" in start_date:
        return "90daysAgo", "today"  # Limit to 90 days for very long periods
    return start_date, end_date

def validate_query_complexity(dimensions: List[str], metrics: List[str]) -> Dict:
    """Validate that dimension/metric combinations are compatible"""
    issues = []
    
    # Check dimension limits
    if len(dimensions) > 9:
        issues.append(f"Too many dimensions ({len(dimensions)}). GA4 allows maximum 9 dimensions per query.")
    
    # Check metric limits
    if len(metrics) > 10:
        issues.append(f"Too many metrics ({len(metrics)}). GA4 allows maximum 10 metrics per query.")
    
    # Check for incompatible combinations
    incompatible_combinations = [
        (['dateHourMinute'], ['totalRevenue']),  # High-cardinality dimension with revenue
        (['pagePath'], ['totalRevenue']),  # Page path with revenue (needs ecommerce)
        (['eventName'], ['totalRevenue'])   # Event name with revenue (needs ecommerce)
    ]
    
    for dims, mets in incompatible_combinations:
        if all(dim in dimensions for dim in dims) and any(metric in metrics for metric in mets):
            issues.append(f"Incompatible combination: {dims} with {mets}")
    
    # Check for required dimensions
    if 'dateHourMinute' in dimensions and len(dimensions) > 1:
        issues.append("dateHourMinute should be used alone or with minimal other dimensions due to high cardinality")
    
    return {
        "valid": len(issues) == 0,
        "issues": issues,
        "message": "Query validation passed" if len(issues) == 0 else f"Query validation issues: {'; '.join(issues)}"
    }

def create_ga_visualization(data: List[Dict], analysis_type: str):
    """Create visualizations for Google Analytics data."""
    try:
        df = pd.DataFrame(data)
        
        # Format dates if present
        if "date" in df.columns:
            df['date'] = pd.to_datetime(df['date'], format='%Y%m%d')
            df = df.sort_values('date')  # Sort by date for proper time series
        
        if analysis_type == "traffic":
            # Traffic analysis with multiple metrics
            if "date" in df.columns:
                # Create subplots for multiple metrics
                from plotly.subplots import make_subplots
                import plotly.graph_objects as go
                
                # Determine which metrics are available
                metrics_to_plot = []
                if "sessions" in df.columns:
                    metrics_to_plot.append(("sessions", "Sessions"))
                if "totalUsers" in df.columns:
                    metrics_to_plot.append(("totalUsers", "Users"))
                if "screenPageViews" in df.columns:
                    metrics_to_plot.append(("screenPageViews", "Page Views"))
                if "conversions" in df.columns:
                    metrics_to_plot.append(("conversions", "Conversions"))
                
                if len(metrics_to_plot) > 1:
                    # Create multi-line chart
                    fig = go.Figure()
                    
                    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728']
                    for i, (metric, label) in enumerate(metrics_to_plot):
                        fig.add_trace(go.Scatter(
                            x=df['date'],
                            y=df[metric],
                            mode='lines+markers',
                            name=label,
                            line=dict(color=colors[i % len(colors)], width=3),
                            marker=dict(size=6)
                        ))
                    
                    fig.update_layout(
                        title="Website Traffic Trends Over Time",
                        xaxis_title="Date",
                        yaxis_title="Count",
                        hovermode='x unified',
                        legend=dict(
                            yanchor="top",
                            y=0.99,
                            xanchor="left",
                            x=0.01
                        ),
                        height=500
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)
                elif len(metrics_to_plot) == 1:
                    # Single metric line chart
                    metric, label = metrics_to_plot[0]
                    fig = px.line(df, x="date", y=metric, 
                                title=f"Website {label} Over Time",
                                markers=True)
                    fig.update_traces(line=dict(width=3), marker=dict(size=6))
                    fig.update_layout(height=500)
                    st.plotly_chart(fig, use_container_width=True)
                
        elif analysis_type == "sources":
            # Traffic sources
            if "source" in df.columns and "sessions" in df.columns:
                # Sort by sessions descending and take top 10
                df_sorted = df.sort_values("sessions", ascending=False).head(10)
                fig = px.bar(df_sorted, x="source", y="sessions",
                           title="Top Traffic Sources",
                           color="sessions",
                           color_continuous_scale="viridis")
                fig.update_layout(height=500)
                st.plotly_chart(fig, use_container_width=True)
                
        elif analysis_type == "pages":
            # Page performance
            if "pagePath" in df.columns and "screenPageViews" in df.columns:
                top_pages = df.nlargest(10, "screenPageViews")
                fig = px.bar(top_pages, x="pagePath", y="screenPageViews",
                           title="Top Pages by Views",
                           color="screenPageViews",
                           color_continuous_scale="plasma")
                fig.update_layout(height=500, xaxis_tickangle=-45)
                st.plotly_chart(fig, use_container_width=True)
                
        elif analysis_type == "devices":
            # Device analysis
            if "deviceCategory" in df.columns and "sessions" in df.columns:
                fig = px.pie(df, values="sessions", names="deviceCategory",
                           title="Traffic by Device Category",
                           color_discrete_sequence=px.colors.qualitative.Set3)
                fig.update_layout(height=500)
                st.plotly_chart(fig, use_container_width=True)
                
        else:
            # Generic visualization
            numeric_cols = df.select_dtypes(include=['number']).columns
            if len(numeric_cols) > 0 and len(df) > 0:
                if "date" in df.columns:
                    # Time series for generic data
                    fig = px.line(df, x="date", y=numeric_cols[0],
                               title="Analytics Data Over Time",
                               markers=True)
                else:
                    # Bar chart for non-time series
                    fig = px.bar(df, x=df.columns[0], y=numeric_cols[0],
                               title="Analytics Data")
                fig.update_layout(height=500)
                st.plotly_chart(fig, use_container_width=True)
        
        # Display data matrix/table below the graph
        st.markdown("### 📊 Data Matrix")
        
        # Format the dataframe for display
        display_df = df.copy()
        if "date" in display_df.columns:
            # Format date for better readability
            display_df['date'] = display_df['date'].dt.strftime('%Y-%m-%d')
        
        # Reorder columns for better presentation
        preferred_order = ['date', 'totalUsers', 'sessions', 'screenPageViews', 'conversions']
        existing_cols = [col for col in preferred_order if col in display_df.columns]
        other_cols = [col for col in display_df.columns if col not in existing_cols]
        display_df = display_df[existing_cols + other_cols]
        
        # Display with better formatting
        st.dataframe(
            display_df, 
            use_container_width=True,
            height=400,
            column_config={
                "date": st.column_config.DateColumn("Date"),
                "totalUsers": st.column_config.NumberColumn("Users", format="%d"),
                "sessions": st.column_config.NumberColumn("Sessions", format="%d"),
                "screenPageViews": st.column_config.NumberColumn("Page Views", format="%d"),
                "conversions": st.column_config.NumberColumn("Conversions", format="%d"),
            }
        )
        
        # Add summary statistics
        st.markdown("### 📈 Summary Statistics")
        col1, col2, col3, col4 = st.columns(4)
        
        if "totalUsers" in df.columns:
            with col1:
                st.metric("Total Users", f"{df['totalUsers'].sum():,}")
        if "sessions" in df.columns:
            with col2:
                st.metric("Total Sessions", f"{df['sessions'].sum():,}")
        if "screenPageViews" in df.columns:
            with col3:
                st.metric("Total Page Views", f"{df['screenPageViews'].sum():,}")
        if "conversions" in df.columns:
            with col4:
                st.metric("Total Conversions", f"{df['conversions'].sum():,}")
                
    except Exception as e:
        logger.error(f"Error creating visualization: {str(e)}")
        st.warning(f"Could not create visualization: {str(e)}")
        st.error(f"Error details: {str(e)}")
        # Still show raw data if visualization fails
        if 'df' in locals():
            st.dataframe(df)

def create_user_analytics_summary(data: List[Dict]) -> str:
    """Create a detailed user analytics summary from GA4 data"""
    if not data:
        return "❌ No user data available."
    
    df = pd.DataFrame(data)
    
    # Sort by date for chronological analysis
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], format='%Y%m%d')
        df = df.sort_values('date')
    
    summary = []
    
    # User Overview
    summary.append("## 👥 User Analytics Overview")
    
    if 'totalUsers' in df.columns:
        total_users = df['totalUsers'].sum()
        avg_daily_users = df['totalUsers'].mean()
        max_users_day = df.loc[df['totalUsers'].idxmax()]
        min_users_day = df.loc[df['totalUsers'].idxmin()]
        
        summary.append(f"**📊 Total Users (30 days)**: {total_users:,}")
        summary.append(f"**📅 Average Daily Users**: {avg_daily_users:.0f}")
        summary.append(f"**🔥 Peak User Day**: {max_users_day['date'].strftime('%Y-%m-%d')} ({max_users_day['totalUsers']} users)")
        summary.append(f"**📉 Lowest User Day**: {min_users_day['date'].strftime('%Y-%m-%d')} ({min_users_day['totalUsers']} users)")
    
    # Session Analysis
    if 'sessions' in df.columns:
        total_sessions = df['sessions'].sum()
        avg_sessions_per_user = total_sessions / total_users if 'totalUsers' in df.columns and total_users > 0 else 0
        
        summary.append(f"\n**🔄 Total Sessions**: {total_sessions:,}")
        summary.append(f"**👤 Avg Sessions per User**: {avg_sessions_per_user:.1f}")
    
    # Engagement Metrics
    if 'screenPageViews' in df.columns:
        total_pageviews = df['screenPageViews'].sum()
        pages_per_session = total_pageviews / total_sessions if 'sessions' in df.columns and total_sessions > 0 else 0
        
        summary.append(f"\n**📄 Total Page Views**: {total_pageviews:,}")
        summary.append(f"**📖 Pages per Session**: {pages_per_session:.1f}")
    
    # Conversion Analysis
    if 'conversions' in df.columns:
        total_conversions = df['conversions'].sum()
        conversion_rate = (total_conversions / total_users * 100) if 'totalUsers' in df.columns and total_users > 0 else 0
        
        summary.append(f"\n**🎯 Total Conversions**: {total_conversions:,}")
        summary.append(f"**💹 User Conversion Rate**: {conversion_rate:.1f}%")
    
    # Daily User Trends
    summary.append("\n## 📈 Daily User Activity")
    
    if len(df) > 1 and 'totalUsers' in df.columns:
        # Calculate week-over-week change
        df_recent = df.tail(7)
        df_previous = df.iloc[-14:-7] if len(df) >= 14 else df.head(7)
        
        recent_avg = df_recent['totalUsers'].mean()
        previous_avg = df_previous['totalUsers'].mean()
        
        if previous_avg > 0:
            change_pct = ((recent_avg - previous_avg) / previous_avg) * 100
            trend_emoji = "📈" if change_pct > 0 else "📉" if change_pct < 0 else "➡️"
            summary.append(f"{trend_emoji} **Week-over-Week Change**: {change_pct:+.1f}%")
    
    # Top Performance Days
    if 'totalUsers' in df.columns and len(df) >= 5:
        summary.append("\n## 🏆 Top User Activity Days")
        top_days = df.nlargest(5, 'totalUsers')
        
        for i, (_, day) in enumerate(top_days.iterrows(), 1):
            date_str = day['date'].strftime('%Y-%m-%d (%A)')
            users = day['totalUsers']
            sessions = day.get('sessions', 'N/A')
            summary.append(f"{i}. **{date_str}**: {users} users, {sessions} sessions")
    
    return "\n".join(summary)

def process_ga_request(user_request: str, property_id: str) -> str:
    """Process user's Google Analytics request."""
    if not user_request:
        return "No request provided. Please enter an analysis request."
    
    try:
        logger.info(f"Processing GA request: {user_request}")
        
        # Parse query and date range for logging
        query_config = parse_natural_language_query(user_request)
        date_range = parse_date_range(user_request)
        logger.info(f"Query config: {query_config}")
        logger.info(f"Date range: {date_range}")
        
        # Test connection first
        logger.info("Testing GA4 connection...")
        connection_test = test_connection(property_id)
        if not connection_test['success']:
            logger.error(f"Connection test failed: {connection_test['message']}")
            return f"❌ Connection failed: {connection_test['message']}"
        logger.info(f"Connection test successful: {connection_test['message']}")
        
        # Use a direct conversation with clear termination
        try:
            # Set a shorter timeout for the conversation
            response = user_proxy.initiate_chat(
                ga_analyst,
                message=f"""
                            TASK: Analyze this Google Analytics request: {user_request}
            
            Authentication details:
            - Service Account File: service-account.json (automatically loaded)
            - Property ID: {property_id}
                
                IMPORTANT DATA HANDLING GUIDELINES:
                1. Start with small date ranges (7-30 days) to test data availability
                2. Use appropriate row limits (max 10,000 rows per query)
                3. Focus on key metrics: sessions, users, pageviews, conversions
                4. If data volume is large, suggest sampling or date range adjustments
                5. Check data availability before running complex queries
                
                REQUIRED STEPS:
                1. Authenticate with GA4 using the service account
                2. Test connection and check available data
                3. Query relevant data with appropriate limits
                4. Provide analysis with insights and data volume considerations
                
                CRITICAL: Complete the analysis and provide a final summary. Do not engage in casual conversation.
                If you encounter any issues, explain what went wrong clearly and stop.
                """,
                max_turns=3  # Limit conversation turns to prevent hanging
            )
            
            # Extract the response with better error handling
            if response and hasattr(response, 'summary') and response.summary:
                result_text = response.summary
            elif response and hasattr(response, 'chat_history') and response.chat_history:
                # Get the last assistant message from chat history
                last_msg = None
                for msg in reversed(response.chat_history):
                    if msg.get('role') == 'assistant' or msg.get('name') == 'ga_analyst':
                        last_msg = msg.get('content', '')
                        break
                result_text = last_msg if last_msg else "Analysis completed but no response received."
            else:
                result_text = "Analysis completed but response format was unexpected."
                
        except Exception as chat_error:
            logger.error(f"AutoGen chat error: {str(chat_error)}")
            result_text = f"❌ Analysis failed due to chat error: {str(chat_error)}\n\nTrying direct analysis instead..."
            
            # Fallback to direct analysis without AutoGen
            try:
                # Parse query and get data directly
                query_config = parse_natural_language_query(user_request)
                date_range = parse_date_range(user_request)
                
                # Get data for analysis
                viz_data = cached_get_analytics_data(
                    property_id=property_id,
                    dimensions=query_config['dimensions'],
                    metrics=query_config['metrics'],
                    start_date=date_range[0],
                    end_date=date_range[1],
                    limit=1000
                )
                
                if viz_data['success']:
                    result_text = f"✅ Direct Analysis Results:\n\n"
                    result_text += f"**Data Retrieved**: {len(viz_data['data'])} rows\n"
                    result_text += f"**Date Range**: {date_range[0]} to {date_range[1]}\n"
                    result_text += f"**Dimensions**: {', '.join(query_config['dimensions'])}\n"
                    result_text += f"**Metrics**: {', '.join(query_config['metrics'])}\n\n"
                    
                    # Basic analysis
                    if viz_data['data']:
                        user_analytics = create_user_analytics_summary(viz_data['data'])
                        result_text = f"✅ Direct Analysis Results:\n\n{user_analytics}"
                else:
                    result_text += f"\n❌ Data retrieval also failed: {viz_data['message']}"
                    
            except Exception as fallback_error:
                logger.error(f"Fallback analysis error: {str(fallback_error)}")
                result_text += f"\n❌ Fallback analysis also failed: {str(fallback_error)}"
        
        # Try to extract data for visualization
        try:
            # Parse the user request to determine visualization type
            query_config = parse_natural_language_query(user_request)
            date_range = parse_date_range(user_request)
            
            # Validate date range to prevent quota issues
            validated_date_range = validate_date_range(date_range[0], date_range[1], user_request)
            if validated_date_range != date_range:
                logger.info(f"Date range adjusted from {date_range} to {validated_date_range} to prevent quota issues")
            
            # Validate query complexity before execution
            query_validation = validate_query_complexity(query_config['dimensions'], query_config['metrics'])
            if not query_validation['valid']:
                logger.warning(f"Query validation failed: {query_validation['message']}")
                result_text += f"\n\n⚠️ Query validation warning: {query_validation['message']}"
                # Continue with a simplified query
                simplified_dimensions = query_config['dimensions'][:3]  # Limit to 3 dimensions
                simplified_metrics = query_config['metrics'][:5]       # Limit to 5 metrics
                logger.info(f"Using simplified query: dimensions={simplified_dimensions}, metrics={simplified_metrics}")
            else:
                simplified_dimensions = query_config['dimensions']
                simplified_metrics = query_config['metrics']
            
            # Get data for visualization (using cached version for better performance)
            viz_data = cached_get_analytics_data(
                property_id=property_id,
                dimensions=simplified_dimensions,
                metrics=simplified_metrics,
                start_date=validated_date_range[0],
                end_date=validated_date_range[1],
                limit=1000  # Limit for visualization
            )
            
            if viz_data['success'] and viz_data['data']:
                # Determine visualization type based on query
                if 'date' in query_config['dimensions']:
                    viz_type = "traffic"
                elif 'pagePath' in query_config['dimensions']:
                    viz_type = "pages"
                elif 'source' in query_config['dimensions']:
                    viz_type = "sources"
                elif 'deviceCategory' in query_config['dimensions']:
                    viz_type = "devices"
                else:
                    viz_type = "generic"
                
                # Create visualization (includes data matrix and summary stats)
                create_ga_visualization(viz_data['data'], viz_type)
                
                # Add data export option
                df = pd.DataFrame(viz_data['data'])
                
                # Export button
                st.markdown("### 📥 Export Data")
                csv = df.to_csv(index=False)
                st.download_button(
                    "Download Data as CSV", 
                    csv, 
                    f"ga4_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", 
                    "text/csv",
                    help="Download the analyzed data as a CSV file",
                    use_container_width=True
                )
                
                result_text += f"\n\n📊 Visualization created for {len(viz_data['data'])} data points."
                result_text += f"\n📥 Data export available above."
                
        except Exception as e:
            logger.error(f"Visualization error: {str(e)}")
            result_text += f"\n\n⚠️ Could not create visualization: {str(e)}"
        
        # Generate user analytics summary instead of generic analysis
        if 'viz_data' in locals() and viz_data['success'] and viz_data['data']:
            user_analytics = create_user_analytics_summary(viz_data['data'])
            return user_analytics
        else:
            return result_text  # Fallback to original text if no data
            
    except Exception as e:
        logger.error(f"Error processing GA request: {str(e)}")
        return f"Error processing request: {str(e)}"

def test_app_startup():
    """Test that all components are properly initialized"""
    try:
        logger.info("Testing app startup...")
        
        # Test environment variables
        property_id = os.getenv("GA4_PROPERTY_ID")
        openai_key = os.getenv("OPENAI_API_KEY")
        
        if not property_id:
            logger.warning("GA4_PROPERTY_ID not found in environment")
        else:
            logger.info(f"GA4_PROPERTY_ID found: {property_id}")
            
        if not openai_key:
            logger.warning("OPENAI_API_KEY not found in environment")
        else:
            logger.info("OPENAI_API_KEY found")
            
        # Test service account file
        if os.path.exists("service-account.json"):
            logger.info("Service account file found")
        else:
            logger.warning("Service account file not found")
            
        # Test AutoGen agents
        if ga_analyst and user_proxy:
            logger.info("AutoGen agents initialized successfully")
        else:
            logger.error("Failed to initialize AutoGen agents")
            
        logger.info("App startup test completed")
        return True
        
    except Exception as e:
        logger.error(f"App startup test failed: {str(e)}")
        return False

def main():
    st.title("Google Analytics AI Report Generator (Service Account)")
    
    # Test app startup
    startup_success = test_app_startup()
    if not startup_success:
        st.error("⚠️ App startup test failed. Check logs for details.")
        st.info("Some features may not work correctly.")
    
    # Sidebar configuration  
    st.sidebar.header("Configuration")
    
    # Get GA4 Property ID from environment variables
    property_id = os.getenv("GA4_PROPERTY_ID")
    if property_id:
        st.sidebar.success(f"✅ GA4 Property ID: {property_id}")
    else:
        st.sidebar.error("❌ GA4_PROPERTY_ID not found in environment variables")
        st.sidebar.info("Please add GA4_PROPERTY_ID to your .env file")
    
    # Load service account JSON file automatically
    st.sidebar.markdown("**Google Analytics Authentication:**")
    
    service_account_path = "service-account.json"
    if os.path.exists(service_account_path):
        try:
            with open(service_account_path, 'r') as f:
                service_account_json = f.read()
            
            # Validate the service account JSON
            validation_result = validate_service_account_json(service_account_json)
            if validation_result["valid"]:
                st.sidebar.success("✅ Service account file loaded and validated")
                st.sidebar.info(validation_result["message"])
                uploaded_file = True
            else:
                st.sidebar.error(f"❌ Service account validation failed: {validation_result['message']}")
                st.sidebar.error(f"Error details: {validation_result['error']}")
                uploaded_file = None
                
        except Exception as e:
            st.sidebar.error(f"❌ Error loading service account file: {str(e)}")
            uploaded_file = None
    else:
        st.sidebar.error("❌ service-account.json file not found")
        st.sidebar.info("Please add your Google Cloud service account JSON file to the project directory")
        uploaded_file = None
    
    # Authentication status
    auth_status = st.sidebar.empty()
    
    # Test connection button
    if st.sidebar.button("🔍 Test GA4 Connection", type="secondary"):
        if property_id and uploaded_file:
            with st.sidebar.spinner("Testing connection..."):
                test_result = test_connection(property_id)
                if test_result['success']:
                    st.sidebar.success("✅ " + test_result['message'])
                else:
                    st.sidebar.error("❌ " + test_result['message'])
        else:
            st.sidebar.warning("⚠️ Please ensure Property ID and service account file are configured")
    
    # Example queries
    st.markdown("""
    ### Example Analysis Requests:
    - "Show me website traffic trends for the last 30 days"
    - "Analyze traffic sources and their performance"
    - "Find top performing pages by page views"
    - "Compare mobile vs desktop user behavior"
    - "Show conversion funnel analysis"
    - "Analyze user engagement by country"
    """)
    
    # User input
    user_request = st.text_input(
        "What would you like to analyze?",
        placeholder="Enter your Google Analytics analysis request..."
    )
    
    # Process request button
    if st.button("Generate Analysis", type="primary"):
        if not property_id:
            st.error("GA4_PROPERTY_ID not found in environment variables. Please add it to your .env file.")
        elif not uploaded_file:
            st.error("Service account file not found. Please ensure service-account.json exists in the project directory.")
        elif not user_request:
            st.error("Please enter an analysis request")
        else:
            try:
                # Service account JSON is loaded automatically from file
                
                # Update authentication status
                auth_status.info("🔄 Processing request...")
                
                # Process the request
                with st.spinner("Analyzing your Google Analytics data..."):
                    result = process_ga_request(user_request, property_id)
                
                # Display results - Show actual user data instead of AutoGen analysis
                st.success("Analysis completed!")
                st.markdown("### 📊 User Analytics Results:")
                st.markdown(result)
                
                # Update authentication status
                auth_status.success("✅ Analysis completed")
                
            except Exception as e:
                st.error(f"Error processing request: {str(e)}")
                auth_status.error("❌ Error occurred")

if __name__ == "__main__":
    main() 