import streamlit as st
import json
from typing import Dict, List, Optional
import logging
import time
from datetime import datetime, timedelta
import plotly.express as px
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

# GA4 Functions (same as before)
def authenticate_ga_service_account(service_account_json: str, property_id: str) -> Dict:
    """Authenticate with GA4 using service account"""
    try:
        # Create temporary file for service account credentials
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            f.write(service_account_json)
            temp_file = f.name
        
        # Create credentials from service account file
        credentials = service_account.Credentials.from_service_account_file(
            temp_file,
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
        
        # Clean up temp file
        os.unlink(temp_file)
        
        return {
            "success": True,
            "client": client,
            "property_id": property_id,
            "message": f"Successfully authenticated with GA4 property {property_id}. Test query returned {len(response.rows)} rows."
        }
        
    except Exception as e:
        # Clean up temp file if it exists
        if 'temp_file' in locals():
            try:
                os.unlink(temp_file)
            except:
                pass
        
        return {
            "success": False,
            "error": str(e),
            "message": f"Authentication failed: {str(e)}"
        }

@st.cache_data(ttl=3600)  # Cache for 1 hour
def get_analytics_data(service_account_json: str, property_id: str, dimensions: List[str], metrics: List[str], 
                      start_date: str, end_date: str, limit: int = 10000) -> Dict:
    """Query GA4 data"""
    try:
        # Get authenticated client
        auth_result = authenticate_ga_service_account(service_account_json, property_id)
        if not auth_result["success"]:
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

def test_connection(service_account_json: str, property_id: str) -> Dict:
    """Test GA4 connection"""
    try:
        result = get_analytics_data(
            service_account_json=service_account_json,
            property_id=property_id,
            dimensions=["date"],
            metrics=["sessions"],
            start_date="yesterday",
            end_date="today",
            limit=1
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

def parse_natural_language_query(user_request: str) -> Dict:
    """Parse user request into GA4 dimensions/metrics"""
    query_mapping = {
        'traffic trends': {'dimensions': ['date'], 'metrics': ['sessions', 'users']},
        'traffic sources': {'dimensions': ['source', 'medium'], 'metrics': ['sessions']},
        'top pages': {'dimensions': ['pagePath'], 'metrics': ['screenPageViews']},
        'device analysis': {'dimensions': ['deviceCategory'], 'metrics': ['sessions']},
        'country analysis': {'dimensions': ['country'], 'metrics': ['sessions']},
        'browser analysis': {'dimensions': ['browser'], 'metrics': ['sessions']},
        'conversion': {'dimensions': ['date'], 'metrics': ['conversions', 'totalRevenue']},
        'engagement': {'dimensions': ['date'], 'metrics': ['engagementRate', 'averageSessionDuration']},
        'bounce rate': {'dimensions': ['date'], 'metrics': ['bounceRate']},
        'new vs returning': {'dimensions': ['userType'], 'metrics': ['sessions']},
        'channel performance': {'dimensions': ['sessionDefaultChannelGroup'], 'metrics': ['sessions', 'conversions']},
        'page performance': {'dimensions': ['pagePath'], 'metrics': ['screenPageViews', 'bounceRate']},
        'geographic analysis': {'dimensions': ['country', 'city'], 'metrics': ['sessions']},
        'time analysis': {'dimensions': ['dateHour'], 'metrics': ['sessions']},
        'campaign analysis': {'dimensions': ['campaign'], 'metrics': ['sessions', 'conversions']}
    }
    
    user_request_lower = user_request.lower()
    for pattern, config in query_mapping.items():
        if pattern in user_request_lower:
            return config
    
    # Default fallback
    return {'dimensions': ['date'], 'metrics': ['sessions']}

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

def create_ga_visualization(data: List[Dict], analysis_type: str):
    """Create visualizations for Google Analytics data."""
    try:
        df = pd.DataFrame(data)
        
        if analysis_type == "traffic":
            # Traffic analysis
            if "date" in df.columns and "sessions" in df.columns:
                fig = px.line(df, x="date", y="sessions", 
                            title="Website Traffic Over Time")
                st.plotly_chart(fig)
                
        elif analysis_type == "sources":
            # Traffic sources
            if "source" in df.columns and "sessions" in df.columns:
                fig = px.bar(df, x="source", y="sessions",
                           title="Traffic by Source")
                st.plotly_chart(fig)
                
        elif analysis_type == "pages":
            # Page performance
            if "pagePath" in df.columns and "screenPageViews" in df.columns:
                top_pages = df.nlargest(10, "screenPageViews")
                fig = px.bar(top_pages, x="pagePath", y="screenPageViews",
                           title="Top Pages by Views")
                st.plotly_chart(fig)
                
        elif analysis_type == "devices":
            # Device analysis
            if "deviceCategory" in df.columns and "sessions" in df.columns:
                fig = px.pie(df, values="sessions", names="deviceCategory",
                           title="Traffic by Device Category")
                st.plotly_chart(fig)
                
        else:
            # Generic visualization
            numeric_cols = df.select_dtypes(include=['number']).columns
            if len(numeric_cols) > 0:
                fig = px.bar(df, x=df.columns[0], y=numeric_cols[0],
                           title="Analytics Data")
                st.plotly_chart(fig)
                
    except Exception as e:
        logger.error(f"Error creating visualization: {str(e)}")
        st.warning(f"Could not create visualization: {str(e)}")

def generate_basic_analysis(data: List[Dict], user_request: str) -> str:
    """Generate basic analysis without AutoGen"""
    if not data:
        return "❌ No data available for analysis."
    
    df = pd.DataFrame(data)
    analysis = []
    
    # Key Findings
    analysis.append("## 🎯 Key Findings")
    analysis.append(f"- **Total Records**: {len(df)} data points")
    
    # Identify numeric columns for analysis
    numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
    
    if 'sessions' in numeric_cols:
        total_sessions = df['sessions'].sum()
        avg_sessions = df['sessions'].mean()
        analysis.append(f"- **Total Sessions**: {total_sessions:,}")
        analysis.append(f"- **Average Sessions**: {avg_sessions:.1f}")
    
    if 'users' in numeric_cols:
        total_users = df['users'].sum()
        analysis.append(f"- **Total Users**: {total_users:,}")
    
    # Trends Analysis
    analysis.append("\n## 📈 Trends Analysis")
    
    if 'date' in df.columns and 'sessions' in numeric_cols:
        # Sort by date for trend analysis
        df_sorted = df.sort_values('date')
        if len(df_sorted) > 1:
            first_sessions = df_sorted.iloc[0]['sessions']
            last_sessions = df_sorted.iloc[-1]['sessions']
            if first_sessions > 0:
                trend_pct = ((last_sessions - first_sessions) / first_sessions) * 100
                trend_direction = "increased" if trend_pct > 0 else "decreased"
                analysis.append(f"- **Traffic Trend**: Sessions {trend_direction} by {abs(trend_pct):.1f}% over the period")
    
    # Top performers
    if len(df) > 1:
        analysis.append("\n## 🏆 Top Performers")
        
        # Find the dimension column (non-numeric, non-date)
        dimension_cols = [col for col in df.columns if col not in numeric_cols and col != 'date']
        
        if dimension_cols and 'sessions' in numeric_cols:
            top_dim = dimension_cols[0]
            top_performer = df.nlargest(1, 'sessions').iloc[0]
            analysis.append(f"- **Top {top_dim}**: {top_performer[top_dim]} ({top_performer['sessions']:,} sessions)")
    
    # Recommendations
    analysis.append("\n## 💡 Recommendations")
    analysis.append("- Monitor key metrics regularly")
    analysis.append("- Focus on top-performing channels/pages")
    analysis.append("- Investigate any significant drops in traffic")
    
    # Next Steps
    analysis.append("\n## 🎯 Next Steps")
    analysis.append("- Set up automated reports for key metrics")
    analysis.append("- Analyze user behavior patterns")
    analysis.append("- Compare performance against historical data")
    
    return "\n".join(analysis)

def process_ga_request(user_request: str, service_account_json: str, property_id: str) -> str:
    """Process user's Google Analytics request directly (no AutoGen)"""
    if not user_request:
        return "No request provided. Please enter an analysis request."
    
    try:
        logger.info(f"Processing GA request: {user_request}")
        
        # Parse query and date range
        query_config = parse_natural_language_query(user_request)
        date_range = parse_date_range(user_request)
        logger.info(f"Query config: {query_config}")
        logger.info(f"Date range: {date_range}")
        
        # Test connection first
        logger.info("Testing GA4 connection...")
        connection_test = test_connection(service_account_json, property_id)
        if not connection_test['success']:
            logger.error(f"Connection test failed: {connection_test['message']}")
            return f"❌ Connection failed: {connection_test['message']}"
        logger.info(f"Connection test successful: {connection_test['message']}")
        
        # Get data for analysis
        viz_data = get_analytics_data(
            service_account_json=service_account_json,
            property_id=property_id,
            dimensions=query_config['dimensions'],
            metrics=query_config['metrics'],
            start_date=date_range[0],
            end_date=date_range[1],
            limit=1000
        )
        
        if not viz_data['success']:
            return f"❌ Data retrieval failed: {viz_data['message']}"
        
        # Create visualization
        if viz_data['data']:
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
            
            # Create visualization
            create_ga_visualization(viz_data['data'], viz_type)
            
            # Add data export option
            df = pd.DataFrame(viz_data['data'])
            
            # Show data summary
            st.markdown("### 📊 Data Summary")
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Rows", len(df))
            with col2:
                st.metric("Date Range", f"{date_range[0]} to {date_range[1]}")
            with col3:
                st.metric("Dimensions", len(query_config['dimensions']))
            
            # Export button
            csv = df.to_csv(index=False)
            st.download_button(
                "📥 Download Data as CSV", 
                csv, 
                f"ga4_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", 
                "text/csv",
                help="Download the analyzed data as a CSV file"
            )
        
        # Generate analysis
        analysis_text = generate_basic_analysis(viz_data['data'], user_request)
        
        return analysis_text
            
    except Exception as e:
        logger.error(f"Error processing GA request: {str(e)}")
        return f"Error processing request: {str(e)}"

def main():
    st.title("Google Analytics AI Report Generator")
    
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
            st.sidebar.success("✅ Service account file loaded automatically")
            uploaded_file = True
        except Exception as e:
            st.sidebar.error(f"❌ Error loading service account file: {str(e)}")
            uploaded_file = None
    else:
        st.sidebar.error("❌ service-account.json file not found")
        uploaded_file = None
    
    # Test connection button
    if st.sidebar.button("🔍 Test GA4 Connection", type="secondary"):
        if property_id and uploaded_file:
            with st.sidebar.spinner("Testing connection..."):
                test_result = test_connection(service_account_json, property_id)
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
    - "Show conversion analysis"
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
                # Process the request
                with st.spinner("Analyzing your Google Analytics data..."):
                    result = process_ga_request(user_request, service_account_json, property_id)
                
                # Display results
                st.success("Analysis completed!")
                st.markdown("### Analysis Results:")
                st.markdown(result)
                
            except Exception as e:
                st.error(f"Error processing request: {str(e)}")

if __name__ == "__main__":
    main()