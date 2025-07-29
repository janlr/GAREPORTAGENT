import streamlit as st
import autogen
import json
import re
from typing import Dict, List, Optional
import logging
import time
from datetime import datetime
import plotly.express as px
import pandas as pd
import os
from dotenv import load_dotenv

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
    "timeout": 300,
    "seed": 42,
    "cache_seed": 42
}

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
    system_message="""You are a Google Analytics 4 expert. You can:
    1. Authenticate with GA4 properties using service accounts
    2. Query analytics data using dimensions and metrics
    3. Analyze trends and patterns
    4. Create visualizations and reports
    
    Available tools:
    - authenticate_ga_service_account: Set up GA4 connection with service account
    - get_analytics_data: Query GA4 data
    - get_available_dimensions: List available dimensions
    - get_available_metrics: List available metrics
    - get_property_info: Get property details
    - test_connection: Test GA4 connection
    
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

def process_ga_request(user_request: str, service_account_json: str, property_id: str) -> str:
    """Process user's Google Analytics request."""
    if not user_request:
        return "No request provided. Please enter an analysis request."
    
    try:
        logger.info(f"Processing GA request: {user_request}")
        
        # Use a direct conversation instead of group chat
        response = user_proxy.initiate_chat(
            ga_analyst,
            message=f"""
            Analyze this Google Analytics request: {user_request}
            
            Authentication details:
            - Service Account JSON: {service_account_json[:100]}...
            - Property ID: {property_id}
            
            Please:
            1. Authenticate with GA4 using the service account
            2. Query relevant data using appropriate dimensions and metrics
            3. Provide a clear analysis with insights
            
            If you encounter any issues, explain what went wrong clearly.
            """
        )
        
        # Extract the response
        if response and hasattr(response, 'summary'):
            return response.summary
        elif response and hasattr(response, 'last_message'):
            return response.last_message
        else:
            return "Analysis completed. Check the chat history for details."
            
    except Exception as e:
        logger.error(f"Error processing GA request: {str(e)}")
        return f"Error processing request: {str(e)}"

def main():
    st.title("Google Analytics AI Report Generator (Service Account)")
    
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
            uploaded_file = True  # Set to True since we have the file
        except Exception as e:
            st.sidebar.error(f"❌ Error loading service account file: {str(e)}")
            uploaded_file = None
    else:
        st.sidebar.error("❌ service-account.json file not found")
        uploaded_file = None
    
    # Authentication status
    auth_status = st.sidebar.empty()
    
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
                # Service account JSON is already loaded above
                
                # Update authentication status
                auth_status.info("🔄 Processing request...")
                
                # Process the request
                with st.spinner("Analyzing your Google Analytics data..."):
                    result = process_ga_request(user_request, service_account_json, property_id)
                
                # Display results
                st.success("Analysis completed!")
                st.markdown("### Analysis Results:")
                st.markdown(result)
                
                # Update authentication status
                auth_status.success("✅ Analysis completed")
                
            except Exception as e:
                st.error(f"Error processing request: {str(e)}")
                auth_status.error("❌ Error occurred")

if __name__ == "__main__":
    main() 