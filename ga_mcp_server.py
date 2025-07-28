import asyncio
import json
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Filter,
    FilterExpression,
    Metric,
    OrderBy,
    RunReportRequest,
)
from google.auth import default
from google.auth.transport.requests import Request
from google.oauth2 import service_account
from mcp.server import Server
from mcp.server.models import InitializationOptions
from mcp.server.stdio import stdio_server
from mcp.types import (
    CallToolRequest,
    CallToolResult,
    ListToolsRequest,
    ListToolsResult,
    Tool,
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GoogleAnalyticsMCPServer:
    def __init__(self):
        self.server = Server("google-analytics-mcp")
        self.client: Optional[BetaAnalyticsDataClient] = None
        self.property_id: Optional[str] = None
        self.credentials: Optional[service_account.Credentials] = None
        
        # Register tools
        self.server.list_tools(self.list_tools)
        self.server.call_tool(self.call_tool)
        
    async def list_tools(self, request: ListToolsRequest) -> ListToolsResult:
        """List available Google Analytics tools."""
        tools = [
            Tool(
                name="authenticate_ga_service_account",
                description="Authenticate with Google Analytics using service account",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "service_account_json": {
                            "type": "string",
                            "description": "Service account JSON content or file path"
                        },
                        "property_id": {
                            "type": "string", 
                            "description": "Google Analytics 4 property ID (not project ID)"
                        }
                    },
                    "required": ["service_account_json", "property_id"]
                }
            ),
            Tool(
                name="get_analytics_data",
                description="Get Google Analytics data with specified dimensions and metrics",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "dimensions": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "List of dimensions to include"
                        },
                        "metrics": {
                            "type": "array", 
                            "items": {"type": "string"},
                            "description": "List of metrics to include"
                        },
                        "date_range": {
                            "type": "object",
                            "properties": {
                                "start_date": {"type": "string"},
                                "end_date": {"type": "string"}
                            },
                            "description": "Date range for the report"
                        },
                        "limit": {
                            "type": "integer",
                            "description": "Maximum number of rows to return"
                        },
                        "filters": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Optional filters to apply"
                        }
                    },
                    "required": ["dimensions", "metrics"]
                }
            ),
            Tool(
                name="get_available_dimensions",
                description="Get list of available dimensions for the GA4 property",
                inputSchema={
                    "type": "object",
                    "properties": {},
                    "required": []
                }
            ),
            Tool(
                name="get_available_metrics", 
                description="Get list of available metrics for the GA4 property",
                inputSchema={
                    "type": "object",
                    "properties": {},
                    "required": []
                }
            ),
            Tool(
                name="get_property_info",
                description="Get information about the current GA4 property",
                inputSchema={
                    "type": "object", 
                    "properties": {},
                    "required": []
                }
            ),
            Tool(
                name="test_connection",
                description="Test the connection to Google Analytics",
                inputSchema={
                    "type": "object",
                    "properties": {},
                    "required": []
                }
            )
        ]
        return ListToolsResult(tools=tools)
    
    async def call_tool(self, request: CallToolRequest) -> CallToolResult:
        """Execute Google Analytics tool calls."""
        try:
            if request.name == "authenticate_ga_service_account":
                return await self._authenticate_ga_service_account(request.arguments)
            elif request.name == "get_analytics_data":
                return await self._get_analytics_data(request.arguments)
            elif request.name == "get_available_dimensions":
                return await self._get_available_dimensions(request.arguments)
            elif request.name == "get_available_metrics":
                return await self._get_available_metrics(request.arguments)
            elif request.name == "get_property_info":
                return await self._get_property_info(request.arguments)
            elif request.name == "test_connection":
                return await self._test_connection(request.arguments)
            else:
                raise ValueError(f"Unknown tool: {request.name}")
        except Exception as e:
            logger.error(f"Error in tool call {request.name}: {str(e)}")
            return CallToolResult(
                content=[{"type": "text", "text": f"Error: {str(e)}"}]
            )
    
    async def _authenticate_ga_service_account(self, args: Dict[str, Any]) -> CallToolResult:
        """Authenticate with Google Analytics using service account."""
        try:
            service_account_json = args["service_account_json"]
            self.property_id = args["property_id"]
            
            # Parse service account JSON
            if service_account_json.endswith('.json'):
                # It's a file path
                with open(service_account_json, 'r') as f:
                    service_account_info = json.load(f)
            else:
                # It's JSON content
                service_account_info = json.loads(service_account_json)
            
            # Create credentials from service account
            self.credentials = service_account.Credentials.from_service_account_info(
                service_account_info,
                scopes=['https://www.googleapis.com/auth/analytics.readonly']
            )
            
            # Create client
            self.client = BetaAnalyticsDataClient(credentials=self.credentials)
            
            # Test connection
            test_request = RunReportRequest(
                property=f"properties/{self.property_id}",
                date_ranges=[DateRange(start_date="7daysAgo", end_date="today")],
                metrics=[Metric(name="sessions")],
                limit=1
            )
            
            response = self.client.run_report(test_request)
            
            return CallToolResult(
                content=[{
                    "type": "text", 
                    "text": f"Successfully authenticated with GA4 property {self.property_id}. Test query returned {len(response.rows)} rows. Service account: {service_account_info.get('client_email', 'Unknown')}"
                }]
            )
            
        except Exception as e:
            logger.error(f"Authentication error: {str(e)}")
            return CallToolResult(
                content=[{"type": "text", "text": f"Authentication failed: {str(e)}"}]
            )
    
    async def _test_connection(self, args: Dict[str, Any]) -> CallToolResult:
        """Test the connection to Google Analytics."""
        if not self.client or not self.property_id:
            return CallToolResult(
                content=[{"type": "text", "text": "Not authenticated. Please run authenticate_ga_service_account first."}]
            )
        
        try:
            # Simple test query
            test_request = RunReportRequest(
                property=f"properties/{self.property_id}",
                date_ranges=[DateRange(start_date="yesterday", end_date="today")],
                metrics=[Metric(name="sessions")],
                limit=1
            )
            
            response = self.client.run_report(test_request)
            
            return CallToolResult(
                content=[{
                    "type": "text",
                    "text": f"Connection test successful! Property ID: {self.property_id}, Sessions yesterday: {response.rows[0].metric_values[0].value if response.rows else 'No data'}"
                }]
            )
            
        except Exception as e:
            logger.error(f"Connection test error: {str(e)}")
            return CallToolResult(
                content=[{"type": "text", "text": f"Connection test failed: {str(e)}"}]
            )
    
    async def _get_analytics_data(self, args: Dict[str, Any]) -> CallToolResult:
        """Get Google Analytics data."""
        if not self.client or not self.property_id:
            return CallToolResult(
                content=[{"type": "text", "text": "Not authenticated. Please run authenticate_ga_service_account first."}]
            )
        
        try:
            dimensions = [Dimension(name=dim) for dim in args["dimensions"]]
            metrics = [Metric(name=metric) for metric in args["metrics"]]
            
            # Handle date range
            date_range = args.get("date_range", {})
            start_date = date_range.get("start_date", "7daysAgo")
            end_date = date_range.get("end_date", "today")
            
            request = RunReportRequest(
                property=f"properties/{self.property_id}",
                date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
                dimensions=dimensions,
                metrics=metrics,
                limit=args.get("limit", 1000)
            )
            
            # Add filters if provided
            if "filters" in args:
                filter_expressions = []
                for filter_str in args["filters"]:
                    # Parse filter string (e.g., "pagePath==/home")
                    if "==" in filter_str:
                        dimension, value = filter_str.split("==", 1)
                        filter_expressions.append(
                            FilterExpression(
                                filter=Filter(
                                    field_name=dimension,
                                    string_filter=Filter.StringFilter(value=value)
                                )
                            )
                        )
                if filter_expressions:
                    request.dimension_filter = FilterExpression(
                        and_group=FilterExpression.FilterExpressionList(
                            expressions=filter_expressions
                        )
                    )
            
            response = self.client.run_report(request)
            
            # Convert to JSON-serializable format
            results = []
            for row in response.rows:
                row_data = {}
                for i, dimension in enumerate(row.dimension_values):
                    row_data[f"dimension_{i}"] = dimension.value
                for i, metric in enumerate(row.metric_values):
                    row_data[f"metric_{i}"] = metric.value
                results.append(row_data)
            
            return CallToolResult(
                content=[{
                    "type": "text",
                    "text": json.dumps({
                        "data": results,
                        "total_rows": len(results),
                        "dimensions": args["dimensions"],
                        "metrics": args["metrics"],
                        "date_range": f"{start_date} to {end_date}"
                    }, indent=2)
                }]
            )
            
        except Exception as e:
            logger.error(f"Error getting analytics data: {str(e)}")
            return CallToolResult(
                content=[{"type": "text", "text": f"Error getting data: {str(e)}"}]
            )
    
    async def _get_available_dimensions(self, args: Dict[str, Any]) -> CallToolResult:
        """Get available dimensions."""
        common_dimensions = [
            "date", "dateHour", "dateHourMinute", "dateMinute",
            "pagePath", "pageTitle", "pageReferrer",
            "source", "medium", "campaign",
            "deviceCategory", "operatingSystem", "browser",
            "country", "region", "city",
            "userType", "sessionDefaultChannelGroup",
            "landingPage", "exitPage",
            "eventName", "customEvent:event_name"
        ]
        
        return CallToolResult(
            content=[{
                "type": "text",
                "text": json.dumps({"available_dimensions": common_dimensions})
            }]
        )
    
    async def _get_available_metrics(self, args: Dict[str, Any]) -> CallToolResult:
        """Get available metrics."""
        common_metrics = [
            "sessions", "totalUsers", "newUsers", "activeUsers",
            "screenPageViews", "eventCount", "eventValue",
            "bounceRate", "engagementRate", "averageSessionDuration",
            "transactions", "totalRevenue", "ecommercePurchases",
            "sessionsPerUser", "screenPageViewsPerSession"
        ]
        
        return CallToolResult(
            content=[{
                "type": "text", 
                "text": json.dumps({"available_metrics": common_metrics})
            }]
        )
    
    async def _get_property_info(self, args: Dict[str, Any]) -> CallToolResult:
        """Get property information."""
        if not self.property_id:
            return CallToolResult(
                content=[{"type": "text", "text": "No property ID set"}]
            )
        
        return CallToolResult(
            content=[{
                "type": "text",
                "text": json.dumps({
                    "property_id": self.property_id,
                    "authenticated": self.client is not None,
                    "service_account_email": self.credentials.service_account_email if self.credentials else None,
                    "available_tools": [
                        "get_analytics_data",
                        "get_available_dimensions", 
                        "get_available_metrics",
                        "test_connection"
                    ]
                })
            }]
        )

async def main():
    """Run the Google Analytics MCP server."""
    server = GoogleAnalyticsMCPServer()
    
    async with stdio_server() as (read_stream, write_stream):
        await server.server.run(
            read_stream,
            write_stream,
            InitializationOptions(
                server_name="google-analytics-mcp",
                server_version="1.0.0",
                capabilities=server.server.get_capabilities(
                    notification_options=None,
                    experimental_capabilities=None,
                ),
            ),
        )

if __name__ == "__main__":
    asyncio.run(main()) 