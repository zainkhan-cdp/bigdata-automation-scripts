import requests
import json
import logging
import datetime
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# Cloudera Manager API Configuration
CM_HOST = "IP_ADDRESS_CM_HOST"
CM_PORT = "7183"
CM_USER = "admin"
CM_PASS = "cdpuser@1234"
CLUSTER_NAME = "MY-CLUSTER"

# Report Configuration
REPORT_DIR = "/home/cdpuser/scripts/daily-cdp-metrics-report/"
REPORT_FILE = os.path.join(REPORT_DIR, "cdp_service_metrics.html")
LOG_FILE = os.path.join(REPORT_DIR, "cdp_service_metrics.log")

# Email Configuration
EXCHANGE_SERVER = "your.exchange.server.com"
SMTP_PORT = 25
SENDER_EMAIL = "your-email@example.com"
RECIPIENT_EMAILS = ["recipient1@example.com"]
EMAIL_SUBJECT = "CDP Service Metrics Utilization Report"

# Ensure report directory exists
os.makedirs(REPORT_DIR, exist_ok=True)

# Setup Logging
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
console_handler.setFormatter(formatter)
logging.getLogger().addHandler(console_handler)

def log(message, level="info"):
    """Logs messages to both console and log file."""
    if level == "error":
        logging.error(message)
    else:
        logging.info(message)

def convert_bytes_to_gb_tb(bytes_value):
    """Convert bytes to GB/TB for better readability."""
    if bytes_value >= 1024 ** 4:
        return f"{bytes_value / (1024 ** 4):.2f} TB"
    else:
        return f"{bytes_value / (1024 ** 3):.2f} GB"

def fetch_service_metrics(service_name, metric_query):
    """Fetch real-time metrics using Cloudera's /timeseries API."""
    url = f"https://{CM_HOST}:{CM_PORT}/api/v54/timeseries?query=SELECT+{metric_query}+WHERE+serviceName={service_name}"

    logging.info(f"📊 Fetching metrics for service: {service_name}")

    try:
        response = requests.get(url, auth=(CM_USER, CM_PASS), verify=False)
        response.raise_for_status()
        return response.json().get("items", [])
    except requests.exceptions.RequestException as e:
        logging.error(f"❌ Error fetching metrics for {service_name}: {e}")
        return []

def parse_metrics(metrics_data):
    """Extract relevant metric values from JSON response."""
    parsed_data = []

    for metric in metrics_data:
        metadata = metric.get("timeSeries", [])
        for ts in metadata:
            metric_name = ts.get("metadata", {}).get("metricName", "Unknown Metric")
            entity_name = ts.get("metadata", {}).get("attributes", {}).get("entityName", "Unknown Entity")
            values = [point["value"] for point in ts.get("data", [])]

            if values:
                latest_value = values[-1]  # Get the latest metric value
                if "bytes" in ts.get("metadata", {}).get("unitNumerators", []):  # Convert bytes to GB/TB
                    latest_value = convert_bytes_to_gb_tb(latest_value)

                parsed_data.append({
                    "metric_name": metric_name,
                    "entity_name": entity_name,
                    "latest_value": latest_value
                })

    return parsed_data

def generate_html_report(service_metrics):
    """Generate an HTML report for CDP Service Metrics."""
    log("📄 Generating HTML report...")

    html_content = f"""<!DOCTYPE html>
<html>
<head>
    <title>CDP Service Metrics Utilization Report</title>
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600&display=swap');

        :root {{
            --primary-color: #003218;
            --secondary-color: #004d26;
            --accent-color: #006633;
            --background-color: #e6f0ea;
            --text-color: #2d3436;
        }}

        body {{
            font-family: 'Inter', sans-serif;
            margin: 2rem;
            background: var(--background-color);
            color: var(--text-color);
        }}

        .report-container {{
            background: white;
            border-radius: 12px;
            box-shadow: 0 4px 20px rgba(0,0,0,0.08);
            padding: 2rem;
            margin: 0 auto;
            max-width: 1200px;
            border: 1px solid #dde5e0;
        }}

        h2 {{
            color: var(--primary-color);
            border-bottom: 3px solid var(--secondary-color);
            padding-bottom: 0.75rem;
            margin-bottom: 1.75rem;
            font-size: 1.8rem;
            font-weight: 600;
            letter-spacing: -0.5px;
        }}

        h3 {{
            color: var(--secondary-color);
            margin: 2rem 0 1.25rem;
            font-size: 1.4rem;
            font-weight: 500;
        }}

        table {{
            width: 100%;
            border-collapse: collapse;
            margin: 1.5rem 0;
            background: white;
            border-radius: 8px;
            overflow: hidden;
            border: 1px solid #e0e8e3;
        }}

        th, td {{
            padding: 14px 18px;
            text-align: left;
            border-bottom: 1px solid #ecf0f1;
        }}

        th {{
            background-color: var(--primary-color);
            color: white;
            font-weight: 500;
            text-transform: uppercase;
            font-size: 0.85rem;
            letter-spacing: 0.5px;
        }}

        tr:nth-child(even) {{
            background-color: #f8faf9;
        }}

        tr:hover {{
            background-color: #f2f7f4;
        }}

        .metric-highlight {{
            font-weight: 500;
            color: var(--accent-color);
            font-size: 1.05rem;
        }}

        .warning {{
            background-color: #fff8e6 !important;
            color: #8a6d05;
            border-left: 4px solid #ffd54f;
        }}

        .critical {{
            background-color: #fdecea !important;
            color: #b71c1c;
            border-left: 4px solid #ef9a9a;
        }}

        .timestamp {{
            color: #5a6860;
            font-size: 0.9rem;
            margin-bottom: 1.75rem;
            display: block;
            font-weight: 400;
        }}

        .footer-note {{
            text-align: center;
            margin-top: 2.5rem;
            color: #5a6860;
            font-size: 0.85rem;
            padding-top: 1rem;
            border-top: 1px solid #e0e8e3;
        }}

        .stat-badge {{
            background: var(--background-color);
            color: var(--secondary-color);
            padding: 4px 10px;
            border-radius: 4px;
            font-weight: 500;
            font-size: 0.9rem;
        }}
    </style>
</head>
<body>
    <div class="report-container">
        <h2>CDP Service Metrics Utilization Report</h2>
        <p><strong>Generated on:</strong> {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        <table>
            <tr>
                <th>Service</th>
                <th>Entity</th>
                <th>Metric</th>
                <th>Value</th>
            </tr>"""

    for service, metrics in service_metrics.items():
        for metric in metrics:
            html_content += f"""
            <tr>
                <td>{service}</td>
                <td>{metric['entity_name']}</td>
                <td>{metric['metric_name']}</td>
                <td>{metric['latest_value']}</td>
            </tr>"""

    html_content += f"""
        </table>
        <div class="footer-note">
            {datetime.datetime.now().strftime('%Y')} • HDFS Storage Monitoring System
        </div>
    </div>
</body>
</html>
"""

    with open(REPORT_FILE, "w") as file:
        file.write(html_content)

    log(f"✅ HTML report saved at: {REPORT_FILE}")

if __name__ == "__main__":
    log("🚀 Fetching CDP Service Metrics using /timeseries API")

    service_metrics = {}

    # Define queries for each service
    services = {
        "hdfs": "dfs_capacity_used, dfs_capacity_free",
        "yarn": "allocated_memory_mb, allocated_vcores",
        "hive": "hive_active_queries, hive_failed_queries",
        "impala": "impala_query_duration, impala_num_queries",
        "zookeeper": "zookeeper_approximate_data_size",
        "spark": "spark_executor_memory_used, spark_jobs_running"
    }

    for service, query in services.items():
        metrics_data = fetch_service_metrics(service, query)
        parsed_metrics = parse_metrics(metrics_data)
        if parsed_metrics:
            service_metrics[service] = parsed_metrics

    if service_metrics:
        generate_html_report(service_metrics)
    else:
        log("❌ No service metrics found. Exiting...", "error")

    log("✅ CDP Service Metrics Report process completed")

