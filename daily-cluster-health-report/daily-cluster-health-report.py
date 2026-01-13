import os
import requests
import logging
import smtplib
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# Configuration
REPORT_DIR = "/home/cdpuser/scripts/daily-cluster-health-report/"
REPORT_FILE = os.path.join(REPORT_DIR, "cdp_health_report.html")
LOG_FILE = os.path.join(REPORT_DIR, "cdp_health_report.log")  # Log file

CM_HOST = "10.11.228.10"
CM_PORT = "7183"
CM_USER = "admin"
CM_PASS = "cdpuser@1234"

# Email Configuration
EXCHANGE_SERVER = "your.exchange.server.com"
SMTP_PORT = 25
SENDER_EMAIL = "your-email@example.com"
RECIPIENT_EMAILS = ["recipient1@example.com"]
EMAIL_SUBJECT = "CDP Cluster Health Report"

# Ensure report directory exists
os.makedirs(REPORT_DIR, exist_ok=True)

# Setup logging (Writes to both file & console)
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

console_handler = logging.StreamHandler()  # Add console output
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
console_handler.setFormatter(formatter)
logging.getLogger().addHandler(console_handler)  # Send logs to console

def log(message, level="info"):
    """Logs messages to both console and log file."""
    if level == "error":
        logging.error(message)
    else:
        logging.info(message)

def get_cluster_health():
    """Fetch cluster health from Cloudera Manager API."""
    url = f"https://{CM_HOST}:{CM_PORT}/api/v54/clusters"
    log(f"Fetching cluster health from {url}")
    try:
        response = requests.get(url, auth=(CM_USER, CM_PASS), verify=False)
        if response.status_code == 200:
            return response.json()["items"]
        else:
            log(f"❌ ERROR: Failed to fetch cluster health. HTTP {response.status_code}", "error")
            return []
    except Exception as e:
        log(f"❌ ERROR: Failed to connect to Cloudera Manager: {e}", "error")
        return []

def get_services_health(cluster_name):
    """Fetch services health from Cloudera Manager API."""
    url = f"https://{CM_HOST}:{CM_PORT}/api/v54/clusters/{cluster_name}/services"
    log(f"Fetching service health for cluster: {cluster_name}")
    try:
        response = requests.get(url, auth=(CM_USER, CM_PASS), verify=False)
        if response.status_code == 200:
            return response.json()["items"]
        else:
            log(f"❌ ERROR: Failed to fetch services health for {cluster_name}. HTTP {response.status_code}", "error")
            return []
    except Exception as e:
        log(f"❌ ERROR: Failed to connect to Cloudera Manager for services: {e}", "error")
        return []

def generate_html_report(services):
    """Generate an HTML report of CDP service health."""
    log("Generating HTML report")

    html = """
    <html>
    <head>
        <title>CDP Cluster Health Report</title>
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600&display=swap');

        :root {
            --primary-color: #003218;
            --secondary-color: #004d26;
            --accent-color: #006633;
            --background-color: #e6f0ea;
            --text-color: #2d3436;
        }

        body {
            font-family: 'Inter', sans-serif;
            margin: 2rem;
            background: var(--background-color);
            color: var(--text-color);
        }

        .report-container {
            background: white;
            border-radius: 12px;
            box-shadow: 0 4px 20px rgba(0,0,0,0.08);
            padding: 2rem;
            margin: 0 auto;
            max-width: 1200px;
            border: 1px solid #dde5e0;
        }

        h2 {
            color: var(--primary-color);
            border-bottom: 3px solid var(--secondary-color);
            padding-bottom: 0.75rem;
            margin-bottom: 1.75rem;
            font-size: 1.8rem;
            font-weight: 600;
            letter-spacing: -0.5px;
        }

        h3 {
            color: var(--secondary-color);
            margin: 2rem 0 1.25rem;
            font-size: 1.4rem;
            font-weight: 500;
        }

        table {
            width: 100%;
            border-collapse: collapse;
            margin: 1.5rem 0;
            background: white;
            border-radius: 8px;
            overflow: hidden;
            border: 1px solid #e0e8e3;
        }

        th, td {
            padding: 14px 18px;
            text-align: left;
            border-bottom: 1px solid #ecf0f1;
        }

        th {
            background-color: var(--primary-color);
            color: white;
            font-weight: 500;
            text-transform: uppercase;
            font-size: 0.85rem;
            letter-spacing: 0.5px;
        }

        tr:nth-child(even) {
            background-color: #f8faf9;
        }

        tr:hover {
            background-color: #f2f7f4;
        }

        .metric-highlight {
            font-weight: 500;
            color: var(--accent-color);
            font-size: 1.05rem;
        }

        .warning {
            background-color: #fff8e6 !important;
            color: #8a6d05;
            border-left: 4px solid #ffd54f;
        }

        .critical {
            background-color: #fdecea !important;
            color: #b71c1c;
            border-left: 4px solid #ef9a9a;
        }

        .timestamp {
            color: #5a6860;
            font-size: 0.9rem;
            margin-bottom: 1.75rem;
            display: block;
            font-weight: 400;
        }

        .footer-note {
            text-align: center;
            margin-top: 2.5rem;
            color: #5a6860;
            font-size: 0.85rem;
            padding-top: 1rem;
            border-top: 1px solid #e0e8e3;
        }

        .stat-badge {
            background: var(--background-color);
            color: var(--secondary-color);
            padding: 4px 10px;
            border-radius: 4px;
            font-weight: 500;
            font-size: 0.9rem;
        }
    </style>
    </head>
    <body>
        <div class="report-container">
            <h2>CDP Cluster Health Report</h2>
            <span class="timestamp">Generated: f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"</span>
            <table>
                <tr>
                    <th>Service</th>
                    <th>Status</th>
                    <th>Health</th>
                    <th>Time</th>
                </tr>
    """
    for service in services:
        name = service["name"]
        health = service["healthSummary"]
        state = service["serviceState"]
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        css_class = "healthy" if health == "GOOD" else "warning" if health == "CONCERNING" else "critical"
        html += f"""
                <tr class='{css_class}'>
                    <td>{name}</td>
                    <td>{state}</td>
                    <td>{health}</td>
                    <td>{timestamp}</td>
                </tr>
        """
    html += "</table><div class=footer-note>{datetime.datetime.now().strftime('%Y')} • HDFS Storage Monitoring System</div></div></body></html>"
    return html

def save_html_report(html_content):
    """Save HTML report to a file."""
    with open(REPORT_FILE, "w") as file:
        file.write(html_content)
    log(f"✅ HTML report saved at: {REPORT_FILE}")

def send_email():
    """Send the generated HTML report via email."""
    try:
        log(f"📧 Sending email to: {', '.join(RECIPIENT_EMAILS)}")

        msg = MIMEMultipart()
        msg["From"] = SENDER_EMAIL
        msg["To"] = ", ".join(RECIPIENT_EMAILS)
        msg["Subject"] = EMAIL_SUBJECT
        msg.attach(MIMEText(open(REPORT_FILE, "r").read(), "html"))

        server = smtplib.SMTP(EXCHANGE_SERVER, SMTP_PORT)
        server.ehlo()
        server.sendmail(SENDER_EMAIL, RECIPIENT_EMAILS, msg.as_string())
        server.quit()

        log("✅ Email sent successfully")
    except Exception as e:
        log(f"❌ ERROR: Failed to send email. {str(e)}", "error")

# Main Execution
if __name__ == "__main__":
    log("🚀 Starting CDP Cluster Health Report Generation")

    clusters = get_cluster_health()
    if not clusters:
        log("❌ No clusters found. Exiting...", "error")
        exit(1)

    for cluster in clusters:
        cluster_name = cluster["name"]
        services = get_services_health(cluster_name)

        if services:
            html_report = generate_html_report(services)
            save_html_report(html_report)
            send_email()
        else:
            log(f"❌ No services found for cluster {cluster_name}", "error")

    log("✅ CDP Health Report process completed")

