# Palo Alto SSL Traffic Analyzer

## 📋 Overview
A Python-based ETL tool designed to analyze SSL/Encrypted traffic from Palo Alto Networks firewalls **without decryption**.

It solves the "Blind Visibility" problem by deterministically correlating **Traffic Logs** (Bytes) with **URL Filtering Logs** (SNI/Domains) and enriching destination IPs with ASN data (via MaxMind).

## 🚀 Key Features
* **Log Correlation**: Merges Traffic and URL logs on `Session ID`.
* **Blind Spot Resolution**: Uses MaxMind GeoIP2 to identify Organizations (e.g., Microsoft, AWS) when SNI is missing.
* **Automated Reporting**: Generates an Excel Dashboard with User, Domain, and App usage metrics.
* **Privacy First**: Runs locally, no data is sent to the cloud.

## 🛠 Installation

1. Clone the repository:
```bash
git clone https://github.com/Pier04Centr/palo-alto-traffic-analyzer.git
```

2. Install dependencies:
```bash
pip install -r requirements.txt

```


3. (Optional) Download `GeoLite2-ASN.mmdb` from MaxMind and place it in the folder.

## 💻 Usage

Run the script from the command line:

```bash
python main.py -t ./logs/traffic.csv -u ./logs/url.csv -o ./report.xlsx

```

### Arguments:

* `-t, --traffic`: Path to the Traffic Log CSV (Required).
* `-u, --url`: Path to the URL Log CSV (Optional).
* `--db`: Path to MaxMind ASN database (Optional).

## 📊 Logic Flow

1. **Ingest**: Reads raw CSV exports from Palo Alto Monitor tab.
2. **Clean**: Normalizes column names and parses Units.
3. **Merge**: Left Join on `Session ID`.
4. **Fallback**: `Domain` -> `Organization` -> `IP Address`.

## 📝 License


MIT License
