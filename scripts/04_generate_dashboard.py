#!/usr/bin/env python3
"""
Monitoring Dashboard Generator

Generates an HTML dashboard with pipeline health metrics and statistics.

Usage:
    python scripts/04_generate_dashboard.py
    python scripts/04_generate_dashboard.py --output /path/to/dashboard.html
"""

import argparse
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.storage.database import Database
from src.utils.config import get_absolute_path, get_settings, load_config
from src.utils.logger import get_logger, setup_logging

logger = get_logger("finloom.dashboard")


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Generate monitoring dashboard")
    parser.add_argument(
        "--output",
        type=str,
        default="dashboard.html",
        help="Output HTML file path",
    )
    return parser.parse_args()


def get_pipeline_stats(db: Database) -> dict:
    """Gather pipeline statistics from database."""
    stats = {
        "generated_at": datetime.now().isoformat(),
        "companies": {},
        "filings": {},
        "facts": {},
        "sections": {},
        "processing": {},
        "quality": {},
    }
    
    # Company stats
    companies_df = db.execute_query("SELECT COUNT(*) as count FROM companies")
    stats["companies"]["total"] = int(companies_df.iloc[0]["count"])
    
    # Filing stats
    filings_df = db.execute_query("""
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN download_status = 'completed' THEN 1 ELSE 0 END) as downloaded,
            SUM(CASE WHEN download_status = 'failed' THEN 1 ELSE 0 END) as failed,
            SUM(CASE WHEN xbrl_processed THEN 1 ELSE 0 END) as xbrl_processed,
            SUM(CASE WHEN sections_processed THEN 1 ELSE 0 END) as sections_processed,
            MIN(filing_date) as earliest_date,
            MAX(filing_date) as latest_date
        FROM filings
    """)
    if not filings_df.empty:
        row = filings_df.iloc[0]
        stats["filings"] = {
            "total": int(row["total"]) if row["total"] else 0,
            "downloaded": int(row["downloaded"]) if row["downloaded"] else 0,
            "failed": int(row["failed"]) if row["failed"] else 0,
            "xbrl_processed": int(row["xbrl_processed"]) if row["xbrl_processed"] else 0,
            "sections_processed": int(row["sections_processed"]) if row["sections_processed"] else 0,
            "earliest_date": str(row["earliest_date"]) if row["earliest_date"] else None,
            "latest_date": str(row["latest_date"]) if row["latest_date"] else None,
        }
    
    # Facts stats
    facts_df = db.execute_query("""
        SELECT 
            COUNT(*) as total,
            COUNT(DISTINCT concept_name) as unique_concepts,
            COUNT(DISTINCT accession_number) as filings_with_facts
        FROM facts
    """)
    if not facts_df.empty:
        row = facts_df.iloc[0]
        stats["facts"] = {
            "total": int(row["total"]) if row["total"] else 0,
            "unique_concepts": int(row["unique_concepts"]) if row["unique_concepts"] else 0,
            "filings_with_facts": int(row["filings_with_facts"]) if row["filings_with_facts"] else 0,
        }
    
    # Sections stats
    sections_df = db.execute_query("""
        SELECT 
            COUNT(*) as total,
            SUM(word_count) as total_words,
            AVG(word_count) as avg_words,
            COUNT(DISTINCT section_type) as section_types
        FROM sections
    """)
    if not sections_df.empty:
        row = sections_df.iloc[0]
        stats["sections"] = {
            "total": int(row["total"]) if row["total"] else 0,
            "total_words": int(row["total_words"]) if row["total_words"] else 0,
            "avg_words": int(row["avg_words"]) if row["avg_words"] else 0,
            "section_types": int(row["section_types"]) if row["section_types"] else 0,
        }
    
    # Processing logs (last 7 days)
    week_ago = (datetime.now() - timedelta(days=7)).isoformat()
    logs_df = db.execute_query(f"""
        SELECT 
            pipeline_stage,
            status,
            COUNT(*) as count
        FROM processing_logs
        WHERE created_at >= '{week_ago}'
        GROUP BY pipeline_stage, status
    """)
    processing = {}
    for _, row in logs_df.iterrows():
        stage = row["pipeline_stage"]
        if stage not in processing:
            processing[stage] = {}
        processing[stage][row["status"]] = int(row["count"])
    stats["processing"] = processing
    
    # Per-company breakdown
    company_df = db.execute_query("""
        SELECT 
            c.ticker,
            c.company_name,
            COUNT(f.accession_number) as total_filings,
            SUM(CASE WHEN f.download_status = 'completed' THEN 1 ELSE 0 END) as downloaded,
            SUM(CASE WHEN f.xbrl_processed THEN 1 ELSE 0 END) as xbrl_done,
            SUM(CASE WHEN f.sections_processed THEN 1 ELSE 0 END) as sections_done
        FROM companies c
        LEFT JOIN filings f ON c.cik = f.cik
        GROUP BY c.ticker, c.company_name
        ORDER BY c.ticker
    """)
    stats["companies"]["breakdown"] = company_df.to_dict("records")
    
    return stats


def generate_html(stats: dict) -> str:
    """Generate HTML dashboard from stats."""
    companies = stats.get("companies", {})
    filings = stats.get("filings", {})
    facts = stats.get("facts", {})
    sections = stats.get("sections", {})
    processing = stats.get("processing", {})
    
    # Calculate percentages
    total_filings = filings.get("total", 0) or 1
    downloaded_pct = (filings.get("downloaded", 0) / total_filings) * 100
    xbrl_pct = (filings.get("xbrl_processed", 0) / total_filings) * 100
    sections_pct = (filings.get("sections_processed", 0) / total_filings) * 100
    
    # Company rows
    company_rows = ""
    for c in companies.get("breakdown", []):
        company_rows += f"""
        <tr>
            <td>{c['ticker']}</td>
            <td>{c['company_name']}</td>
            <td>{c['total_filings']}</td>
            <td>{c['downloaded']}</td>
            <td>{c['xbrl_done']}</td>
            <td>{c['sections_done']}</td>
        </tr>
        """
    
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>FinLoom SEC Pipeline Dashboard</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #f5f7fa;
            color: #333;
            line-height: 1.6;
        }}
        .container {{
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
        }}
        header {{
            background: linear-gradient(135deg, #1a237e, #3949ab);
            color: white;
            padding: 30px;
            border-radius: 10px;
            margin-bottom: 30px;
        }}
        header h1 {{ font-size: 2em; margin-bottom: 10px; }}
        header p {{ opacity: 0.9; }}
        .cards {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }}
        .card {{
            background: white;
            border-radius: 10px;
            padding: 25px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.08);
        }}
        .card h3 {{
            color: #666;
            font-size: 0.9em;
            text-transform: uppercase;
            margin-bottom: 10px;
        }}
        .card .value {{
            font-size: 2.5em;
            font-weight: 700;
            color: #1a237e;
        }}
        .card .sub {{ color: #888; font-size: 0.9em; margin-top: 5px; }}
        .progress-section {{
            background: white;
            border-radius: 10px;
            padding: 25px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.08);
            margin-bottom: 30px;
        }}
        .progress-section h2 {{ margin-bottom: 20px; color: #333; }}
        .progress-bar {{
            background: #e0e0e0;
            border-radius: 10px;
            height: 20px;
            margin-bottom: 15px;
            overflow: hidden;
        }}
        .progress-fill {{
            height: 100%;
            border-radius: 10px;
            transition: width 0.5s ease;
        }}
        .progress-fill.green {{ background: linear-gradient(90deg, #4caf50, #8bc34a); }}
        .progress-fill.blue {{ background: linear-gradient(90deg, #2196f3, #03a9f4); }}
        .progress-fill.purple {{ background: linear-gradient(90deg, #9c27b0, #e91e63); }}
        .progress-label {{
            display: flex;
            justify-content: space-between;
            font-size: 0.9em;
            color: #666;
            margin-bottom: 5px;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            background: white;
            border-radius: 10px;
            overflow: hidden;
            box-shadow: 0 2px 10px rgba(0,0,0,0.08);
        }}
        th, td {{
            padding: 15px;
            text-align: left;
            border-bottom: 1px solid #eee;
        }}
        th {{
            background: #f8f9fa;
            font-weight: 600;
            color: #555;
            text-transform: uppercase;
            font-size: 0.85em;
        }}
        tr:hover {{ background: #f8f9fa; }}
        .footer {{
            text-align: center;
            padding: 30px;
            color: #888;
            font-size: 0.9em;
        }}
        .status-ok {{ color: #4caf50; }}
        .status-warn {{ color: #ff9800; }}
        .status-error {{ color: #f44336; }}
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>FinLoom SEC Pipeline Dashboard</h1>
            <p>Last updated: {stats['generated_at']}</p>
        </header>
        
        <div class="cards">
            <div class="card">
                <h3>Companies Tracked</h3>
                <div class="value">{companies.get('total', 0)}</div>
                <div class="sub">Target companies</div>
            </div>
            <div class="card">
                <h3>Total Filings</h3>
                <div class="value">{filings.get('total', 0)}</div>
                <div class="sub">{filings.get('earliest_date', 'N/A')} to {filings.get('latest_date', 'N/A')}</div>
            </div>
            <div class="card">
                <h3>Facts Extracted</h3>
                <div class="value">{facts.get('total', 0):,}</div>
                <div class="sub">{facts.get('unique_concepts', 0)} unique concepts</div>
            </div>
            <div class="card">
                <h3>Sections Extracted</h3>
                <div class="value">{sections.get('total', 0):,}</div>
                <div class="sub">{sections.get('total_words', 0):,} total words</div>
            </div>
        </div>
        
        <div class="progress-section">
            <h2>Processing Progress</h2>
            
            <div class="progress-label">
                <span>Downloaded</span>
                <span>{filings.get('downloaded', 0)} / {filings.get('total', 0)} ({downloaded_pct:.1f}%)</span>
            </div>
            <div class="progress-bar">
                <div class="progress-fill green" style="width: {downloaded_pct}%"></div>
            </div>
            
            <div class="progress-label">
                <span>XBRL Processed</span>
                <span>{filings.get('xbrl_processed', 0)} / {filings.get('total', 0)} ({xbrl_pct:.1f}%)</span>
            </div>
            <div class="progress-bar">
                <div class="progress-fill blue" style="width: {xbrl_pct}%"></div>
            </div>
            
            <div class="progress-label">
                <span>Sections Extracted</span>
                <span>{filings.get('sections_processed', 0)} / {filings.get('total', 0)} ({sections_pct:.1f}%)</span>
            </div>
            <div class="progress-bar">
                <div class="progress-fill purple" style="width: {sections_pct}%"></div>
            </div>
        </div>
        
        <h2 style="margin-bottom: 20px;">Company Breakdown</h2>
        <table>
            <thead>
                <tr>
                    <th>Ticker</th>
                    <th>Company</th>
                    <th>Total Filings</th>
                    <th>Downloaded</th>
                    <th>XBRL Done</th>
                    <th>Sections Done</th>
                </tr>
            </thead>
            <tbody>
                {company_rows}
            </tbody>
        </table>
        
        <div class="footer">
            <p>FinLoom SEC Data Pipeline - Dashboard generated automatically</p>
        </div>
    </div>
</body>
</html>
"""
    return html


def main():
    """Main entry point."""
    args = parse_args()
    
    # Setup
    setup_logging()
    load_config()
    
    logger.info("Generating monitoring dashboard...")
    
    with Database() as db:
        # Gather stats
        stats = get_pipeline_stats(db)
        
        # Generate HTML
        html = generate_html(stats)
        
        # Write output
        output_path = get_absolute_path(args.output)
        with open(output_path, "w") as f:
            f.write(html)
        
        logger.info(f"Dashboard generated: {output_path}")
    
    # Also output JSON stats
    json_path = output_path.with_suffix(".json")
    with open(json_path, "w") as f:
        json.dump(stats, f, indent=2, default=str)
    logger.info(f"Stats JSON: {json_path}")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
