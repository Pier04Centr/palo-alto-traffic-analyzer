import pandas as pd
import xlsxwriter
import argparse
import sys
import os
from pathlib import Path
from typing import Optional

# ================= CONFIGURAZIONE =================
COLOR_HEADER = '#1F4E78'
COLOR_TEXT_HEADER = '#FFFFFF'
COLOR_BAR_TOTAL = '#5B9BD5'

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.str.strip()
    renames = {
        'Session ID': 'Session ID', 'Source User': 'User',
        'Bytes Sent': 'Bytes Sent', 'Bytes Received': 'Bytes Received',
        'URL/Filename': 'URL', 'Application': 'Application',
        'Destination address': 'Destination address',
        'Category': 'Category'
    }
    df.rename(columns={k: v for k, v in renames.items() if k in df.columns}, inplace=True)
    return df

def extract_root_domain(url: str) -> str:
    if pd.isna(url) or url == '': return "N/A"
    try:
        url_str = str(url).lower().split('/')[0]
        parts = url_str.split('.')
        if len(parts) >= 2: return f"{parts[-2]}.{parts[-1]}"
        return url_str
    except: return str(url)

def get_isp_info(ip: str, reader) -> str:
    try:
        response = reader.asn(str(ip).strip())
        return response.autonomous_system_organization
    except: return "Unknown/Private"

def load_and_process(file_traffic: Path, file_url: Path, db_path: Optional[Path]) -> Optional[pd.DataFrame]:
    print("--- 1. ETL Process Started ---")
    
    try:
        df_t = pd.read_csv(file_traffic, sep=None, engine='python')
        df_t = clean_dataframe(df_t)
    except Exception as e:
        print(f"CRITICAL ERROR (Traffic Log): {e}"); return None

    try:
        if file_url and file_url.exists():
            df_u = pd.read_csv(file_url, sep=None, engine='python')
            df_u = clean_dataframe(df_u)
            if 'Session ID' in df_u.columns:
                df_u['Session ID'] = df_u['Session ID'].astype(str)
                df_u = df_u.drop_duplicates(subset=['Session ID'], keep='first')
                df_u['Dominio'] = df_u['URL'].apply(extract_root_domain) if 'URL' in df_u.columns else 'N/A'
                if 'Category' in df_u.columns: df_u.rename(columns={'Category': 'Url_Category'}, inplace=True)
                df_u = df_u[[c for c in ['Session ID', 'Dominio', 'Url_Category'] if c in df_u.columns]]
        else:
            df_u = pd.DataFrame()
    except: df_u = pd.DataFrame()

    if 'Session ID' in df_t.columns: df_t['Session ID'] = df_t['Session ID'].astype(str)
    
    print("-> Merging Datasets...")
    df_final = pd.merge(df_t, df_u, on='Session ID', how='left')

    if db_path and db_path.exists() and 'Destination address' in df_final.columns:
        try:
            import geoip2.database
            print(f"-> Enriching with MaxMind DB: {db_path.name}")
            with geoip2.database.Reader(str(db_path)) as reader:
                unique_ips = df_final['Destination address'].unique()
                ip_map = {ip: get_isp_info(ip, reader) for ip in unique_ips}
                df_final['Organization'] = df_final['Destination address'].map(ip_map)
        except ImportError: pass
    else: df_final['Organization'] = None

    # Logic Fallback
    if 'Url_Category' in df_final.columns:
        df_final['Category'] = df_final['Url_Category'].fillna(df_final.get('Category', 'Uncategorized'))
    df_final['Category'] = df_final['Category'].fillna('Uncategorized')

    if 'Dominio' not in df_final.columns: df_final['Dominio'] = None
    mask_no_dom = df_final['Dominio'].isna()
    if 'Organization' in df_final.columns:
        df_final.loc[mask_no_dom, 'Dominio'] = df_final.loc[mask_no_dom, 'Organization']
    
    if 'Destination address' in df_final.columns:
        df_final['Dominio'] = df_final['Dominio'].fillna('IP: ' + df_final['Destination address'].astype(str))
    
    df_final['User'] = df_final['User'].fillna('Unknown').astype(str).apply(lambda x: x.split('\\')[-1])
    
    for col in ['Bytes', 'Bytes Sent', 'Bytes Received']:
        if col not in df_final.columns: df_final[col] = 0
        
    df_final['Total MB'] = df_final['Bytes'] / 1048576
    df_final['Sent MB'] = df_final['Bytes Sent'] / 1048576
    df_final['Received MB'] = df_final['Bytes Received'] / 1048576

    return df_final

def genera_excel(df: pd.DataFrame, output_path: Path):
    print(f"--- 2. Generating Report: {output_path} ---")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    writer = pd.ExcelWriter(output_path, engine='xlsxwriter')
    workbook = writer.book
    
    fmt_head = workbook.add_format({'bold': True, 'bg_color': COLOR_HEADER, 'font_color': COLOR_TEXT_HEADER, 'border': 1})
    fmt_num = workbook.add_format({'num_format': '#,##0.00 "MB"'})

    def write_sheet(d, name):
        d.to_excel(writer, sheet_name=name, index=False, startrow=1)
        ws = writer.sheets[name]
        for i, col in enumerate(d.columns):
            ws.write(1, i, col, fmt_head)
            ws.set_column(i, i, 15 if "MB" in col else 25, fmt_num if "MB" in col else None)

    df_dom = df.groupby(['Dominio', 'Category'])[['Total MB', 'Sent MB', 'Received MB']].sum().reset_index().sort_values('Total MB', ascending=False)
    df_usr = df.groupby('User')[['Total MB', 'Sent MB', 'Received MB']].sum().reset_index().sort_values('Total MB', ascending=False)
    
    write_sheet(df_dom, 'Domain_Analysis')
    write_sheet(df_usr, 'User_Ranking')
    
    writer.close()
    print("--- Done ---")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-t', '--traffic', required=True, type=Path)
    parser.add_argument('-u', '--url', required=False, type=Path)
    parser.add_argument('-o', '--output', required=False, default=Path('./report.xlsx'), type=Path)
    parser.add_argument('--db', required=False, type=Path)
    args = parser.parse_args()
    
    if df := load_and_process(args.traffic, args.url, args.db):
        genera_excel(df, args.output)