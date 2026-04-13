# API Data Scraping and Google Sheets Integration
import requests
import json
import gspread
from google.oauth2.service_account import Credentials
import os
from datetime import datetime
import math
import logging
import sys
import time

# ==================== CONFIGURATION ====================
# Google Sheets Configuration
SPREADSHEET_ID = "1p7Ft5JxI1wA6Qvqc6OieMeT5sdZyuRygkyfmdlbzXn4"
CREDENTIALS_FILE = "credentials.json"

# API Configuration
API_URL = "https://registries.health.gov.il/api/Cosmetics/GetCosmetics"
MAX_RESULT_PER_PAGE = 100  # Number of records per page when fetching data
MAX_RETRIES = 3  # Number of retries for failed API calls
RETRY_DELAY = 2  # Seconds to wait between retries

# Google Sheets API Configuration
SHEETS_BATCH_SIZE = 1000  # Reduced from 5000 to avoid 502 errors
SHEETS_BATCH_DELAY = 1  # Seconds to wait between batches
SHEETS_MAX_RETRIES = 5  # Number of retries for Google Sheets API calls
SHEETS_RETRY_DELAY = 5  # Initial delay for retries (exponential backoff)

# Logging Configuration
ENABLE_LOGGING = True

# ==================== LOGGING SETUP ====================
if ENABLE_LOGGING:
    # Create logs directory if not exists
    logs_dir = "logs"
    if not os.path.exists(logs_dir):
        os.makedirs(logs_dir)
    
    # Create log filename by date: automation-DD-MM-YYYY.log
    today = datetime.now()
    log_filename = f"automation-{today.strftime('%d-%m-%Y')}.log"
    log_file_path = os.path.join(logs_dir, log_filename)
    
    # Log WARNING, ERROR, and INFO to file, not print to terminal
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)  # Log INFO, WARNING and ERROR
    
    # File handler: write all INFO, WARNING and ERROR to file
    file_handler = logging.FileHandler(log_file_path, encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(file_handler)
else:
    logger = logging.getLogger(__name__)
    logger.addHandler(logging.NullHandler())

# ==================== API FUNCTIONS ====================

def get_api_data_sheet1(max_result=100, page_number=1, retry_count=0):
    # Get data for Sheet 1 (filtered columns) - simple API call without businessNotificationItemId and businessTypeNotificationId
    payload = {
        "isDescending": False,
        "maxResult": max_result,
        "pageNumber": page_number
    }
    
    headers = {"Content-Type": "application/json"}
    
    try:
        response = requests.post(API_URL, json=payload, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        if 'returnObject' in data and 'cosmeticsList' in data['returnObject']:
            result = {
                'data': data['returnObject']['cosmeticsList'],
                'totalRows': data['returnObject'].get('totalRows', 0),
                'maxResults': data['returnObject'].get('maxResults', max_result)
            }
            # Log notification codes for debugging
            notification_codes = [item.get('notificationCode', '') for item in result['data']]
            logger.info(f"Sheet 1 - Page {page_number}: Fetched {len(result['data'])} records. Notification codes: {notification_codes[:10]}...")  # Log first 10
            return result
        return {'data': [], 'totalRows': 0, 'maxResults': max_result}
    except Exception as e:
        if retry_count < MAX_RETRIES:
            logger.warning(f"Error fetching Sheet 1 data (page {page_number}), retry {retry_count + 1}/{MAX_RETRIES}: {e}")
            time.sleep(RETRY_DELAY)
            return get_api_data_sheet1(max_result, page_number, retry_count + 1)
        else:
            logger.error(f"Error fetching Sheet 1 data (page {page_number}) after {MAX_RETRIES} retries: {e}")
            print(f"❌ Error fetching Sheet 1 data (page {page_number}): {e}")
            return {'data': [], 'totalRows': 0, 'maxResults': max_result}

def get_api_data_by_notification_code(notification_code, use_filter=True):
    """
    Query API directly by notificationCode (bypasses pagination)
    Returns the record if found, None otherwise
    """
    payload = {
        "isDescending": False,
        "maxResult": 100,
        "pageNumber": 0,  # API uses 0-based indexing when querying by code
        "notificationCode": notification_code
    }
    
    if use_filter:
        payload["businessNotificationItemId"] = 34
        payload["businessTypeNotificationId"] = 5
    
    headers = {"Content-Type": "application/json"}
    
    try:
        response = requests.post(API_URL, json=payload, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        if 'returnObject' in data and 'cosmeticsList' in data['returnObject']:
            records = data['returnObject']['cosmeticsList']
            if records and len(records) > 0:
                return records[0]  # Return first record
        return None
    except Exception as e:
        logger.error(f"Error querying notification code {notification_code}: {e}")
        return None

def get_api_data_sheet2(max_result=100, page_number=1, retry_count=0):
    # Get data for Sheet 2 (all columns) - API call with businessNotificationItemId: 34 and businessTypeNotificationId: 5
    payload = {
        "isDescending": False,
        "maxResult": max_result,
        "pageNumber": page_number,
        "businessNotificationItemId": 34,
        "businessTypeNotificationId": 5
    }
    
    headers = {"Content-Type": "application/json"}
    
    try:
        response = requests.post(API_URL, json=payload, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        if 'returnObject' in data and 'cosmeticsList' in data['returnObject']:
            result = {
                'data': data['returnObject']['cosmeticsList'],
                'totalRows': data['returnObject'].get('totalRows', 0),
                'maxResults': data['returnObject'].get('maxResults', max_result)
            }
            # Log notification codes for debugging
            notification_codes = [item.get('notificationCode', '') for item in result['data']]
            logger.info(f"Sheet 2 - Page {page_number}: Fetched {len(result['data'])} records. Notification codes: {notification_codes[:10]}...")  # Log first 10
            return result
        return {'data': [], 'totalRows': 0, 'maxResults': max_result}
    except Exception as e:
        if retry_count < MAX_RETRIES:
            logger.warning(f"Error fetching Sheet 2 data (page {page_number}), retry {retry_count + 1}/{MAX_RETRIES}: {e}")
            time.sleep(RETRY_DELAY)
            return get_api_data_sheet2(max_result, page_number, retry_count + 1)
        else:
            logger.error(f"Error fetching Sheet 2 data (page {page_number}) after {MAX_RETRIES} retries: {e}")
            print(f"❌ Error fetching Sheet 2 data (page {page_number}): {e}")
            return {'data': [], 'totalRows': 0, 'maxResults': max_result}

def get_all_pages_sheet1(max_result=None):
    if max_result is None:
        max_result = MAX_RESULT_PER_PAGE
    
    print("Fetching Sheet 1 data (all pages)...")
    logger.info("Starting Sheet 1 data fetch")
    
    all_data = []
    print(f"  Fetching page 0...")
    page0_data = get_api_data_sheet1(max_result=max_result, page_number=0)
    if page0_data['data']:
        all_data.extend(page0_data['data'])
        logger.info(f"Sheet 1 - Page 0: Found {len(page0_data['data'])} records")
        print(f"    ✓ Page 0: Found {len(page0_data['data'])} records")
    
    print(f"  Fetching page 1...")
    first_page = get_api_data_sheet1(max_result=max_result, page_number=1)
    if first_page['data']:
        existing_codes = set([item.get('notificationCode', '') for item in all_data])
        for record in first_page['data']:
            code = record.get('notificationCode', '')
            if code not in existing_codes:
                all_data.append(record)
                existing_codes.add(code)
    
    total_rows = first_page['totalRows']
    max_results = first_page['maxResults']
    
    if total_rows == 0:
        logger.warning("No data for Sheet 1")
        return []
    
    estimated_pages = math.ceil(total_rows / max_results)
    print(f"  Total rows (from API): {total_rows}, Estimated pages: {estimated_pages}")
    
    page = 2
    consecutive_empty_pages = 0
    max_consecutive_empty = 2
    
    while True:
        print(f"  Fetching page {page}...")
        page_data = get_api_data_sheet1(max_result=max_result, page_number=page)
        
        if page_data['data']:
            all_data.extend(page_data['data'])
            consecutive_empty_pages = 0
            page += 1
            if page > estimated_pages * 2:
                logger.warning(f"Sheet 1 - Fetched {page - 1} pages but estimated only {estimated_pages}. Stopping.")
                break
        else:
            consecutive_empty_pages += 1
            if consecutive_empty_pages >= max_consecutive_empty:
                break
            page += 1
    
    actual_count = len(all_data)
    print(f"✓ Fetched {actual_count} records for Sheet 1")
    
    if actual_count != total_rows:
        difference = total_rows - actual_count
        if difference > 0:
            missing_records = find_missing_records_sheet1(all_data, page - max_consecutive_empty, estimated_pages, max_result, difference, total_rows)
            if missing_records:
                all_data.extend(missing_records)
    
    known_missing_codes = ["2042025160147", "1742025091730", "1742025093606", "2042025153631"]
    existing_codes = set([item.get('notificationCode', '') for item in all_data])
    
    for code in known_missing_codes:
        if code not in existing_codes:
            record = get_api_data_by_notification_code(code, use_filter=False)
            if record:
                all_data.append(record)
                existing_codes.add(code)
            time.sleep(0.3)
    
    return all_data

def get_all_pages_sheet2(max_result=None):
    if max_result is None:
        max_result = MAX_RESULT_PER_PAGE
    
    print("Fetching Sheet 2 data (all pages)...")
    logger.info("Starting Sheet 2 data fetch")
    
    all_data = []
    print(f"  Fetching page 0...")
    page0_data = get_api_data_sheet2(max_result=max_result, page_number=0)
    if page0_data['data']:
        all_data.extend(page0_data['data'])
    
    print(f"  Fetching page 1...")
    first_page = get_api_data_sheet2(max_result=max_result, page_number=1)
    if first_page['data']:
        existing_codes = set([item.get('notificationCode', '') for item in all_data])
        for record in first_page['data']:
            code = record.get('notificationCode', '')
            if code not in existing_codes:
                all_data.append(record)
                existing_codes.add(code)
    
    total_rows = first_page['totalRows']
    max_results = first_page['maxResults']
    
    if total_rows == 0:
        logger.warning("No data for Sheet 2")
        return []
    
    estimated_pages = math.ceil(total_rows / max_results)
    
    page = 2
    consecutive_empty_pages = 0
    max_consecutive_empty = 2
    
    while True:
        print(f"  Fetching page {page}...")
        page_data = get_api_data_sheet2(max_result=max_result, page_number=page)
        
        if page_data['data']:
            all_data.extend(page_data['data'])
            consecutive_empty_pages = 0
            page += 1
            if page > estimated_pages * 2:
                break
        else:
            consecutive_empty_pages += 1
            if consecutive_empty_pages >= max_consecutive_empty:
                break
            page += 1
    
    actual_count = len(all_data)
    print(f"✓ Fetched {actual_count} records for Sheet 2")
    
    if actual_count != total_rows:
        difference = total_rows - actual_count
        if difference > 0:
            missing_records = find_missing_records_sheet2(all_data, page - max_consecutive_empty, estimated_pages, max_result, difference, total_rows)
            if missing_records:
                all_data.extend(missing_records)
    
    known_missing_codes = ["2042025160147", "1742025091730", "1742025093606", "2042025153631"]
    existing_codes = set([item.get('notificationCode', '') for item in all_data])
    
    for code in known_missing_codes:
        if code not in existing_codes:
            record = get_api_data_by_notification_code(code, use_filter=True)
            if record:
                all_data.append(record)
                existing_codes.add(code)
            else:
                record = get_api_data_by_notification_code(code, use_filter=False)
                if record:
                    all_data.append(record)
                    existing_codes.add(code)
            time.sleep(0.3)
    
    return all_data

# ==================== MISSING RECORDS RECOVERY ====================

def find_missing_records_sheet1(existing_data, last_page_with_data, estimated_pages, max_result, expected_missing, total_rows_from_api):
    found_records = []
    existing_codes = set([item.get('notificationCode', '') for item in existing_data])
    
    pages_to_retry = list(range(max(1, last_page_with_data - 10), last_page_with_data + 1))
    for page_num in pages_to_retry:
        for attempt in range(5):
            page_data = get_api_data_sheet1(max_result=max_result, page_number=page_num, retry_count=0)
            if page_data['data']:
                for record in page_data['data']:
                    code = record.get('notificationCode', '')
                    if code and code not in existing_codes:
                        found_records.append(record)
                        existing_codes.add(code)
                if len(found_records) >= expected_missing:
                    break
            time.sleep(0.3)
        if len(found_records) >= expected_missing:
            break
            
    return found_records

def find_missing_records_sheet2(existing_data, last_page_with_data, estimated_pages, max_result, expected_missing, total_rows_from_api):
    found_records = []
    existing_codes = set([item.get('notificationCode', '') for item in existing_data])
    
    pages_to_retry = list(range(max(1, last_page_with_data - 10), last_page_with_data + 1))
    for page_num in pages_to_retry:
        for attempt in range(5):
            page_data = get_api_data_sheet2(max_result=max_result, page_number=page_num, retry_count=0)
            if page_data['data']:
                for record in page_data['data']:
                    code = record.get('notificationCode', '')
                    if code and code not in existing_codes:
                        found_records.append(record)
                        existing_codes.add(code)
                if len(found_records) >= expected_missing:
                    break
            time.sleep(0.3)
        if len(found_records) >= expected_missing:
            break
            
    return found_records

def check_notification_code_exists(notification_code, max_pages_to_check=100):
    for page in range(1, min(max_pages_to_check, 100) + 1):
        page_data = get_api_data_sheet1(max_result=100, page_number=page)
        codes = [item.get('notificationCode', '') for item in page_data['data']]
        if notification_code in codes:
            return (True, 1)
        if not page_data['data']:
            break
    
    for page in range(1, min(max_pages_to_check, 20) + 1):
        page_data = get_api_data_sheet2(max_result=100, page_number=page)
        codes = [item.get('notificationCode', '') for item in page_data['data']]
        if notification_code in codes:
            return (True, 2)
        if not page_data['data']:
            break
            
    return (False, 0)

# ==================== DATA PROCESSING ====================

def extract_sheet1_fields(data_list):
    result = []
    for item in data_list:
        result.append({
            'nameCosmeticHeb': item.get('nameCosmeticHeb', ''),
            'nameCosmeticEng': item.get('nameCosmeticEng', ''),
            'notificationCode': item.get('notificationCode', ''),
            'importTrack': item.get('importTrack', ''),
            'rpCorporation': item.get('rpCorporation', ''),
            'manufacturer': item.get('manufacturer', ''),
            'importer': item.get('importer', '')
        })
    return result

def format_packages(packages_list):
    if not packages_list or not isinstance(packages_list, list):
        return ""
    formatted = []
    for pkg in packages_list:
        if isinstance(pkg, dict):
            package_name = pkg.get('packageName', '')
            quantity = pkg.get('quantity', '')
            measurement = pkg.get('measurementDesc', '')
            if package_name or quantity or measurement:
                formatted.append(f"{package_name} {quantity} {measurement}".strip())
    return " | ".join(formatted)

def format_shades(shades_list):
    if not shades_list or not isinstance(shades_list, list):
        return []
    shade_names = []
    for idx, shade in enumerate(shades_list):
        if isinstance(shade, dict):
            shade_name = shade.get('shadeName', '')
            if shade_name:
                shade_names.append(shade_name)
    return shade_names

def flatten_dict_for_sheet2(d, parent_key='', sep='_'):
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if k == 'packages' and isinstance(v, list):
            items.append((new_key, format_packages(v)))
        elif k == 'shades' and isinstance(v, list):
            pass
        elif isinstance(v, dict):
            items.extend(flatten_dict_for_sheet2(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            items.append((new_key, json.dumps(v, ensure_ascii=False)))
        else:
            items.append((new_key, v))
    return dict(items)

# ==================== GOOGLE SHEETS FUNCTIONS ====================

def setup_google_sheets_client():
    if not os.path.exists(CREDENTIALS_FILE):
        print("❌ ERROR: credentials.json not found")
        return None
    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    try:
        creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=scope)
        return gspread.authorize(creds)
    except Exception as e:
        print(f"❌ Error setting up Google Sheets client: {e}")
        return None

def create_google_sheet_example(use_sample_data=True, spreadsheet_id=None):
    client = setup_google_sheets_client()
    if not client: return None
    
    if spreadsheet_id:
        spreadsheet = client.open_by_key(spreadsheet_id)
    else:
        sheet_name = f"Cosmetics Data Example - {datetime.now().strftime('%Y%m%d_%H%M%S')}"
        spreadsheet = client.create(sheet_name)
        spreadsheet.share('', perm_type='anyone', role='reader')
    
    try:
        if use_sample_data:
            data_sheet1 = get_api_data_sheet1(max_result=10, page_number=1)['data']
            data_sheet2 = get_api_data_sheet2(max_result=10, page_number=1)['data']
        else:
            data_sheet1 = get_all_pages_sheet1(max_result=100)
            data_sheet2 = get_all_pages_sheet2(max_result=100)
        
        # --- SHEET 1 ---
        try:
            worksheet1 = spreadsheet.worksheet("כל המוצרים")
            worksheet1.clear()
        except:
            worksheet1 = spreadsheet.sheet1
            worksheet1.update_title("כל המוצרים")
        
        sheet1_data = extract_sheet1_fields(data_sheet1)
        
        # Build Lookup Dictionary for Dates
        sheet2_dates = {}
        for item in data_sheet2:
            code = item.get('notificationCode', '')
            if code:
                sheet2_dates[code] = {
                    'firstDate': item.get('firstDate', ''),
                    'lastDate': item.get('lastDate', '')
                }
        
        headers1 = ['nameCosmeticHeb', 'nameCosmeticEng', 'notificationCode', 'importTrack', 'rpCorporation', 'manufacturer', 'importer', 'firstDate', 'lastDate']
        all_rows = [headers1]
        for item in sheet1_data:
            code = item.get('notificationCode', '')
            dates = sheet2_dates.get(code, {'firstDate': '', 'lastDate': ''})
            row = [
                item.get('nameCosmeticHeb', ''), item.get('nameCosmeticEng', ''), item.get('notificationCode', ''),
                item.get('importTrack', ''), item.get('rpCorporation', ''), item.get('manufacturer', ''),
                item.get('importer', ''), dates['firstDate'], dates['lastDate']
            ]
            all_rows.append(row)
            
        batch_size = SHEETS_BATCH_SIZE
        for i in range(0, len(all_rows), batch_size):
            batch = all_rows[i:i + batch_size]
            append_rows_with_retry(worksheet1, batch, "Sheet 1 Batch")
            time.sleep(SHEETS_BATCH_DELAY)
            
        # --- SHEET 2 ---
        try:
            worksheet2 = spreadsheet.worksheet("גלי עמיר בעמ")
            worksheet2.clear()
        except:
            worksheet2 = spreadsheet.add_worksheet(title="גלי עמיר בעמ", rows=1000, cols=50)
            
        if data_sheet2:
            all_rows2 = []
            first_item = data_sheet2[0]
            flattened_first = flatten_dict_for_sheet2(first_item)
            base_headers = ['notificationCode', 'importTrack', 'rpCorporation', 'manufacturer', 'importer']
            other_headers = [k for k in flattened_first.keys() if k not in base_headers]
            if 'shades' not in other_headers: other_headers.append('shades')
            if 'shades2' not in other_headers: other_headers.append('shades2')
            headers2 = base_headers + other_headers
            all_rows2.append(headers2)
            
            for item in data_sheet2:
                flattened_item = flatten_dict_for_sheet2(item)
                shades = item.get('shades', [])
                shade_names = format_shades(shades)
                if shade_names:
                    all_shades_str = " | ".join(shade_names)
                    row = []
                    for h in headers2:
                        if h == 'shades': row.append(all_shades_str)
                        elif h == 'shades2': row.append('')
                        else: row.append(flattened_item.get(h, ''))
                    all_rows2.append(row)
                    for shade_name in shade_names:
                        row = []
                        for h in headers2:
                            if h == 'shades': row.append('')
                            elif h == 'shades2': row.append(shade_name)
                            else: row.append(flattened_item.get(h, ''))
                        all_rows2.append(row)
                else:
                    row = []
                    for h in headers2:
                        if h == 'shades' or h == 'shades2': row.append('')
                        else: row.append(flattened_item.get(h, ''))
                    all_rows2.append(row)
                    
            for i in range(0, len(all_rows2), batch_size):
                batch = all_rows2[i:i + batch_size]
                append_rows_with_retry(worksheet2, batch, "Sheet 2 Batch")
                time.sleep(SHEETS_BATCH_DELAY)
                
        return spreadsheet.url
    except Exception as e:
        print(f"❌ Error: {e}")
        return None

def append_rows_with_retry(worksheet, rows, batch_name="batch"):
    import gspread.exceptions
    for attempt in range(SHEETS_MAX_RETRIES):
        try:
            worksheet.append_rows(rows)
            return True
        except gspread.exceptions.APIError as e:
            if "502" in str(e) or "rate limit" in str(e).lower() or "quota" in str(e).lower():
                time.sleep(SHEETS_RETRY_DELAY * (2 ** attempt))
                continue
            raise
        except Exception:
            raise
    return False

def update_existing_sheet(spreadsheet_id=None):
    if spreadsheet_id is None:
        spreadsheet_id = SPREADSHEET_ID
        
    client = setup_google_sheets_client()
    if not client: return False
    
    try:
        spreadsheet = client.open_by_key(spreadsheet_id)
        data_sheet1 = get_all_pages_sheet1()
        data_sheet2 = get_all_pages_sheet2()
        
        # --- Cross Reference Missing Codes ---
        codes_sheet1 = set([item.get('notificationCode', '') for item in data_sheet1])
        codes_sheet2 = set([item.get('notificationCode', '') for item in data_sheet2])
        missing_in_sheet1 = codes_sheet2 - codes_sheet1
        
        if missing_in_sheet1:
            for code in missing_in_sheet1:
                record = get_api_data_by_notification_code(code, use_filter=False)
                if record and code not in codes_sheet1:
                    data_sheet1.append(record)
                    codes_sheet1.add(code)
                time.sleep(0.3)
                
        # --- Update Sheet 1 ---
        worksheet1 = spreadsheet.worksheet("כל המוצרים")
        sheet1_data = extract_sheet1_fields(data_sheet1)
        
        # Build Lookup Dictionary for Dates
        sheet2_dates = {}
        for item in data_sheet2:
            code = item.get('notificationCode', '')
            if code:
                sheet2_dates[code] = {
                    'firstDate': item.get('firstDate', ''),
                    'lastDate': item.get('lastDate', '')
                }
                
        headers1 = ['nameCosmeticHeb', 'nameCosmeticEng', 'notificationCode', 'importTrack', 'rpCorporation', 'manufacturer', 'importer', 'firstDate', 'lastDate']
        all_rows = [headers1]
        for item in sheet1_data:
            code = item.get('notificationCode', '')
            dates = sheet2_dates.get(code, {'firstDate': '', 'lastDate': ''})
            row = [
                item.get('nameCosmeticHeb', ''), item.get('nameCosmeticEng', ''), item.get('notificationCode', ''),
                item.get('importTrack', ''), item.get('rpCorporation', ''), item.get('manufacturer', ''),
                item.get('importer', ''), dates['firstDate'], dates['lastDate']
            ]
            all_rows.append(row)
            
        worksheet1.clear()
        
        # FORCE Grid Expansion to ensure Columns H and I exist
        try:
            current_cols = worksheet1.col_count
            if current_cols < 9:
                worksheet1.add_cols(9 - current_cols)
        except Exception:
            pass # Fails if gspread version is old, fallback to resize
            
        try:
            worksheet1.resize(rows=len(all_rows), cols=9)
        except Exception as e:
            logger.warning(f"Could not resize Sheet 1: {e}")
            
        batch_size = SHEETS_BATCH_SIZE
        for i in range(0, len(all_rows), batch_size):
            batch = all_rows[i:i + batch_size]
            start_row = i + 1
            end_row = min(i + batch_size, len(all_rows))
            range_name = f"A{start_row}:I{end_row}"
            
            try:
                worksheet1.update(range_name, batch, value_input_option='RAW')
            except Exception as e:
                if "502" in str(e) or "rate limit" in str(e).lower() or "quota" in str(e).lower():
                    time.sleep(5)
                    worksheet1.update(range_name, batch, value_input_option='RAW')
                else:
                    raise
            time.sleep(SHEETS_BATCH_DELAY)
            
        # --- Update Sheet 2 ---
        worksheet2 = spreadsheet.worksheet("גלי עמיר בעמ")
        if data_sheet2:
            all_rows2 = []
            first_item = data_sheet2[0]
            flattened_first = flatten_dict_for_sheet2(first_item)
            base_headers = ['notificationCode', 'importTrack', 'rpCorporation', 'manufacturer', 'importer']
            other_headers = [k for k in flattened_first.keys() if k not in base_headers]
            if 'shades' not in other_headers: other_headers.append('shades')
            if 'shades2' not in other_headers: other_headers.append('shades2')
            headers2 = base_headers + other_headers
            all_rows2.append(headers2)
            
            for item in data_sheet2:
                flattened_item = flatten_dict_for_sheet2(item)
                shades = item.get('shades', [])
                shade_names = format_shades(shades)
                if shade_names:
                    all_shades_str = " | ".join(shade_names)
                    row = []
                    for h in headers2:
                        if h == 'shades': row.append(all_shades_str)
                        elif h == 'shades2': row.append('')
                        else: row.append(flattened_item.get(h, ''))
                    all_rows2.append(row)
                    for shade_name in shade_names:
                        row = []
                        for h in headers2:
                            if h == 'shades': row.append('')
                            elif h == 'shades2': row.append(shade_name)
                            else: row.append(flattened_item.get(h, ''))
                        all_rows2.append(row)
                else:
                    row = []
                    for h in headers2:
                        if h == 'shades' or h == 'shades2': row.append('')
                        else: row.append(flattened_item.get(h, ''))
                    all_rows2.append(row)
                    
            worksheet2.clear()
            num_cols = len(headers2)
            
            try:
                current_cols = worksheet2.col_count
                if current_cols < num_cols:
                    worksheet2.add_cols(num_cols - current_cols)
                worksheet2.resize(rows=len(all_rows2), cols=num_cols)
            except Exception as e:
                logger.warning(f"Could not resize Sheet 2: {e}")
                
            def get_column_letter(col_num):
                result = ""
                while col_num > 0:
                    col_num -= 1
                    result = chr(65 + (col_num % 26)) + result
                    col_num //= 26
                return result
                
            last_col_letter = get_column_letter(num_cols)
            
            for i in range(0, len(all_rows2), batch_size):
                batch = all_rows2[i:i + batch_size]
                start_row = i + 1
                end_row = min(i + batch_size, len(all_rows2))
                range_name = f"A{start_row}:{last_col_letter}{end_row}"
                
                try:
                    worksheet2.update(range_name, batch, value_input_option='RAW')
                except Exception as e:
                    if "502" in str(e) or "rate limit" in str(e).lower() or "quota" in str(e).lower():
                        time.sleep(5)
                        worksheet2.update(range_name, batch, value_input_option='RAW')
                    else:
                        raise
                time.sleep(SHEETS_BATCH_DELAY)
                
        print("✅ Update completed!")
        return True
    except Exception as e:
        print(f"❌ Error updating Google Sheet: {e}")
        import traceback
        traceback.print_exc()
        return False

# ==================== MAIN ====================

if __name__ == "__main__":
    if len(sys.argv) > 1:
        command = sys.argv[1]
        if command == "test":
            print("Testing API calls...")
            data1 = get_api_data_sheet1(max_result=5, page_number=1)
            data2 = get_api_data_sheet2(max_result=5, page_number=1)
            print(f"\nSheet 1 - Total rows: {data1['totalRows']}, Records: {len(data1['data'])}")
            print(f"Sheet 2 - Total rows: {data2['totalRows']}, Records: {len(data2['data'])}")
    else:
        if not SPREADSHEET_ID:
            print("❌ ERROR: SPREADSHEET_ID not set in main.py")
            sys.exit(1)
        update_existing_sheet()
