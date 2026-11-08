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
SHEETS_BATCH_SIZE = 1000  # Batch size to avoid 502 errors
SHEETS_BATCH_DELAY = 1  # Seconds to wait between batches
SHEETS_MAX_RETRIES = 5  # Number of retries for Google Sheets API calls
SHEETS_RETRY_DELAY = 5  # Initial delay for retries (exponential backoff)

# Logging Configuration
ENABLE_LOGGING = True

# ==================== SUPPLIERS CONFIGURATION ====================
# Extracted from your provided table (Name: {item_id: מספר, type_id: סוג})
TARGET_SUPPLIERS = {
    'רייס פרופשונל בעמ': {'item_id': 1423, 'type_id': 1},
    'רון ביוטיפול בע"מ': {'item_id': 3041, 'type_id': 1},
    'קיי.די.אל.אר בע"מ': {'item_id': 3525, 'type_id': 2},
    'צורי הדס': {'item_id': 3197, 'type_id': 2},
    "פובלי' כ א בעמ": {'item_id': 1298, 'type_id': 1},
    'סנסיישן ייצור ושיווק בעמ': {'item_id': 1545, 'type_id': 2},
    'סלוקובסקי-סלוק שרון': {'item_id': 2965, 'type_id': 1},
    'נייל פארם - מעבדות סקין בעמ': {'item_id': 1513, 'type_id': 4},
    'נטלי גפן סחר ויבוא בע"מ': {'item_id': 3035, 'type_id': 1},
    'מיידלע סבונים בעמ': {'item_id': 1691, 'type_id': 2},
    'מאמי קר בעמ': {'item_id': 1374, 'type_id': 2},
    'טופ סאן שירותים בעמ': {'item_id': 1552, 'type_id': 1},
    'בוקו אס. ואי מעבדות קוסמטיקה בעמ': {'item_id': 2836, 'type_id': 2},
    'ב.עלמה יבוא ושיווק בעמ': {'item_id': 3051, 'type_id': 1},
    'אס אל קוסמטיקס': {'item_id': 1468, 'type_id': 2},
}


# ==================== LOGGING SETUP ====================
if ENABLE_LOGGING:
    logs_dir = "logs"
    if not os.path.exists(logs_dir):
        os.makedirs(logs_dir)
    
    today = datetime.now()
    log_filename = f"automation-{today.strftime('%d-%m-%Y')}.log"
    log_file_path = os.path.join(logs_dir, log_filename)
    
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    
    file_handler = logging.FileHandler(log_file_path, encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(file_handler)
else:
    logger = logging.getLogger(__name__)
    logger.addHandler(logging.NullHandler())


# ==================== FORMATTING FUNCTIONS ====================
def format_api_date(date_str):
    if not date_str:
        return ""
    try:
        if 'T' in str(date_str):
            dt_part = str(date_str).split('T')[0]
            dt = datetime.strptime(dt_part, "%Y-%m-%d")
            return dt.strftime("%d-%m-%Y")
        return str(date_str)
    except Exception:
        return str(date_str)

def extract_sheet1_fields(data_list):
    result = []
    for item in data_list:
        result.append({
            'nameCosmeticHeb': item.get('nameCosmeticHeb', ''),
            'nameCosmeticEng': item.get('nameCosmeticEng', ''),
            'notificationCode': protect_leading_zero(item.get('notificationCode', '')),
            'importTrack': item.get('importTrack', ''),
            'rpCorporation': item.get('rpCorporation', ''),
            'manufacturer': item.get('manufacturer', ''),
            'importer': item.get('importer', ''),
            'firstDate': format_api_date(item.get('firstDate', '')),
            'lastDate': format_api_date(item.get('lastDate', ''))
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
    for shade in shades_list:
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
            pass  # Shades handled separately
        elif isinstance(v, dict):
            items.extend(flatten_dict_for_sheet2(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            items.append((new_key, json.dumps(v, ensure_ascii=False)))
        else:
            items.append((new_key, protect_leading_zero(v)))
    return dict(items)

def protect_leading_zero(value):
    if value is None:
        return ""
    val_str = str(value)
    if val_str.startswith('0') and val_str.isdigit():
        return f"'{val_str}"
    return value

def get_column_letter(col_num):
    """Convert column number to letter (1->A, 2->B, ..., 27->AA)"""
    result = ""
    while col_num > 0:
        col_num -= 1
        result = chr(65 + (col_num % 26)) + result
        col_num //= 26
    return result


# ==================== API FUNCTIONS ====================
def get_api_data_sheet1(max_result=100, page_number=1, retry_count=0):
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
            return {
                'data': data['returnObject']['cosmeticsList'],
                'totalRows': data['returnObject'].get('totalRows', 0)
            }
        return {'data': [], 'totalRows': 0}
    except Exception as e:
        if retry_count < MAX_RETRIES:
            time.sleep(RETRY_DELAY)
            return get_api_data_sheet1(max_result, page_number, retry_count + 1)
        else:
            logger.error(f"Error fetching Sheet 1 data (page {page_number}): {e}")
            return {'data': [], 'totalRows': 0}

def get_all_pages_sheet1(max_result=100):
    print("Fetching ALL PRODUCTS data...")
    logger.info("Starting Sheet 1 data fetch")
    all_data = []
    
    # Try page 0 first
    page0_data = get_api_data_sheet1(max_result=max_result, page_number=0)
    if page0_data['data']:
        all_data.extend(page0_data['data'])
    
    page = 1
    consecutive_empty = 0
    
    while consecutive_empty < 2:
        print(f"  Fetching page {page}...")
        page_data = get_api_data_sheet1(max_result=max_result, page_number=page)
        
        if page_data['data']:
            all_data.extend(page_data['data'])
            consecutive_empty = 0
        else:
            consecutive_empty += 1
        page += 1
            
    print(f"✓ Fetched {len(all_data)} total records for all products.")
    return all_data

def get_api_data_by_supplier(business_id, type_id, max_result=100, page_number=1, retry_count=0):
    payload = {
        "isDescending": False,
        "maxResult": max_result,
        "pageNumber": page_number,
        "businessNotificationItemId": business_id,
        "businessTypeNotificationId": type_id
    }
    headers = {"Content-Type": "application/json"}
    
    try:
        response = requests.post(API_URL, json=payload, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        if 'returnObject' in data and 'cosmeticsList' in data['returnObject']:
            return {'data': data['returnObject']['cosmeticsList']}
        return {'data': []}
    except Exception as e:
        if retry_count < MAX_RETRIES:
            time.sleep(RETRY_DELAY)
            return get_api_data_by_supplier(business_id, type_id, max_result, page_number, retry_count + 1)
        else:
            logger.error(f"Error fetching data (ID {business_id}, Type {type_id}, page {page_number}): {e}")
            return {'data': []}

def get_all_pages_by_supplier(business_id, type_id, supplier_name, max_result=100):
    print(f"  Fetching API data for {supplier_name} (Item ID: {business_id}, Type ID: {type_id})...")
    all_data = []
    
    # Fetch page 0
    page0_data = get_api_data_by_supplier(business_id, type_id, max_result=max_result, page_number=0)
    if page0_data['data']:
        all_data.extend(page0_data['data'])

    page = 1
    consecutive_empty = 0
    
    while consecutive_empty < 2:
        page_data = get_api_data_by_supplier(business_id, type_id, max_result=max_result, page_number=page)
        if page_data['data']:
            all_data.extend(page_data['data'])
            consecutive_empty = 0
        else:
            consecutive_empty += 1
        page += 1
        
    print(f"  ✓ Found {len(all_data)} records for {supplier_name}")
    return all_data


# ==================== GOOGLE SHEETS FUNCTIONS ====================
def setup_google_sheets_client():
    if not os.path.exists(CREDENTIALS_FILE):
        print(f"❌ ERROR: {CREDENTIALS_FILE} not found")
        return None
    
    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    
    try:
        creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=scope)
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        print(f"❌ Error setting up Google Sheets client: {e}")
        return None

def update_existing_sheet(spreadsheet_id=None):
    if spreadsheet_id is None:
        spreadsheet_id = SPREADSHEET_ID
    
    print("="*60)
    print("UPDATE GOOGLE SHEET")
    print("="*60)
    
    client = setup_google_sheets_client()
    if not client:
        return False
    
    try:
        spreadsheet = client.open_by_key(spreadsheet_id)
        print(f"✓ Opened spreadsheet: {spreadsheet.url}")
        
        # ==========================================
        # PART 1: MAIN SHEET (All Suppliers, Few Columns)
        # ==========================================
        data_sheet1 = get_all_pages_sheet1()
        
        worksheet1 = spreadsheet.worksheet("כל המוצרים")
        sheet1_data = extract_sheet1_fields(data_sheet1)
        
        headers1 = ['nameCosmeticHeb', 'nameCosmeticEng', 'notificationCode', 'importTrack', 'rpCorporation', 'manufacturer', 'importer', 'firstDate', 'lastDate']
        all_rows1 = [headers1]
        
        for item in sheet1_data:
            row = [
                item.get('nameCosmeticHeb', ''), item.get('nameCosmeticEng', ''), item.get('notificationCode', ''),
                item.get('importTrack', ''), item.get('rpCorporation', ''), item.get('manufacturer', ''),
                item.get('importer', ''), item.get('firstDate', ''), item.get('lastDate', '')
            ]
            all_rows1.append(row)

        print(f"  Clearing old data from Main Sheet...")
        worksheet1.clear()
        
        try:
            worksheet1.resize(rows=len(all_rows1), cols=len(headers1))
        except Exception:
            pass

        batch_size = SHEETS_BATCH_SIZE
        for i in range(0, len(all_rows1), batch_size):
            batch = all_rows1[i:i + batch_size]
            start_row = i + 1
            end_row = min(i + batch_size, len(all_rows1))
            range_name = f"A{start_row}:I{end_row}" # A to I for 9 columns
            
            worksheet1.update(range_name, batch, value_input_option='USER_ENTERED')
            time.sleep(SHEETS_BATCH_DELAY)
            
        print(f"✓ Updated Main Sheet (כל המוצרים): {len(all_rows1)-1} rows")


        # ==========================================
        # PART 2: THE TARGET SUPPLIER SHEETS 
        # ==========================================
        for supplier_name, ids in TARGET_SUPPLIERS.items():
            business_id = ids['item_id']
            type_id = ids['type_id']
            
            print(f"\n{'='*40}")
            print(f"Processing Supplier: {supplier_name}")
            print(f"{'='*40}")
            
            supplier_data = get_all_pages_by_supplier(business_id, type_id, supplier_name)
            if not supplier_data:
                continue
                
            try:
                worksheet2 = spreadsheet.worksheet(supplier_name)
                worksheet2.clear()
            except gspread.exceptions.WorksheetNotFound:
                worksheet2 = spreadsheet.add_worksheet(title=supplier_name, rows=1000, cols=50)

            all_rows2 = []
            flattened_first = flatten_dict_for_sheet2(supplier_data[0])
            
            base_headers = ['notificationCode', 'importTrack', 'rpCorporation', 'manufacturer', 'importer']
            other_headers = [k for k in flattened_first.keys() if k not in base_headers]
            
            if 'shades' not in other_headers:
                other_headers.append('shades')
            if 'shades2' not in other_headers:
                other_headers.append('shades2')
                
            headers2 = base_headers + other_headers
            all_rows2.append(headers2)
            
            for item in supplier_data:
                flattened_item = flatten_dict_for_sheet2(item)
                shades = item.get('shades', [])
                shade_names = format_shades(shades)
                
                if shade_names:
                    # First row
                    row = []
                    for h in headers2:
                        if h == 'shades':
                            row.append(" | ".join(shade_names))
                        elif h == 'shades2':
                            row.append('')
                        else:
                            row.append(flattened_item.get(h, ''))
                    all_rows2.append(row)
                    
                    # Subsequent rows for expanded shades
                    for shade_name in shade_names:
                        row = []
                        for h in headers2:
                            if h == 'shades':
                                row.append('')
                            elif h == 'shades2':
                                row.append(shade_name)
                            else:
                                row.append(flattened_item.get(h, ''))
                        all_rows2.append(row)
                else:
                    # No shades
                    row = [flattened_item.get(h, '') if h not in ['shades', 'shades2'] else '' for h in headers2]
                    all_rows2.append(row)

            try:
                worksheet2.resize(rows=len(all_rows2), cols=len(headers2))
            except Exception:
                pass

            last_col_letter = get_column_letter(len(headers2))
            
            for i in range(0, len(all_rows2), batch_size):
                batch = all_rows2[i:i + batch_size]
                start_row = i + 1
                end_row = min(i + batch_size, len(all_rows2))
                range_name = f"A{start_row}:{last_col_letter}{end_row}"
                
                worksheet2.update(range_name, batch, value_input_option='USER_ENTERED')
                time.sleep(SHEETS_BATCH_DELAY)
                
            print(f"✓ Updated {supplier_name} tab: {len(all_rows2)-1} total rows")

        print("\n✅ Update completed for Main Sheet and all target suppliers!")
        return True
        
    except Exception as e:
        logger.error(f"Error updating Google Sheet: {e}", exc_info=True)
        print(f"\n❌ Error updating Google Sheet: {e}")
        return False

# ==================== MAIN ====================
if __name__ == "__main__":
    if not SPREADSHEET_ID:
        print("❌ ERROR: SPREADSHEET_ID not set in main.py")
        sys.exit(1)
        
    update_existing_sheet()
