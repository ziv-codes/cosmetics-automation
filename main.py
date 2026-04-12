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
    # Get all data from Sheet 1 via pagination
    # ✅ FIX: Fetch until we get empty page instead of relying on totalRows
    if max_result is None:
        max_result = MAX_RESULT_PER_PAGE
    
    print("Fetching Sheet 1 data (all pages)...")
    logger.info("Starting Sheet 1 data fetch")
    
    # ✅ FIX: Try page 0 first (API might start from 0)
    all_data = []
    print(f"  Fetching page 0...")
    page0_data = get_api_data_sheet1(max_result=max_result, page_number=0)
    if page0_data['data']:
        all_data.extend(page0_data['data'])
        logger.info(f"Sheet 1 - Page 0: Found {len(page0_data['data'])} records")
        print(f"    ✓ Page 0: Found {len(page0_data['data'])} records")
    
    # Get first page (page 1) to know totalRows (for reference only)
    print(f"  Fetching page 1...")
    first_page = get_api_data_sheet1(max_result=max_result, page_number=1)
    if first_page['data']:
        # Check for duplicates before adding
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
    
    # Calculate estimated number of pages (for reference)
    estimated_pages = math.ceil(total_rows / max_results)
    print(f"  Total rows (from API): {total_rows}, Estimated pages: {estimated_pages}")
    logger.info(f"Sheet 1 - Total rows from API: {total_rows}, Estimated pages: {estimated_pages}")
    
    # ✅ FIX: Fetch pages until we get empty page (more reliable than using totalRows)
    page = 2
    consecutive_empty_pages = 0
    max_consecutive_empty = 2  # Stop after 2 consecutive empty pages
    
    while True:
        print(f"  Fetching page {page}...")
        page_data = get_api_data_sheet1(max_result=max_result, page_number=page)
        
        if page_data['data']:
            # Got data, add to all_data
            all_data.extend(page_data['data'])
            consecutive_empty_pages = 0  # Reset counter
            page += 1
            
            # Safety limit: if we've fetched way more than expected, something's wrong
            if page > estimated_pages * 2:
                logger.warning(f"Sheet 1 - Fetched {page - 1} pages but estimated only {estimated_pages}. Stopping to prevent infinite loop.")
                break
        else:
            # Empty page
            consecutive_empty_pages += 1
            logger.warning(f"Sheet 1 - Page {page} returned no data (consecutive empty: {consecutive_empty_pages})")
            
            if consecutive_empty_pages >= max_consecutive_empty:
                # Stop after N consecutive empty pages
                logger.info(f"Sheet 1 - Stopping after {consecutive_empty_pages} consecutive empty pages")
                break
            
            page += 1
    
    # ✅ VERIFY: Check if we got all records
    actual_count = len(all_data)
    # Calculate last page with data: if we stopped due to empty pages, subtract them
    if consecutive_empty_pages >= max_consecutive_empty:
        last_page_with_data = page - max_consecutive_empty
    else:
        last_page_with_data = page - 1
    
    print(f"✓ Fetched {actual_count} records for Sheet 1 (from {last_page_with_data} pages)")
    logger.info(f"Sheet 1 - Fetched {actual_count} records from {last_page_with_data} pages (API reported {total_rows})")
    
    if actual_count != total_rows:
        difference = total_rows - actual_count
        logger.warning(f"Sheet 1 - MISMATCH: API reported {total_rows} records, but fetched {actual_count} (difference: {difference})")
        print(f"⚠ WARNING: API reported {total_rows} records, but fetched {actual_count} (difference: {difference})")
        
        if difference > 0:
            print(f"\n🔍 Attempting to find missing {difference} records...")
            logger.info(f"Sheet 1 - Attempting to find missing {difference} records")
            
            # Try to find missing records by:
            # 1. Retry last few pages with more attempts
            # 2. Check pages around the last page with data
            # 3. Try fetching with different page numbers
            
            missing_records = find_missing_records_sheet1(
                all_data, 
                last_page_with_data, 
                estimated_pages, 
                max_result, 
                difference,
                total_rows  # Pass total_rows for analysis
            )
            
            if missing_records:
                print(f"✓ Found {len(missing_records)} additional records!")
                logger.info(f"Sheet 1 - Found {len(missing_records)} additional records")
                all_data.extend(missing_records)
                actual_count = len(all_data)
                print(f"✓ Total records after recovery: {actual_count}")
            else:
                print(f"   Could not find missing records. They may have been deleted or API has pagination bug.")
                logger.warning(f"Sheet 1 - Could not recover missing {difference} records")
    
    # ✅ Check for duplicates
    notification_codes = [item.get('notificationCode', '') for item in all_data]
    unique_codes = set(notification_codes)
    if len(notification_codes) != len(unique_codes):
        duplicates = [code for code in notification_codes if notification_codes.count(code) > 1]
        logger.warning(f"Sheet 1 - Found {len(notification_codes) - len(unique_codes)} duplicate notification codes: {set(duplicates)}")
        print(f"⚠ WARNING: Found duplicate notification codes in Sheet 1")
    
    # ✅ ALWAYS check for specific missing codes reported by client (even if count matches)
    known_missing_codes = ["2042025160147", "1742025091730", "1742025093606", "2042025153631"]
    existing_codes = set(notification_codes)
    added_codes = []
    
    for code in known_missing_codes:
        if code not in existing_codes:
            print(f"  🔍 Checking known missing code: {code}...")
            # Query directly without filter (Sheet 1 has no filter)
            record = get_api_data_by_notification_code(code, use_filter=False)
            if record:
                all_data.append(record)
                existing_codes.add(code)
                added_codes.append(code)
                logger.info(f"Sheet 1 - Added missing record {code} via direct query (no filter)")
                print(f"    ✓ Added missing record: {code} (found in API without filter)")
            else:
                logger.warning(f"Sheet 1 - Code {code} not found in API (no filter)")
                print(f"    ✗ Code {code} not found in API")
            time.sleep(0.3)  # Small delay between queries
    
    if added_codes:
        print(f"  ✓ Added {len(added_codes)} missing records to Sheet 1: {added_codes}")
        logger.info(f"Sheet 1 - Added {len(added_codes)} missing records: {added_codes}")
    
    # ✅ Log all notification codes for debugging (only first 100 to avoid log file too large)
    final_codes = [item.get('notificationCode', '') for item in all_data]
    unique_codes = set(final_codes)
    logger.info(f"Sheet 1 - All notification codes ({len(unique_codes)} unique): {sorted(list(unique_codes))[:100]}...")
    
    return all_data

def get_all_pages_sheet2(max_result=None):
    # Get all data from Sheet 2 via pagination
    # ✅ FIX: Fetch until we get empty page instead of relying on totalRows
    if max_result is None:
        max_result = MAX_RESULT_PER_PAGE
    
    print("Fetching Sheet 2 data (all pages)...")
    logger.info("Starting Sheet 2 data fetch")
    
    # ✅ FIX: Try page 0 first (API might start from 0)
    all_data = []
    print(f"  Fetching page 0...")
    page0_data = get_api_data_sheet2(max_result=max_result, page_number=0)
    if page0_data['data']:
        all_data.extend(page0_data['data'])
        logger.info(f"Sheet 2 - Page 0: Found {len(page0_data['data'])} records")
        print(f"    ✓ Page 0: Found {len(page0_data['data'])} records")
    
    # Get first page (page 1) to know totalRows (for reference only)
    print(f"  Fetching page 1...")
    first_page = get_api_data_sheet2(max_result=max_result, page_number=1)
    if first_page['data']:
        # Check for duplicates before adding
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
    
    # Calculate estimated number of pages (for reference)
    estimated_pages = math.ceil(total_rows / max_results)
    print(f"  Total rows (from API): {total_rows}, Estimated pages: {estimated_pages}")
    logger.info(f"Sheet 2 - Total rows from API: {total_rows}, Estimated pages: {estimated_pages}")
    
    # ✅ FIX: Fetch pages until we get empty page (more reliable than using totalRows)
    page = 2
    consecutive_empty_pages = 0
    max_consecutive_empty = 2  # Stop after 2 consecutive empty pages
    
    while True:
        print(f"  Fetching page {page}...")
        page_data = get_api_data_sheet2(max_result=max_result, page_number=page)
        
        if page_data['data']:
            # Got data, add to all_data
            all_data.extend(page_data['data'])
            consecutive_empty_pages = 0  # Reset counter
            page += 1
            
            # Safety limit: if we've fetched way more than expected, something's wrong
            if page > estimated_pages * 2:
                logger.warning(f"Sheet 2 - Fetched {page - 1} pages but estimated only {estimated_pages}. Stopping to prevent infinite loop.")
                break
        else:
            # Empty page
            consecutive_empty_pages += 1
            logger.warning(f"Sheet 2 - Page {page} returned no data (consecutive empty: {consecutive_empty_pages})")
            
            if consecutive_empty_pages >= max_consecutive_empty:
                # Stop after N consecutive empty pages
                logger.info(f"Sheet 2 - Stopping after {consecutive_empty_pages} consecutive empty pages")
                break
            
            page += 1
    
    # ✅ VERIFY: Check if we got all records
    actual_count = len(all_data)
    # Calculate last page with data: if we stopped due to empty pages, subtract them
    if consecutive_empty_pages >= max_consecutive_empty:
        last_page_with_data = page - max_consecutive_empty
    else:
        last_page_with_data = page - 1
    
    print(f"✓ Fetched {actual_count} records for Sheet 2 (from {last_page_with_data} pages)")
    logger.info(f"Sheet 2 - Fetched {actual_count} records from {last_page_with_data} pages (API reported {total_rows})")
    
    if actual_count != total_rows:
        difference = total_rows - actual_count
        logger.warning(f"Sheet 2 - MISMATCH: API reported {total_rows} records, but fetched {actual_count} (difference: {difference})")
        print(f"⚠ WARNING: API reported {total_rows} records, but fetched {actual_count} (difference: {difference})")
        
        if difference > 0:
            print(f"\n🔍 Attempting to find missing {difference} records...")
            logger.info(f"Sheet 2 - Attempting to find missing {difference} records")
            
            missing_records = find_missing_records_sheet2(
                all_data, 
                last_page_with_data, 
                estimated_pages, 
                max_result, 
                difference,
                total_rows  # Pass total_rows for analysis
            )
            
            if missing_records:
                print(f"✓ Found {len(missing_records)} additional records!")
                logger.info(f"Sheet 2 - Found {len(missing_records)} additional records")
                all_data.extend(missing_records)
                actual_count = len(all_data)
                print(f"✓ Total records after recovery: {actual_count}")
            else:
                print(f"   Could not find missing records. They may have been deleted or API has pagination bug.")
                logger.warning(f"Sheet 2 - Could not recover missing {difference} records")
    
    # ✅ Check for duplicates
    notification_codes = [item.get('notificationCode', '') for item in all_data]
    unique_codes = set(notification_codes)
    if len(notification_codes) != len(unique_codes):
        duplicates = [code for code in notification_codes if notification_codes.count(code) > 1]
        logger.warning(f"Sheet 2 - Found {len(notification_codes) - len(unique_codes)} duplicate notification codes: {set(duplicates)}")
        print(f"⚠ WARNING: Found duplicate notification codes in Sheet 2")
    
    # ✅ ALWAYS check for specific missing codes reported by client (even if count matches)
    known_missing_codes = ["2042025160147", "1742025091730", "1742025093606", "2042025153631"]
    existing_codes = set(notification_codes)
    added_codes = []
    
    for code in known_missing_codes:
        if code not in existing_codes:
            print(f"  🔍 Checking known missing code: {code}...")
            # Try with filter first (Sheet 2 filter)
            record = get_api_data_by_notification_code(code, use_filter=True)
            if record:
                all_data.append(record)
                existing_codes.add(code)
                added_codes.append(code)
                logger.info(f"Sheet 2 - Added missing record {code} via direct query (with filter)")
                print(f"    ✓ Added missing record: {code} (found with Sheet 2 filter)")
            else:
                # Try without filter - if found, it means it doesn't match Sheet 2 filter
                record = get_api_data_by_notification_code(code, use_filter=False)
                if record:
                    logger.warning(f"Sheet 2 - Code {code} exists but does NOT match Sheet 2 filter (businessNotificationItemId=34, businessTypeNotificationId=5)")
                    print(f"    ⚠ Code {code} exists but does NOT match Sheet 2 filter criteria")
                else:
                    logger.warning(f"Sheet 2 - Code {code} not found in API")
                    print(f"    ✗ Code {code} not found in API")
            time.sleep(0.3)  # Small delay between queries
    
    if added_codes:
        print(f"  ✓ Added {len(added_codes)} missing records: {added_codes}")
        logger.info(f"Sheet 2 - Added {len(added_codes)} missing records: {added_codes}")
    
    # ✅ Log all notification codes for debugging (only first 100 to avoid log file too large)
    final_codes = [item.get('notificationCode', '') for item in all_data]
    unique_codes = set(final_codes)
    logger.info(f"Sheet 2 - All notification codes ({len(unique_codes)} unique): {sorted(list(unique_codes))[:100]}...")
    
    return all_data

# ==================== MISSING RECORDS RECOVERY ====================

def find_missing_records_sheet1(existing_data, last_page_with_data, estimated_pages, max_result, expected_missing, total_rows_from_api):
    """
    Try to find missing records by checking pages around the last page with data
    """
    found_records = []
    existing_codes = set([item.get('notificationCode', '') for item in existing_data])
    
    print(f"  Checking pages around page {last_page_with_data}...")
    logger.info(f"Sheet 1 - Starting missing records search. Last page with data: {last_page_with_data}, Expected missing: {expected_missing}")
    
    # Strategy 1: Analyze page 551 - it only has 62 records instead of 100
    # This suggests the missing 100 records might be in a different page or were deleted
    print(f"  Analyzing page 551 (has 62 records, missing 38)...")
    page_551_data = get_api_data_sheet1(max_result=max_result, page_number=551, retry_count=0)
    page_551_codes = set([item.get('notificationCode', '') for item in page_551_data['data']])
    logger.info(f"Sheet 1 - Page 551 has {len(page_551_data['data'])} records")
    
    # Strategy 2: Try fetching with reverse order (isDescending: True)
    # This might reveal records that are at the "end" but not accessible via normal pagination
    print(f"  Trying reverse order fetch (isDescending: True) to find missing records...")
    try:
        reverse_payload = {
            "isDescending": True,  # Reverse order
            "maxResult": max_result,
            "pageNumber": 1
        }
        response = requests.post(API_URL, json=reverse_payload, headers={"Content-Type": "application/json"}, timeout=30)
        if response.status_code == 200:
            reverse_data = response.json()
            if 'returnObject' in reverse_data and 'cosmeticsList' in reverse_data['returnObject']:
                reverse_codes = set([item.get('notificationCode', '') for item in reverse_data['returnObject']['cosmeticsList']])
                missing_in_reverse = existing_codes - reverse_codes
                if missing_in_reverse:
                    logger.info(f"Sheet 1 - Found {len(missing_in_reverse)} codes in normal order but not in reverse order")
                    print(f"  Found {len(missing_in_reverse)} codes that appear in normal order but not in reverse")
    except Exception as e:
        logger.warning(f"Sheet 1 - Error trying reverse order: {e}")
    
    # Strategy 3: Retry last few pages with more attempts
    pages_to_retry = list(range(max(1, last_page_with_data - 10), last_page_with_data + 1))
    print(f"  Retrying last {len(pages_to_retry)} pages with multiple attempts...")
    for page_num in pages_to_retry:
        for attempt in range(5):  # Retry up to 5 times
            page_data = get_api_data_sheet1(max_result=max_result, page_number=page_num, retry_count=0)
            if page_data['data']:
                for record in page_data['data']:
                    code = record.get('notificationCode', '')
                    if code and code not in existing_codes:
                        found_records.append(record)
                        existing_codes.add(code)
                        logger.info(f"Sheet 1 - Found missing record {code} on page {page_num} (attempt {attempt + 1})")
                        print(f"    ✓ Found missing record: {code} on page {page_num}")
                if len(found_records) >= expected_missing:
                    break
            time.sleep(0.3)  # Small delay between retries
        if len(found_records) >= expected_missing:
            break
    
    # Strategy 4: Check if API has a different totalRows when queried again
    print(f"  Re-checking API totalRows to see if it changed...")
    first_page_again = get_api_data_sheet1(max_result=max_result, page_number=1, retry_count=0)
    new_total_rows = first_page_again.get('totalRows', 0)
    if new_total_rows != 0:
        logger.info(f"Sheet 1 - API now reports totalRows: {new_total_rows} (previously: {total_rows_from_api})")
        if new_total_rows != total_rows_from_api:
            print(f"    ⚠ API totalRows changed! Now: {new_total_rows}, Previously: {total_rows_from_api}")
            diff = total_rows_from_api - new_total_rows
            if diff > 0:
                print(f"    This suggests {diff} records were deleted/removed")
            else:
                print(f"    This suggests {abs(diff)} records were added")
    
    # Strategy 5: Check pages after last page (in case API has pagination bug)
    if len(found_records) < expected_missing:
        print(f"  Checking pages after {last_page_with_data} (up to {estimated_pages + 20})...")
        for page_num in range(last_page_with_data + 1, min(estimated_pages + 20, last_page_with_data + 30)):
            page_data = get_api_data_sheet1(max_result=max_result, page_number=page_num, retry_count=0)
            if page_data['data']:
                for record in page_data['data']:
                    code = record.get('notificationCode', '')
                    if code and code not in existing_codes:
                        found_records.append(record)
                        existing_codes.add(code)
                        logger.info(f"Sheet 1 - Found missing record {code} on page {page_num}")
                        print(f"    ✓ Found missing record: {code} on page {page_num}")
                if len(found_records) >= expected_missing:
                    break
    
    # Strategy 6: Check if there are duplicate records that were counted twice
    print(f"  Checking for potential duplicate counting in API...")
    all_codes_list = [item.get('notificationCode', '') for item in existing_data]
    unique_codes_count = len(set(all_codes_list))
    if len(all_codes_list) != unique_codes_count:
        duplicates = len(all_codes_list) - unique_codes_count
        logger.warning(f"Sheet 1 - Found {duplicates} duplicate codes in fetched data")
        print(f"    ⚠ Found {duplicates} duplicate codes - API might be counting duplicates")
    
    if found_records:
        print(f"  ✓ Found {len(found_records)} missing records!")
    else:
        print(f"  ✗ Could not find missing records using standard methods")
        print(f"  📊 Analysis:")
        print(f"     - Page 551 has 62 records (missing 38 from expected 100)")
        print(f"     - Page 552 is empty")
        print(f"     - Total fetched: {len(existing_data)} records")
        print(f"     - API reported: {total_rows_from_api} records")
        print(f"     - Difference: {expected_missing} records")
        print(f"  💡 Possible reasons:")
        print(f"     1. API's totalRows is inaccurate (calculated incorrectly)")
        print(f"     2. {expected_missing} records were deleted/removed after API calculated totalRows")
        print(f"     3. API has pagination bug that skips some records")
        print(f"     4. Records exist but are filtered/hidden by API")
    
    return found_records

def find_missing_records_sheet2(existing_data, last_page_with_data, estimated_pages, max_result, expected_missing, total_rows_from_api):
    """
    Try to find missing records by checking pages around the last page with data
    """
    found_records = []
    existing_codes = set([item.get('notificationCode', '') for item in existing_data])
    
    print(f"  Checking pages around page {last_page_with_data}...")
    logger.info(f"Sheet 2 - Starting missing records search. Last page with data: {last_page_with_data}, Expected missing: {expected_missing}")
    
    # Strategy 1: Retry last few pages with more attempts
    pages_to_retry = list(range(max(1, last_page_with_data - 10), last_page_with_data + 1))
    print(f"  Retrying last {len(pages_to_retry)} pages with multiple attempts...")
    for page_num in pages_to_retry:
        for attempt in range(5):  # Retry up to 5 times
            page_data = get_api_data_sheet2(max_result=max_result, page_number=page_num, retry_count=0)
            if page_data['data']:
                for record in page_data['data']:
                    code = record.get('notificationCode', '')
                    if code and code not in existing_codes:
                        found_records.append(record)
                        existing_codes.add(code)
                        logger.info(f"Sheet 2 - Found missing record {code} on page {page_num} (attempt {attempt + 1})")
                        print(f"    ✓ Found missing record: {code} on page {page_num}")
                if len(found_records) >= expected_missing:
                    break
            time.sleep(0.3)  # Small delay between retries
        if len(found_records) >= expected_missing:
            break
    
    # Strategy 2: Check if API has a different totalRows when queried again
    print(f"  Re-checking API totalRows to see if it changed...")
    first_page_again = get_api_data_sheet2(max_result=max_result, page_number=1, retry_count=0)
    new_total_rows = first_page_again.get('totalRows', 0)
    if new_total_rows != 0:
        logger.info(f"Sheet 2 - API now reports totalRows: {new_total_rows} (previously: {total_rows_from_api})")
        if new_total_rows != total_rows_from_api:
            print(f"    ⚠ API totalRows changed! Now: {new_total_rows}, Previously: {total_rows_from_api}")
            diff = total_rows_from_api - new_total_rows
            if diff > 0:
                print(f"    This suggests {diff} records were deleted/removed")
            else:
                print(f"    This suggests {abs(diff)} records were added")
    
    # Strategy 3: Check pages after last page
    if len(found_records) < expected_missing:
        print(f"  Checking pages after {last_page_with_data} (up to {estimated_pages + 20})...")
        for page_num in range(last_page_with_data + 1, min(estimated_pages + 20, last_page_with_data + 30)):
            page_data = get_api_data_sheet2(max_result=max_result, page_number=page_num, retry_count=0)
            if page_data['data']:
                for record in page_data['data']:
                    code = record.get('notificationCode', '')
                    if code and code not in existing_codes:
                        found_records.append(record)
                        existing_codes.add(code)
                        logger.info(f"Sheet 2 - Found missing record {code} on page {page_num}")
                        print(f"    ✓ Found missing record: {code} on page {page_num}")
                if len(found_records) >= expected_missing:
                    break
    
    # Strategy 4: Try querying with pageNumber = 0 (API might start from 0)
    if len(found_records) < expected_missing:
        print(f"  Trying pageNumber = 0 (API might start from 0 instead of 1)...")
        try:
            payload_page0 = {
                "isDescending": False,
                "maxResult": max_result,
                "pageNumber": 0,
                "businessNotificationItemId": 34,
                "businessTypeNotificationId": 5
            }
            response = requests.post(API_URL, json=payload_page0, headers={"Content-Type": "application/json"}, timeout=30)
            response.raise_for_status()
            data = response.json()
            if 'returnObject' in data and 'cosmeticsList' in data['returnObject']:
                for record in data['returnObject']['cosmeticsList']:
                    code = record.get('notificationCode', '')
                    if code and code not in existing_codes:
                        found_records.append(record)
                        existing_codes.add(code)
                        logger.info(f"Sheet 2 - Found missing record {code} on page 0")
                        print(f"    ✓ Found missing record: {code} on page 0")
        except Exception as e:
            logger.warning(f"Sheet 2 - Error querying page 0: {e}")
    
    # Strategy 5: Try querying with direct notificationCode for known missing codes
    # Known missing codes that customer reported
    known_missing_codes = ["2042025160147", "1742025091730", "1742025093606", "2042025153631"]
    if len(found_records) < expected_missing:
        print(f"  Querying known missing notification codes directly...")
        for code in known_missing_codes:
            if code not in existing_codes:
                print(f"    Querying notification code: {code}...")
                record = get_api_data_by_notification_code(code, use_filter=True)
                if record:
                    found_records.append(record)
                    existing_codes.add(code)
                    logger.info(f"Sheet 2 - Found missing record {code} via direct query")
                    print(f"    ✓ Found missing record: {code} via direct query")
                else:
                    # Try without filter
                    record = get_api_data_by_notification_code(code, use_filter=False)
                    if record:
                        found_records.append(record)
                        existing_codes.add(code)
                        logger.info(f"Sheet 2 - Found missing record {code} via direct query (no filter)")
                        print(f"    ✓ Found missing record: {code} via direct query (no filter)")
                time.sleep(0.5)  # Small delay between queries
    
    if found_records:
        print(f"  ✓ Found {len(found_records)} missing records!")
    else:
        print(f"  ✗ Could not find missing records using standard methods")
        print(f"  📊 Analysis:")
        print(f"     - Total fetched: {len(existing_data)} records")
        print(f"     - API reported: {total_rows_from_api} records")
        print(f"     - Difference: {expected_missing} records")
        print(f"  💡 Possible reasons:")
        print(f"     1. API's totalRows is inaccurate (calculated incorrectly)")
        print(f"     2. {expected_missing} records were deleted/removed after API calculated totalRows")
        print(f"     3. API has pagination bug that skips some records")
        print(f"     4. Records exist but are filtered/hidden by API")
    
    return found_records

# ==================== CODE VERIFICATION ====================

def check_notification_code_exists(notification_code, max_pages_to_check=100):
    """
    Check if a specific notification code exists in API
    Returns: (found, sheet_number) where sheet_number is 1, 2, or 0 (not found)
    """
    print(f"\n🔍 Checking notification code: {notification_code}")
    logger.info(f"Checking notification code: {notification_code}")
    
    # Check Sheet 1 (no filter)
    print("  Checking in Sheet 1 (no filter)...")
    for page in range(1, min(max_pages_to_check, 100) + 1):  # Limit to 100 pages for performance
        page_data = get_api_data_sheet1(max_result=100, page_number=page)
        codes = [item.get('notificationCode', '') for item in page_data['data']]
        if notification_code in codes:
            logger.info(f"✓ Found {notification_code} in Sheet 1, page {page}")
            print(f"  ✓ Found in Sheet 1, page {page}")
            return (True, 1)
        if not page_data['data']:  # Empty page, stop
            break
    
    # Check Sheet 2 (with filter)
    print("  Checking in Sheet 2 (with filter)...")
    for page in range(1, min(max_pages_to_check, 20) + 1):  # Limit to 20 pages for performance
        page_data = get_api_data_sheet2(max_result=100, page_number=page)
        codes = [item.get('notificationCode', '') for item in page_data['data']]
        if notification_code in codes:
            logger.info(f"✓ Found {notification_code} in Sheet 2, page {page}")
            print(f"  ✓ Found in Sheet 2, page {page}")
            return (True, 2)
        if not page_data['data']:  # Empty page, stop
            break
    
    logger.warning(f"✗ Notification code {notification_code} NOT FOUND in API")
    print(f"  ✗ NOT FOUND in API")
    return (False, 0)

# ==================== DATA PROCESSING ====================

def extract_sheet1_fields(data_list):
    # Extract required columns for Sheet 1: notificationCode, importTrack, rpCorporation, manufacturer, importer
    result = []
    for item in data_list:
        result.append({
            'nameCosmeticHeb': item.get('nameCosmeticHeb', ''),
            'nameCosmeticEng': item.get('nameCosmeticEng', ''),
            'notificationCode': item.get('notificationCode', ''),
            'importTrack': item.get('importTrack', ''),
            'rpCorporation': item.get('rpCorporation', ''),
            'manufacturer': item.get('manufacturer', ''),
            'importer': item.get('importer', ''),
            'firstDate': item.get('firstDate', ''),
            'lastDate': item.get('lastDate', '')   
        })
    return result

def format_packages(packages_list):
    # Format packages: only get packageName, quantity, measurementDesc - format: "packagename quantity measurementDesc | packagename quantity measurementDesc"
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
    # Format shades: each color is a separate row, only color name - returns list of color names
    if not shades_list or not isinstance(shades_list, list):
        return []
    
    shade_names = []
    for idx, shade in enumerate(shades_list):
        if isinstance(shade, dict):
            shade_name = shade.get('shadeName', '')
            if shade_name:
                shade_names.append(shade_name)
        # Skip invalid shades
    
    return shade_names

def flatten_dict_for_sheet2(d, parent_key='', sep='_'):
    # Flatten nested dictionary for Sheet 2 with special handling for packages and shades - packages: format to string "name qty desc | name qty desc", shades: handled separately (each color one row)
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        
        if k == 'packages' and isinstance(v, list):
            # Handle packages specially
            items.append((new_key, format_packages(v)))
        elif k == 'shades' and isinstance(v, list):
            # Shades will be handled separately, don't flatten here - skip, don't add to items
            pass
        elif isinstance(v, dict):
            items.extend(flatten_dict_for_sheet2(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            # Convert list to string representation
            items.append((new_key, json.dumps(v, ensure_ascii=False)))
        else:
            items.append((new_key, v))
    return dict(items)

# ==================== GOOGLE SHEETS FUNCTIONS ====================

def setup_google_sheets_client():
    # Setup Google Sheets client with credentials
    if not os.path.exists(CREDENTIALS_FILE):
        print("\n" + "="*60)
        print("❌ ERROR: credentials.json not found")
        print("="*60)
        print("\nSetup Google Sheets API credentials:")
        print("1. Go to https://console.cloud.google.com/")
        print("2. Create new project or select existing project")
        print("3. Enable Google Sheets API and Google Drive API")
        print("4. Create Service Account and download JSON key")
        print("5. Rename JSON file to 'credentials.json' and place in this directory")
        print("6. Share Google Sheet with Service Account email")
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

def create_google_sheet_example(use_sample_data=True, spreadsheet_id=None):
    # Create sample Google Sheet with 2 sheets - use_sample_data=True: get 10 sample records for quick test, use_sample_data=False: get all data (takes time), spreadsheet_id: if provided, update existing sheet instead of creating new
    print("="*60)
    print("CREATE SAMPLE GOOGLE SHEET")
    print("="*60)
    
    # Setup Google Sheets client
    client = setup_google_sheets_client()
    if not client:
        return None
    
    # If spreadsheet_id provided, use existing sheet
    if spreadsheet_id:
        try:
            print(f"\nOpening existing Google Sheet...")
            spreadsheet = client.open_by_key(spreadsheet_id)
            print(f"✓ Opened sheet: {spreadsheet.url}")
        except Exception as e:
            print(f"❌ Cannot open sheet with ID: {spreadsheet_id}")
            print(f"Error: {e}")
            return None
    else:
        # Create new Google Sheet
        sheet_name = f"Cosmetics Data Example - {datetime.now().strftime('%Y%m%d_%H%M%S')}"
        print(f"\nCreating Google Sheet: {sheet_name}...")
        
        try:
            spreadsheet = client.create(sheet_name)
            # Make it readable by anyone with link
            spreadsheet.share('', perm_type='anyone', role='reader')
            
            print(f"✓ Created sheet: {spreadsheet.url}")
        except Exception as e:
            error_msg = str(e)
            if 'storageQuotaExceeded' in error_msg or 'quota' in error_msg.lower():
                print("\n" + "="*60)
                print("❌ ERROR: Google Drive storage quota exceeded!")
                print("="*60)
                print("\n💡 SOLUTION:")
                print("1. Create Google Sheet manually:")
                print("   - Go to https://sheets.google.com")
                print("   - Create new sheet")
                print("   - Share with Service Account email:")
                print(f"     {Credentials.from_service_account_file(CREDENTIALS_FILE).service_account_email}")
                print("   - Copy Spreadsheet ID from URL (between /d/ and /edit)")
                print("   - Run: python main.py create --id <spreadsheet_id>")
                print("\n2. Or delete some files in Google Drive to free up space")
                return None
            else:
                raise
    
    try:
        # Get data from API
        print()
        if use_sample_data:
            print("Fetching sample data (10 records)...")
            data_sheet1 = get_api_data_sheet1(max_result=10, page_number=1)['data']
            data_sheet2 = get_api_data_sheet2(max_result=10, page_number=1)['data']
        else:
            print("Fetching all data (may take time)...")
            data_sheet1 = get_all_pages_sheet1(max_result=100)
            data_sheet2 = get_all_pages_sheet2(max_result=100)
        
        # Create Sheet 1 - filtered columns
        print("\nCreating Sheet 1 (filtered columns)...")
        
        # Check if sheet already exists
        try:
            worksheet1 = spreadsheet.worksheet("כל המוצרים")
            worksheet1.clear()  # Clear if exists
        except:
            worksheet1 = spreadsheet.sheet1
            worksheet1.update_title("כל המוצרים")
        
        # Extract required columns
        sheet1_data = extract_sheet1_fields(data_sheet1)
        
        # Headers in order: notificationCode, importTrack, rpCorporation, manufacturer, importer, firstDate, lastDate
        headers1 = ['nameCosmeticHeb', 'nameCosmeticEng', 'notificationCode', 'importTrack', 'rpCorporation', 'manufacturer', 'importer', 'firstDate', 'lastDate']
        
        # Prepare all rows for batch write
        all_rows = [headers1]  # Header row
        for item in sheet1_data:
            row = [
                item.get('nameCosmeticHeb', ''),
                item.get('nameCosmeticEng', ''),
                item.get('notificationCode', ''),
                item.get('importTrack', ''),
                item.get('rpCorporation', ''),
                item.get('manufacturer', ''),
                item.get('importer', ''),
                item.get('firstDate', ''),  # ADDED
                item.get('lastDate', '')    # ADDED
            ]
            all_rows.append(row)
        
        
        
        # Write batch to avoid rate limit
        batch_size = SHEETS_BATCH_SIZE
        total_batches = math.ceil(len(all_rows) / batch_size)
        for i in range(0, len(all_rows), batch_size):
            batch = all_rows[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            batch_name = f"Sheet 1 - Batch {batch_num}/{total_batches}"
            print(f"  Writing {batch_name} ({len(batch)} rows)...")
            append_rows_with_retry(worksheet1, batch, batch_name)
            
            # Add delay between batches to avoid rate limiting
            if i + batch_size < len(all_rows):  # Don't delay after last batch
                time.sleep(SHEETS_BATCH_DELAY)
        
        print(f"✓ Sheet 1: {len(sheet1_data)} rows")
        
        # Create Sheet 2 - all columns
        print("\nCreating Sheet 2 (all columns)...")
        
        # Check if sheet already exists
        try:
            worksheet2 = spreadsheet.worksheet("גלי עמיר בעמ")
            worksheet2.clear()  # Clear if exists
        except:
            worksheet2 = spreadsheet.add_worksheet(title="גלי עמיר בעמ", rows=1000, cols=50)
        
        if data_sheet2:
            # Process data with special handling for packages and shades
            all_rows2 = []
            
            # Create headers in correct order
            first_item = data_sheet2[0]
            flattened_first = flatten_dict_for_sheet2(first_item)
            
            # Order: notificationCode, importTrack, rpCorporation, manufacturer, importer, ... (other fields)
            base_headers = ['notificationCode', 'importTrack', 'rpCorporation', 'manufacturer', 'importer']
            other_headers = [k for k in flattened_first.keys() if k not in base_headers]
            # Add 'shades' and 'shades2' to headers if not exists - shades: all shades joined by | (first row), shades2: individual shades (subsequent rows)
            if 'shades' not in other_headers:
                other_headers.append('shades')
            if 'shades2' not in other_headers:
                other_headers.append('shades2')
            headers2 = base_headers + other_headers
            
            # Add header row
            all_rows2.append(headers2)
            
            # Process each item
            for idx, item in enumerate(data_sheet2):
                flattened_item = flatten_dict_for_sheet2(item)
                
                # Process shades: each color is a separate row
                shades = item.get('shades', [])
                shade_names = format_shades(shades)
                
                if shade_names:
                    # Create first row: main product with all shades joined by |
                    all_shades_str = " | ".join(shade_names)
                    row = []
                    for h in headers2:
                        if h == 'shades':
                            row.append(all_shades_str)  # All shades joined by |
                        elif h == 'shades2':
                            row.append('')  # Empty in first row
                        else:
                            row.append(flattened_item.get(h, ''))
                    all_rows2.append(row)
                    
                    # Create subsequent rows: each shade in separate row
                    for shade_name in shade_names:
                        row = []
                        for h in headers2:
                            if h == 'shades':
                                row.append('')  # Empty in shade rows
                            elif h == 'shades2':
                                row.append(shade_name)  # Individual shade
                            else:
                                row.append(flattened_item.get(h, ''))
                        all_rows2.append(row)
                else:
                    # If no shades, create 1 row with both columns empty
                    row = []
                    for h in headers2:
                        if h == 'shades' or h == 'shades2':
                            row.append('')  # Empty string when no shades
                        else:
                            row.append(flattened_item.get(h, ''))
                    all_rows2.append(row)
            
            # Write batch to avoid rate limit
            batch_size = SHEETS_BATCH_SIZE
            total_batches = math.ceil(len(all_rows2) / batch_size)
            for i in range(0, len(all_rows2), batch_size):
                batch = all_rows2[i:i + batch_size]
                batch_num = (i // batch_size) + 1
                batch_name = f"Sheet 2 - Batch {batch_num}/{total_batches}"
                print(f"  Writing {batch_name} ({len(batch)} rows)...")
                append_rows_with_retry(worksheet2, batch, batch_name)
                
                # Add delay between batches to avoid rate limiting
                if i + batch_size < len(all_rows2):  # Don't delay after last batch
                    time.sleep(SHEETS_BATCH_DELAY)
            
            # Actual row count = total rows (including header) - 1 header row
            total_rows = len(all_rows2) - 1
            print(f"✓ Sheet 2: {total_rows} rows (from {len(data_sheet2)} items) with {len(headers2)} columns")
        else:
            print("⚠ No data for Sheet 2")
        
        # Completed
        print("\n" + "="*60)
        print("✅ COMPLETED!")
        print(f"📊 Google Sheet URL: {spreadsheet.url}")
        print("="*60)
        
        return spreadsheet.url
        
    except Exception as e:
        print(f"\n❌ Error creating Google Sheet: {e}")
        print("\nCheck:")
        print("1. Is credentials.json correct?")
        print("2. Has Service Account been shared with Google Sheet?")
        print("3. Are Google Sheets API and Google Drive API enabled?")
        import traceback
        traceback.print_exc()
        return None

def append_rows_with_retry(worksheet, rows, batch_name="batch"):
    """
    Append rows to Google Sheet with retry logic and exponential backoff.
    Handles 502 errors and rate limiting.
    """
    import gspread.exceptions
    
    for attempt in range(SHEETS_MAX_RETRIES):
        try:
            worksheet.append_rows(rows)
            logger.info(f"✓ {batch_name}: Successfully appended {len(rows)} rows")
            return True
        except gspread.exceptions.APIError as e:
            error_str = str(e)
            # Check if it's a 502 error or rate limit error
            if "502" in error_str or "rate limit" in error_str.lower() or "quota" in error_str.lower():
                if attempt < SHEETS_MAX_RETRIES - 1:
                    # Exponential backoff: 5s, 10s, 20s, 40s, 80s
                    delay = SHEETS_RETRY_DELAY * (2 ** attempt)
                    logger.warning(f"⚠ {batch_name}: Google Sheets API error (attempt {attempt + 1}/{SHEETS_MAX_RETRIES}): {error_str[:100]}")
                    logger.info(f"  Retrying in {delay} seconds...")
                    print(f"⚠ {batch_name}: API error, retrying in {delay}s... (attempt {attempt + 1}/{SHEETS_MAX_RETRIES})")
                    time.sleep(delay)
                    continue
                else:
                    logger.error(f"❌ {batch_name}: Failed after {SHEETS_MAX_RETRIES} attempts: {error_str[:200]}")
                    raise
            else:
                # Other errors, don't retry
                logger.error(f"❌ {batch_name}: Non-retryable error: {error_str[:200]}")
                raise
        except Exception as e:
            # Other exceptions, don't retry
            logger.error(f"❌ {batch_name}: Unexpected error: {str(e)[:200]}")
            raise
    
    return False

def update_existing_sheet(spreadsheet_id=None):
    # Update data to existing Google Sheet - used for automation, runs monthly
    if spreadsheet_id is None:
        spreadsheet_id = SPREADSHEET_ID
    
    print("="*60)
    print("UPDATE GOOGLE SHEET")
    print("="*60)
    
    logger.info("Starting Google Sheet update")
    
    client = setup_google_sheets_client()
    if not client:
        logger.error("Cannot setup Google Sheets client")
        return False
    
    try:
        spreadsheet = client.open_by_key(spreadsheet_id)
        print(f"✓ Opened spreadsheet: {spreadsheet.url}")
        
        # Get all data from API
        print("\nFetching all data from API (may take time)...")
        data_sheet1 = get_all_pages_sheet1()
        data_sheet2 = get_all_pages_sheet2()
        
        # ✅ Compare notification codes between sheets
        codes_sheet1 = set([item.get('notificationCode', '') for item in data_sheet1])
        codes_sheet2 = set([item.get('notificationCode', '') for item in data_sheet2])
        
        print(f"\n📊 Comparing notification codes between sheets...")
        print(f"   Sheet 1: {len(codes_sheet1)} unique codes")
        print(f"   Sheet 2: {len(codes_sheet2)} unique codes")
        
        # ✅ CRITICAL FIX: Find codes in Sheet 2 but not in Sheet 1, then query and add them
        missing_in_sheet1 = codes_sheet2 - codes_sheet1
        print(f"   Codes in Sheet 2 but NOT in Sheet 1: {len(missing_in_sheet1)}")
        
        if missing_in_sheet1:
            logger.warning(f"Found {len(missing_in_sheet1)} notification codes in Sheet 2 but not in Sheet 1")
            print(f"⚠ WARNING: Found {len(missing_in_sheet1)} notification codes in Sheet 2 but not in Sheet 1")
            print(f"   Querying missing codes from API to add to Sheet 1...")
            
            added_to_sheet1 = []
            for code in missing_in_sheet1:
                print(f"  🔍 Querying code {code} from API (no filter)...")
                # Query directly without filter (Sheet 1 has no filter)
                record = get_api_data_by_notification_code(code, use_filter=False)
                if record:
                    # Check if not already added (avoid duplicates)
                    if code not in codes_sheet1:
                        data_sheet1.append(record)
                        codes_sheet1.add(code)
                        added_to_sheet1.append(code)
                        logger.info(f"Added code {code} to Sheet 1 from API (no filter)")
                        print(f"    ✓ Added code {code} to Sheet 1")
                    else:
                        print(f"    ⚠ Code {code} already exists in Sheet 1 (duplicate check)")
                else:
                    logger.warning(f"Code {code} not found in API (no filter) - may have been deleted")
                    print(f"    ✗ Code {code} not found in API")
                time.sleep(0.3)  # Small delay between queries
            
            if added_to_sheet1:
                print(f"  ✓ Added {len(added_to_sheet1)} missing records to Sheet 1: {added_to_sheet1[:10]}...")
                logger.info(f"Added {len(added_to_sheet1)} missing records to Sheet 1: {added_to_sheet1}")
                # Update data_sheet1 count
                original_count = len(data_sheet1) - len(added_to_sheet1)
                print(f"  📊 Sheet 1 now has {len(data_sheet1)} records (was {original_count})")
            else:
                print(f"  ℹ No records were added (they may have been deleted from API or already exist)")
        else:
            print(f"  ✅ All codes in Sheet 2 are already in Sheet 1!")
            logger.info("All codes in Sheet 2 are already in Sheet 1 - no missing records")
        
        # Find codes in Sheet 1 but not in Sheet 2
        missing_in_sheet2 = codes_sheet1 - codes_sheet2
        if missing_in_sheet2:
            logger.info(f"Found {len(missing_in_sheet2)} notification codes in Sheet 1 but not in Sheet 2")
            print(f"ℹ INFO: Found {len(missing_in_sheet2)} notification codes in Sheet 1 but not in Sheet 2")
            print(f"   This is EXPECTED because Sheet 2 has filter (businessNotificationItemId: 34, businessTypeNotificationId: 5)")
            print(f"   Sheet 2 only shows records matching the filter criteria.")
        
        # Check for specific missing codes mentioned by client
        client_missing_codes = ['2042025160147', '1742025091730', '1742025093606', '2042025153631']
        print(f"\n🔍 Checking specific notification codes mentioned by client...")
        for code in client_missing_codes:
            found_in_sheet1 = code in codes_sheet1
            found_in_sheet2 = code in codes_sheet2
            
            if found_in_sheet1 and found_in_sheet2:
                logger.info(f"✓ Notification code {code} found in BOTH sheets")
                print(f"✓ Notification code {code} found in BOTH sheets")
            elif found_in_sheet1:
                logger.warning(f"⚠ Notification code {code} found in Sheet 1 but NOT in Sheet 2 (filtered out)")
                print(f"⚠ Notification code {code} found in Sheet 1 but NOT in Sheet 2")
                print(f"   This is expected - Sheet 2 has filter that excludes this code")
            elif found_in_sheet2:
                logger.warning(f"⚠ Notification code {code} found in Sheet 2 but NOT in Sheet 1 (unexpected)")
                print(f"⚠ Notification code {code} found in Sheet 2 but NOT in Sheet 1")
            else:
                # Not found in scraped data, check API directly
                logger.warning(f"✗ Notification code {code} NOT FOUND in scraped data. Checking API directly...")
                print(f"✗ Notification code {code} NOT FOUND in scraped data. Checking API directly...")
                found, sheet_num = check_notification_code_exists(code, max_pages_to_check=50)
                if found:
                    logger.error(f"CRITICAL: Code {code} EXISTS in API (Sheet {sheet_num}) but was NOT scraped!")
                    print(f"❌ CRITICAL: Code {code} EXISTS in API but was NOT scraped!")
                    print(f"   This indicates a scraping issue - the code exists but wasn't fetched.")
                else:
                    logger.warning(f"Code {code} does NOT exist in API - it may have been deleted or never existed")
                    print(f"ℹ Code {code} does NOT exist in API - it may have been deleted or never existed")
                    print(f"   Please verify on the website: https://registries.health.gov.il/Cosmetics")
        
        # Update Sheet 1
        worksheet1 = spreadsheet.worksheet("כל המוצרים")
        sheet1_data = extract_sheet1_fields(data_sheet1)
        
        # Prepare all rows for batch write
        headers1 = ['nameCosmeticHeb', 'nameCosmeticEng', 'notificationCode', 'importTrack', 'rpCorporation', 'manufacturer', 'importer', 'firstDate', 'lastDate']
        all_rows = [headers1]  # Header row
        for item in sheet1_data:
            row = [
                item.get('nameCosmeticHeb', ''),
                item.get('nameCosmeticEng', ''),
                item.get('notificationCode', ''),
                item.get('importTrack', ''),
                item.get('rpCorporation', ''),
                item.get('manufacturer', ''),
                item.get('importer', ''),
                item.get('firstDate', ''),  # ADDED
                item.get('lastDate', '')    # ADDED
            ]
            all_rows.append(row)





        # Clear old data first
        print(f"  Clearing old data from Sheet 1...")
        worksheet1.clear()
        
        # ✅ FIX: Resize sheet to exact number of rows needed (no buffer to avoid leftover data)
        total_rows_needed = len(all_rows)
        print(f"  Resizing Sheet 1 to {total_rows_needed} rows...")
        try:
            worksheet1.resize(rows=total_rows_needed, cols=len(headers1))
            logger.info(f"Resized Sheet 1 to {total_rows_needed} rows")
        except Exception as e:
            logger.warning(f"Could not resize Sheet 1: {e}. Continuing anyway...")
        
        # ✅ FIX: Use update() instead of append_rows() for better reliability with large datasets
        # Write in batches using update() with A1 notation
        batch_size = SHEETS_BATCH_SIZE
        total_batches = math.ceil(len(all_rows) / batch_size)
        
        for i in range(0, len(all_rows), batch_size):
            batch = all_rows[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            batch_name = f"Sheet 1 - Batch {batch_num}/{total_batches}"
            
            # Calculate range for this batch (A1 notation)
            start_row = i + 1  # +1 because A1 notation is 1-based
            end_row = min(i + batch_size, len(all_rows))
            range_name = f"A{start_row}:G{end_row}"  # G is column 7 (nameCosmeticHeb to importer)
            
            print(f"  Writing {batch_name} ({len(batch)} rows) to range {range_name}...")
            
            try:
                worksheet1.update(range_name, batch, value_input_option='RAW')
                logger.info(f"✓ {batch_name}: Successfully updated {len(batch)} rows at {range_name}")
            except Exception as e:
                error_str = str(e)
                if "502" in error_str or "rate limit" in error_str.lower() or "quota" in error_str.lower():
                    # Retry with exponential backoff
                    for attempt in range(SHEETS_MAX_RETRIES):
                        delay = SHEETS_RETRY_DELAY * (2 ** attempt)
                        logger.warning(f"⚠ {batch_name}: Google Sheets API error (attempt {attempt + 1}/{SHEETS_MAX_RETRIES}): {error_str[:100]}")
                        print(f"⚠ {batch_name}: API error, retrying in {delay}s... (attempt {attempt + 1}/{SHEETS_MAX_RETRIES})")
                        time.sleep(delay)
                        try:
                            worksheet1.update(range_name, batch, value_input_option='RAW')
                            logger.info(f"✓ {batch_name}: Successfully updated after retry")
                            break
                        except Exception as retry_e:
                            if attempt == SHEETS_MAX_RETRIES - 1:
                                logger.error(f"❌ {batch_name}: Failed after {SHEETS_MAX_RETRIES} attempts")
                                raise
                else:
                    logger.error(f"❌ {batch_name}: Non-retryable error: {error_str[:200]}")
                    raise
            
            # Add delay between batches to avoid rate limiting
            if i + batch_size < len(all_rows):  # Don't delay after last batch
                time.sleep(SHEETS_BATCH_DELAY)
        
        # ✅ FIX: Resize back to exact number of rows to remove any leftover rows
        try:
            worksheet1.resize(rows=len(all_rows), cols=len(headers1))
            logger.info(f"Final resize Sheet 1 to {len(all_rows)} rows to remove leftover data")
        except Exception as e:
            logger.warning(f"Could not final resize Sheet 1: {e}. Continuing anyway...")
        
        print(f"✓ Updated Sheet 1: {len(sheet1_data)} rows")
        logger.info(f"Updated Sheet 1: {len(sheet1_data)} rows")
        
        # Update Sheet 2
        worksheet2 = spreadsheet.worksheet("גלי עמיר בעמ")
        
        if data_sheet2:
            
            # Process data with special handling for packages and shades
            all_rows2 = []
            
            # Create headers in correct order - get all fields from first item
            first_item = data_sheet2[0]
            flattened_first = flatten_dict_for_sheet2(first_item)
            
            # Create headers in order: notificationCode, importTrack, rpCorporation, manufacturer, importer, ... (other fields)
            base_headers = ['notificationCode', 'importTrack', 'rpCorporation', 'manufacturer', 'importer']
            other_headers = [k for k in flattened_first.keys() if k not in base_headers]
            # Add 'shades' and 'shades2' to headers if not exists - shades: all shades joined by | (first row), shades2: individual shades (subsequent rows)
            if 'shades' not in other_headers:
                other_headers.append('shades')
            if 'shades2' not in other_headers:
                other_headers.append('shades2')
            headers2 = base_headers + other_headers
            
            # Add header row
            all_rows2.append(headers2)
            
            # Process each item
            for idx, item in enumerate(data_sheet2):
                flattened_item = flatten_dict_for_sheet2(item)
                
                # Process shades: each color is a separate row
                shades = item.get('shades', [])
                shade_names = format_shades(shades)
                
                if shade_names:
                    # Create first row: main product with all shades joined by |
                    all_shades_str = " | ".join(shade_names)
                    row = []
                    for h in headers2:
                        if h == 'shades':
                            row.append(all_shades_str)  # All shades joined by |
                        elif h == 'shades2':
                            row.append('')  # Empty in first row
                        else:
                            row.append(flattened_item.get(h, ''))
                    all_rows2.append(row)
                    
                    # Create subsequent rows: each shade in separate row
                    for shade_name in shade_names:
                        row = []
                        for h in headers2:
                            if h == 'shades':
                                row.append('')  # Empty in shade rows
                            elif h == 'shades2':
                                row.append(shade_name)  # Individual shade
                            else:
                                row.append(flattened_item.get(h, ''))
                        all_rows2.append(row)
                else:
                    # If no shades, create 1 row with both columns empty
                    row = []
                    for h in headers2:
                        if h == 'shades' or h == 'shades2':
                            row.append('')  # Empty string when no shades
                        else:
                            row.append(flattened_item.get(h, ''))
                    all_rows2.append(row)
            
            # Clear old data first
            print(f"  Clearing old data from Sheet 2...")
            worksheet2.clear()
            
            # ✅ FIX: Resize sheet to exact number of rows needed (no buffer to avoid leftover data)
            total_rows_needed = len(all_rows2)
            num_cols = len(headers2)
            print(f"  Resizing Sheet 2 to {total_rows_needed} rows, {num_cols} columns...")
            try:
                worksheet2.resize(rows=total_rows_needed, cols=num_cols)
                logger.info(f"Resized Sheet 2 to {total_rows_needed} rows, {num_cols} columns")
            except Exception as e:
                logger.warning(f"Could not resize Sheet 2: {e}. Continuing anyway...")
            
            # ✅ FIX: Use update() instead of append_rows() for better reliability with large datasets
            # Write in batches using update() with A1 notation
            batch_size = SHEETS_BATCH_SIZE
            total_batches = math.ceil(len(all_rows2) / batch_size)
            
            # Calculate last column letter (A=1, B=2, ..., Z=26, AA=27, etc.)
            def get_column_letter(col_num):
                """Convert column number to letter (1->A, 2->B, ..., 27->AA)"""
                result = ""
                while col_num > 0:
                    col_num -= 1
                    result = chr(65 + (col_num % 26)) + result
                    col_num //= 26
                return result
            
            last_col_letter = get_column_letter(num_cols)
            
            for i in range(0, len(all_rows2), batch_size):
                batch = all_rows2[i:i + batch_size]
                batch_num = (i // batch_size) + 1
                batch_name = f"Sheet 2 - Batch {batch_num}/{total_batches}"
                
                # Calculate range for this batch (A1 notation)
                start_row = i + 1  # +1 because A1 notation is 1-based
                end_row = min(i + batch_size, len(all_rows2))
                range_name = f"A{start_row}:{last_col_letter}{end_row}"
                
                print(f"  Writing {batch_name} ({len(batch)} rows) to range {range_name}...")
                
                try:
                    worksheet2.update(range_name, batch, value_input_option='RAW')
                    logger.info(f"✓ {batch_name}: Successfully updated {len(batch)} rows at {range_name}")
                except Exception as e:
                    error_str = str(e)
                    if "502" in error_str or "rate limit" in error_str.lower() or "quota" in error_str.lower():
                        # Retry with exponential backoff
                        for attempt in range(SHEETS_MAX_RETRIES):
                            delay = SHEETS_RETRY_DELAY * (2 ** attempt)
                            logger.warning(f"⚠ {batch_name}: Google Sheets API error (attempt {attempt + 1}/{SHEETS_MAX_RETRIES}): {error_str[:100]}")
                            print(f"⚠ {batch_name}: API error, retrying in {delay}s... (attempt {attempt + 1}/{SHEETS_MAX_RETRIES})")
                            time.sleep(delay)
                            try:
                                worksheet2.update(range_name, batch, value_input_option='RAW')
                                logger.info(f"✓ {batch_name}: Successfully updated after retry")
                                break
                            except Exception as retry_e:
                                if attempt == SHEETS_MAX_RETRIES - 1:
                                    logger.error(f"❌ {batch_name}: Failed after {SHEETS_MAX_RETRIES} attempts")
                                    raise
                    else:
                        logger.error(f"❌ {batch_name}: Non-retryable error: {error_str[:200]}")
                        raise
                
                # Add delay between batches to avoid rate limiting
                if i + batch_size < len(all_rows2):  # Don't delay after last batch
                    time.sleep(SHEETS_BATCH_DELAY)
            
            # ✅ FIX: Resize back to exact number of rows to remove any leftover rows
            try:
                worksheet2.resize(rows=len(all_rows2), cols=num_cols)
                logger.info(f"Final resize Sheet 2 to {len(all_rows2)} rows to remove leftover data")
            except Exception as e:
                logger.warning(f"Could not final resize Sheet 2: {e}. Continuing anyway...")
            
            # Actual row count = total rows (including header) - 1 header row
            total_rows = len(all_rows2) - 1
            print(f"✓ Updated Sheet 2: {total_rows} rows (from {len(data_sheet2)} items)")
            logger.info(f"Updated Sheet 2: {total_rows} rows (from {len(data_sheet2)} items)")
        else:
            logger.warning("No data for Sheet 2")
        
        print("\n✅ Update completed!")
        logger.info("Google Sheet update completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Error updating Google Sheet: {e}", exc_info=True)
        print(f"\n❌ Error updating Google Sheet: {e}")
        import traceback
        traceback.print_exc()
        return False

# ==================== MAIN ====================

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        command = sys.argv[1]
        
        if command == "test":
            # Test API calls
            print("Testing API calls...")
            data1 = get_api_data_sheet1(max_result=5, page_number=1)
            data2 = get_api_data_sheet2(max_result=5, page_number=1)
            print(f"\nSheet 1 - Total rows: {data1['totalRows']}, Records: {len(data1['data'])}")
            print(f"Sheet 2 - Total rows: {data2['totalRows']}, Records: {len(data2['data'])}")
        else:
            print("❌ Invalid command")
            print("\nUsage:")
            print("  python main.py        # Update Google Sheet with all data")
            print("  python main.py test   # Test API calls")
    else:
        # Default: update Google Sheet with all data
        if not SPREADSHEET_ID:
            print("❌ ERROR: SPREADSHEET_ID not set in main.py")
            sys.exit(1)
        
        update_existing_sheet()
