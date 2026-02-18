"""
Gators Scrub - BLA API Integration - ULTRA FAST VERSION
Optimized for maximum processing speed
"""
import requests
import time
import streamlit as st
from dotenv import load_dotenv
import os
import re
import zipfile
import io
import sqlite3
from datetime import datetime
import concurrent.futures
from threading import Lock

load_dotenv()

BLA_BULK_URL = "https://api.blacklistalliance.net/bulk/upload"
DB_PATH = 'data/gators.db'

# Progress tracking lock
progress_lock = Lock()

def get_api_key(user_id=None):
    """Get API key - user's custom key or default from env"""
    if user_id:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute('SELECT api_key FROM users WHERE id = ?', (user_id,))
        result = cursor.fetchone()
        conn.close()
        if result and result[0]:
            return result[0].strip()
    
    # Fallback to env var
    return os.getenv('BLA_API_KEY', '').strip()

def save_api_key(user_id, api_key):
    """Save user's custom API key"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('UPDATE users SET api_key = ? WHERE id = ?', (api_key, user_id))
    conn.commit()
    conn.close()

def test_api_connection(api_key=None):
    """Test BLA API connection with provided key"""
    if not api_key:
        return False, "❌ No API key provided"
    
    try:
        test_phones = ["5551234567"]
        boundary = "----WebKitFormBoundary" + str(time.time()).replace('.', '')
        
        body = f"--{boundary}\r\n"
        body += f'Content-Disposition: form-data; name="filetype"\r\n\r\n'
        body += f"phone\r\n"
        body += f"--{boundary}\r\n"
        body += f'Content-Disposition: form-data; name="file"; filename="test.txt"\r\n'
        body += f"Content-Type: text/plain\r\n\r\n"
        body += f"{chr(10).join(test_phones)}\r\n"
        body += f"--{boundary}--\r\n"
        
        url = f"{BLA_BULK_URL}?key={api_key}"
        headers = {
            "accept": "application/zip",
            "content-type": f"multipart/form-data; boundary={boundary}"
        }
        
        response = requests.post(url, data=body.encode('utf-8'), headers=headers, timeout=30)
        
        if response.status_code == 200:
            return True, "✅ API Key is valid and working!"
        elif response.status_code == 400:
            return True, "✅ API Key is valid (received expected response)"
        elif response.status_code == 401:
            return False, "❌ Invalid API key (401 Unauthorized)"
        else:
            return False, f"❌ API Error: {response.status_code}"
    except Exception as e:
        return False, f"❌ Connection Failed: {str(e)}"

def upload_chunk(args):
    """Upload a single chunk - for parallel processing"""
    chunk, api_key, chunk_num, total_chunks = args
    
    try:
        boundary = "----WebKitFormBoundary" + str(time.time()).replace('.', '') + str(chunk_num)
        file_content = "\n".join(chunk)
        
        body = f"--{boundary}\r\n"
        body += f'Content-Disposition: form-data; name="filetype"\r\n\r\n'
        body += f"phone\r\n"
        body += f"--{boundary}\r\n"
        body += f'Content-Disposition: form-data; name="file"; filename="phones_{chunk_num}.txt"\r\n'
        body += f"Content-Type: text/plain\r\n\r\n"
        body += f"{file_content}\r\n"
        body += f"--{boundary}--\r\n"
        
        headers = {
            "accept": "application/zip",
            "content-type": f"multipart/form-data; boundary={boundary}"
        }
        
        url = f"{BLA_BULK_URL}?key={api_key}"
        
        response = requests.post(
            url,
            data=body.encode('utf-8'),
            headers=headers,
            timeout=120  # Increased timeout for larger chunks
        )
        
        if response.status_code == 200:
            # Parse results
            chunk_results = parse_bla_zip_response(response.content, chunk)
            return chunk_results, chunk_num, total_chunks, None
        else:
            # On error, mark as UNKNOWN
            error_results = {phone: 'UNKNOWN' for phone in chunk}
            return error_results, chunk_num, total_chunks, f"HTTP {response.status_code}"
            
    except Exception as e:
        error_results = {phone: 'UNKNOWN' for phone in chunk}
        return error_results, chunk_num, total_chunks, str(e)

def bulk_blacklist_lookup_parallel(phone_numbers, api_key, progress_callback=None, chunk_size=2000, max_workers=3):
    """
    PARALLEL processing for maximum speed
    """
    results = {}
    
    if not phone_numbers or not api_key:
        for phone in phone_numbers:
            results[phone] = 'GOOD'
        return results
    
    try:
        # Split into chunks
        chunks = []
        for i in range(0, len(phone_numbers), chunk_size):
            chunk = phone_numbers[i:i+chunk_size]
            chunks.append(chunk)
        
        total_chunks = len(chunks)
        
        # Prepare arguments for parallel processing
        chunk_args = [(chunks[i], api_key, i+1, total_chunks) for i in range(total_chunks)]
        
        # Process chunks in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all chunks
            future_to_chunk = {executor.submit(upload_chunk, arg): arg for arg in chunk_args}
            
            completed = 0
            for future in concurrent.futures.as_completed(future_to_chunk):
                chunk_results, chunk_num, total, error = future.result()
                
                # Update results
                results.update(chunk_results)
                
                completed += 1
                
                # Update progress
                if progress_callback:
                    progress_callback(completed, total, f"Chunk {chunk_num}/{total}")
                
                if error:
                    print(f"Chunk {chunk_num} error: {error}")
        
        return results
        
    except Exception as e:
        print(f"Parallel processing error: {str(e)}")
        # Fallback to sequential
        return bulk_blacklist_lookup_sequential(phone_numbers, api_key, progress_callback)

def bulk_blacklist_lookup_sequential(phone_numbers, api_key, progress_callback=None, chunk_size=2000):
    """
    Sequential processing - fallback method
    """
    results = {}
    
    if not phone_numbers or not api_key:
        for phone in phone_numbers:
            results[phone] = 'GOOD'
        return results
    
    try:
        # Process in chunks
        total_chunks = (len(phone_numbers) + chunk_size - 1) // chunk_size
        
        for i in range(0, len(phone_numbers), chunk_size):
            chunk = phone_numbers[i:i+chunk_size]
            chunk_num = i // chunk_size + 1
            
            if progress_callback:
                progress_callback(chunk_num, total_chunks, f"Processing chunk {chunk_num}")
            
            # Upload chunk
            chunk_results, _, _, _ = upload_chunk((chunk, api_key, chunk_num, total_chunks))
            results.update(chunk_results)
            
            # Small delay to avoid rate limiting
            time.sleep(0.2)
    
    except Exception as e:
        print(f"Sequential processing error: {str(e)}")
        for phone in phone_numbers:
            results[phone] = 'UNKNOWN'
    
    return results

# Default to parallel for maximum speed
bulk_blacklist_lookup = bulk_blacklist_lookup_parallel

def parse_bla_zip_response(zip_content, original_phones):
    """
    Parse BLA ZIP response to identify GOOD and BAD numbers
    Optimized for speed
    """
    results = {}
    
    # Initialize all phones as GOOD by default
    for phone in original_phones:
        results[phone] = 'GOOD'
    
    try:
        zip_data = io.BytesIO(zip_content)
        with zipfile.ZipFile(zip_data, 'r') as zip_file:
            
            # Check federal DNC list (these are BAD)
            if 'federal_dnc.txt' in zip_file.namelist():
                with zip_file.open('federal_dnc.txt') as f:
                    content = f.read().decode('utf-8')
                    for line in content.split('\n'):
                        phone = line.strip()
                        if phone:
                            clean_phone = re.sub(r'\D', '', phone)
                            if len(clean_phone) >= 10:
                                clean_phone = clean_phone[-10:]
                                results[clean_phone] = 'BAD'
            
            # Numbers in all_clean.txt are confirmed GOOD
            if 'all_clean.txt' in zip_file.namelist():
                with zip_file.open('all_clean.txt') as f:
                    content = f.read().decode('utf-8')
                    for line in content.split('\n'):
                        phone = line.strip()
                        if phone:
                            clean_phone = re.sub(r'\D', '', phone)
                            if len(clean_phone) >= 10:
                                clean_phone = clean_phone[-10:]
                                if clean_phone not in results or results[clean_phone] != 'BAD':
                                    results[clean_phone] = 'GOOD'
            
    except Exception as e:
        print(f"Error parsing ZIP: {str(e)}")
    
    return results