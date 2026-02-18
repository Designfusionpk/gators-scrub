"""
Simple SQLite database - NO SQLAlchemy to avoid compatibility issues
"""
import sqlite3
import json
from datetime import datetime
import os
import pandas as pd

DB_PATH = 'data/scrubber.db'

def init_database():
    """Initialize database tables"""
    os.makedirs('data', exist_ok=True)
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Users table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password TEXT NOT NULL,
            email TEXT,
            company TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            is_active INTEGER DEFAULT 1,
            total_scrubbed INTEGER DEFAULT 0
        )
    ''')
    
    # Scrub batches table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS scrub_batches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id TEXT UNIQUE NOT NULL,
            user_id INTEGER,
            username TEXT,
            filename TEXT,
            total_numbers INTEGER DEFAULT 0,
            processed_numbers INTEGER DEFAULT 0,
            good_count INTEGER DEFAULT 0,
            bad_count INTEGER DEFAULT 0,
            error_count INTEGER DEFAULT 0,
            start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            end_time TIMESTAMP,
            status TEXT DEFAULT 'PENDING'
        )
    ''')
    
    # Scrubbed numbers table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS scrubbed_numbers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id TEXT,
            user_id INTEGER,
            phone_number TEXT,
            original_number TEXT,
            status TEXT,
            area_code TEXT,
            scrub_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Create indexes for performance
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_batch_id ON scrubbed_numbers(batch_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_user_id ON scrubbed_numbers(user_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_status ON scrubbed_numbers(status)')
    
    conn.commit()
    conn.close()
    
    print(f"Database initialized at {DB_PATH}")

def hash_password(password):
    """Simple password hashing"""
    import hashlib
    return hashlib.sha256(password.encode()).hexdigest()

def create_user(username, password, email, company):
    """Create new user"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
            INSERT INTO users (username, password, email, company, created_at)
            VALUES (?, ?, ?, ?, ?)
        ''', (username, hash_password(password), email, company, datetime.now()))
        conn.commit()
        success = True
        message = "User created successfully"
    except sqlite3.IntegrityError:
        success = False
        message = "Username already exists"
    except Exception as e:
        success = False
        message = f"Error: {str(e)}"
    
    conn.close()
    return success, message

def authenticate_user(username, password):
    """Verify user credentials"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT id FROM users 
        WHERE username = ? AND password = ? AND is_active = 1
    ''', (username, hash_password(password)))
    
    result = cursor.fetchone()
    conn.close()
    
    return result[0] if result else None

def create_batch(batch_id, user_id, username, filename):
    """Create new scrub batch"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute('''
        INSERT INTO scrub_batches (batch_id, user_id, username, filename, start_time, status)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', (batch_id, user_id, username, filename, datetime.now(), 'PROCESSING'))
    
    conn.commit()
    conn.close()

def update_batch_progress(batch_id, processed, good, bad, error):
    """Update batch progress"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute('''
        UPDATE scrub_batches 
        SET processed_numbers = ?, good_count = ?, bad_count = ?, error_count = ?
        WHERE batch_id = ?
    ''', (processed, good, bad, error, batch_id))
    
    conn.commit()
    conn.close()

def complete_batch(batch_id):
    """Mark batch as completed"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute('''
        UPDATE scrub_batches 
        SET status = 'COMPLETED', end_time = ?
        WHERE batch_id = ?
    ''', (datetime.now(), batch_id))
    
    conn.commit()
    conn.close()

def save_scrubbed_numbers(numbers_data):
    """Save scrubbed numbers in bulk"""
    if not numbers_data:
        return
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    for data in numbers_data:
        cursor.execute('''
            INSERT INTO scrubbed_numbers 
            (batch_id, user_id, phone_number, original_number, status, area_code, scrub_date)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            data['batch_id'],
            data['user_id'],
            data['phone_number'],
            data.get('original_number', data['phone_number']),
            data['status'],
            data['area_code'],
            datetime.now()
        ))
    
    conn.commit()
    conn.close()

def get_user_stats(user_id):
    """Get statistics for a user"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Total batches
    cursor.execute('SELECT COUNT(*) FROM scrub_batches WHERE user_id = ?', (user_id,))
    total_batches = cursor.fetchone()[0]
    
    # Total good numbers
    cursor.execute('SELECT SUM(good_count) FROM scrub_batches WHERE user_id = ?', (user_id,))
    total_good = cursor.fetchone()[0] or 0
    
    # Total bad numbers
    cursor.execute('SELECT SUM(bad_count) FROM scrub_batches WHERE user_id = ?', (user_id,))
    total_bad = cursor.fetchone()[0] or 0
    
    # Recent batches
    cursor.execute('''
        SELECT batch_id, filename, total_numbers, good_count, bad_count, status, start_time
        FROM scrub_batches 
        WHERE user_id = ? 
        ORDER BY start_time DESC 
        LIMIT 10
    ''', (user_id,))
    
    recent = cursor.fetchall()
    
    conn.close()
    
    return {
        'total_batches': total_batches,
        'total_good': int(total_good),
        'total_bad': int(total_bad),
        'recent_batches': recent
    }

def get_admin_stats():
    """Get global statistics for admin"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute('SELECT COUNT(*) FROM users')
    total_users = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(*) FROM scrub_batches')
    total_batches = cursor.fetchone()[0]
    
    cursor.execute('SELECT SUM(total_numbers) FROM scrub_batches')
    total_numbers = cursor.fetchone()[0] or 0
    
    cursor.execute('SELECT SUM(good_count) FROM scrub_batches')
    total_good = cursor.fetchone()[0] or 0
    
    cursor.execute('SELECT SUM(bad_count) FROM scrub_batches')
    total_bad = cursor.fetchone()[0] or 0
    
    cursor.execute('SELECT COUNT(*) FROM scrub_batches WHERE date(start_time) = date("now")')
    active_today = cursor.fetchone()[0]
    
    conn.close()
    
    return {
        'total_users': total_users,
        'total_batches': total_batches,
        'total_numbers': int(total_numbers),
        'total_good': int(total_good),
        'total_bad': int(total_bad),
        'active_today': active_today
    }

def get_all_users():
    """Get all users for admin"""
    conn = sqlite3.connect(DB_PATH)
    
    query = '''
        SELECT id, username, email, company, created_at, is_active, total_scrubbed
        FROM users
        ORDER BY created_at DESC
    '''
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    return df

def get_all_batches(limit=100):
    """Get all batches for admin"""
    conn = sqlite3.connect(DB_PATH)
    
    query = f'''
        SELECT batch_id, username, filename, total_numbers, good_count, bad_count, 
               status, start_time, end_time
        FROM scrub_batches
        ORDER BY start_time DESC
        LIMIT {limit}
    '''
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    return df

def get_batch_numbers(batch_id):
    """Get scrubbed numbers for a batch"""
    conn = sqlite3.connect(DB_PATH)
    
    query = '''
        SELECT phone_number, status, area_code, scrub_date
        FROM scrubbed_numbers
        WHERE batch_id = ?
        ORDER BY 
            CASE WHEN status = 'BAD' THEN 1 ELSE 2 END,
            phone_number
    '''
    
    df = pd.read_sql_query(query, conn, params=(batch_id,))
    conn.close()
    
    return df

def update_user_total(user_id, increment):
    """Update user's total scrubbed count"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute('''
        UPDATE users 
        SET total_scrubbed = total_scrubbed + ?
        WHERE id = ?
    ''', (increment, user_id))
    
    conn.commit()
    conn.close()