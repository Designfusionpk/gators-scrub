"""
Gators Scrub - Ultimate Phone Number Scrubber
ULTRA-FAST with Parallel Processing
"""
import streamlit as st
import pandas as pd
import uuid
import time
from datetime import datetime
import os
import sys
import sqlite3
import re
import base64

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from utils.bla_api import *
from dotenv import load_dotenv

load_dotenv()

# Page configuration
st.set_page_config(
    page_title="Gators Scrub",
    page_icon="🐊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for Century Gothic font and styling
st.markdown("""
<style>
    /* Import Century Gothic */
    @import url('https://fonts.cdnfonts.com/css/century-gothic');
    
    /* Apply Century Gothic to all text */
    html, body, [class*="css"]  {
        font-family: 'Century Gothic', sans-serif !important;
    }
    
    /* Headers */
    h1, h2, h3, h4, h5, h6 {
        font-family: 'Century Gothic', sans-serif !important;
        font-weight: 600 !important;
    }
    
    /* Metric labels */
    .css-1xarl3l {
        font-family: 'Century Gothic', sans-serif !important;
    }
    
    /* Buttons */
    .stButton button {
        font-family: 'Century Gothic', sans-serif !important;
        font-weight: 600 !important;
    }
    
    /* Sidebar */
    .css-1d391kg {
        font-family: 'Century Gothic', sans-serif !important;
    }
    
    /* Success/Warning/Error boxes */
    .stAlert {
        font-family: 'Century Gothic', sans-serif !important;
    }
    
    /* Custom header with logo */
    .gators-header {
        display: flex;
        align-items: center;
        padding: 1rem;
        background: linear-gradient(90deg, #006341 0%, #FA4616 100%);
        border-radius: 10px;
        margin-bottom: 2rem;
        color: white;
    }
    
    .gators-header h1 {
        margin: 0;
        color: white;
        font-size: 2.5rem;
        font-weight: 700;
        text-shadow: 2px 2px 4px rgba(0,0,0,0.2);
    }
    
    .gators-header img {
        height: 60px;
        margin-right: 20px;
        filter: drop-shadow(2px 2px 4px rgba(0,0,0,0.2));
    }
    
    /* Stats cards */
    .stat-card {
        background: white;
        padding: 1.5rem;
        border-radius: 10px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        border-left: 5px solid #FA4616;
        margin-bottom: 1rem;
    }
    
    /* Bad number alert */
    .bad-alert {
        background: #ffebee;
        color: #c62828;
        padding: 1rem;
        border-radius: 10px;
        border-left: 5px solid #c62828;
        margin: 1rem 0;
        font-weight: 600;
    }
    
    /* API key section */
    .api-section {
        background: #f5f5f5;
        padding: 1.5rem;
        border-radius: 10px;
        margin: 1rem 0;
    }
    
    /* Speed settings panel */
    .speed-panel {
        background: #e8f5e9;
        padding: 1rem;
        border-radius: 10px;
        border-left: 5px solid #006341;
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)

# Initialize session state
if 'authenticated' not in st.session_state:
    st.session_state.authenticated = False
if 'username' not in st.session_state:
    st.session_state.username = None
if 'user_id' not in st.session_state:
    st.session_state.user_id = None
if 'processing' not in st.session_state:
    st.session_state.processing = False
if 'current_batch' not in st.session_state:
    st.session_state.current_batch = None
if 'api_key_valid' not in st.session_state:
    st.session_state.api_key_valid = False

# ============================================
# DATABASE FUNCTIONS - UPDATED WITH API KEY
# ============================================
DB_PATH = 'data/gators.db'

def init_database():
    """Initialize database with API key column"""
    os.makedirs('data', exist_ok=True)
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Users table with API key
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password TEXT NOT NULL,
            email TEXT,
            company TEXT,
            api_key TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            is_active INTEGER DEFAULT 1,
            total_scrubbed INTEGER DEFAULT 0
        )
    ''')
    
    # Check if api_key column exists (for older databases)
    cursor.execute("PRAGMA table_info(users)")
    columns = [col[1] for col in cursor.fetchall()]
    if 'api_key' not in columns:
        cursor.execute('ALTER TABLE users ADD COLUMN api_key TEXT')
    
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
            detailed_status TEXT,
            area_code TEXT,
            scrub_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Create indexes
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_batch_id ON scrubbed_numbers(batch_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_user_id ON scrubbed_numbers(user_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_status ON scrubbed_numbers(status)')
    
    # Create default admin user
    cursor.execute("SELECT COUNT(*) FROM users WHERE username = 'admin'")
    if cursor.fetchone()[0] == 0:
        admin_password = hash_password('admin123')
        cursor.execute('''
            INSERT INTO users (username, password, email, company, api_key, is_active)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', ('admin', admin_password, 'admin@gators.com', 'Gators Admin', '', 1))
    
    conn.commit()
    conn.close()

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
        message = "✅ Account created successfully!"
    except sqlite3.IntegrityError:
        success = False
        message = "❌ Username already exists"
    except Exception as e:
        success = False
        message = f"❌ Error: {str(e)}"
    
    conn.close()
    return success, message

def authenticate_user(username, password):
    """Verify user credentials"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT id, api_key FROM users 
        WHERE username = ? AND password = ? AND is_active = 1
    ''', (username, hash_password(password)))
    
    result = cursor.fetchone()
    conn.close()
    
    if result:
        st.session_state.user_api_key = result[1] if result[1] else ''
        return result[0]
    return None

def get_user_api_key(user_id):
    """Get user's API key"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('SELECT api_key FROM users WHERE id = ?', (user_id,))
    result = cursor.fetchone()
    conn.close()
    return result[0] if result and result[0] else ''

def update_user_api_key(user_id, api_key):
    """Update user's API key"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('UPDATE users SET api_key = ? WHERE id = ?', (api_key, user_id))
    conn.commit()
    conn.close()
    st.session_state.user_api_key = api_key

def create_batch(batch_id, user_id, username, filename, total_numbers):
    """Create new scrub batch"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO scrub_batches 
        (batch_id, user_id, username, filename, total_numbers, start_time, status)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', (batch_id, user_id, username, filename, total_numbers, datetime.now(), 'PROCESSING'))
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

def complete_batch(batch_id, good, bad, error):
    """Mark batch as completed"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        UPDATE scrub_batches 
        SET status = 'COMPLETED', end_time = ?, good_count = ?, bad_count = ?, error_count = ?
        WHERE batch_id = ?
    ''', (datetime.now(), good, bad, error, batch_id))
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
            (batch_id, user_id, phone_number, original_number, status, detailed_status, area_code)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            data['batch_id'],
            data['user_id'],
            data['phone_number'],
            data.get('original_number', data['phone_number']),
            data['status'],
            data.get('detailed_status', data['status']),
            data['area_code']
        ))
    
    conn.commit()
    conn.close()

def get_user_stats(user_id):
    """Get statistics for a user"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute('SELECT COUNT(*) FROM scrub_batches WHERE user_id = ?', (user_id,))
    total_batches = cursor.fetchone()[0]
    
    cursor.execute('SELECT SUM(total_numbers) FROM scrub_batches WHERE user_id = ?', (user_id,))
    total_scrubbed = cursor.fetchone()[0] or 0
    
    cursor.execute('SELECT SUM(good_count) FROM scrub_batches WHERE user_id = ?', (user_id,))
    total_good = cursor.fetchone()[0] or 0
    
    cursor.execute('SELECT SUM(bad_count) FROM scrub_batches WHERE user_id = ?', (user_id,))
    total_bad = cursor.fetchone()[0] or 0
    
    today = datetime.now().date()
    cursor.execute('''
        SELECT COUNT(*) FROM scrub_batches 
        WHERE user_id = ? AND date(start_time) = ?
    ''', (user_id, today))
    today_batches = cursor.fetchone()[0]
    
    cursor.execute('''
        SELECT batch_id, filename, total_numbers, good_count, bad_count, 
               error_count, status, start_time, end_time
        FROM scrub_batches 
        WHERE user_id = ? 
        ORDER BY start_time DESC 
        LIMIT 10
    ''', (user_id,))
    
    recent = cursor.fetchall()
    conn.close()
    
    return {
        'total_batches': total_batches,
        'total_scrubbed': int(total_scrubbed),
        'total_good': int(total_good),
        'total_bad': int(total_bad),
        'today_batches': today_batches,
        'recent_batches': recent
    }

def get_batch_numbers(batch_id):
    """Get scrubbed numbers for a batch"""
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query('''
        SELECT phone_number, status, detailed_status, area_code, scrub_date
        FROM scrubbed_numbers
        WHERE batch_id = ?
        ORDER BY 
            CASE WHEN status = 'BAD' THEN 1 
                 WHEN status = 'UNKNOWN' THEN 2
                 ELSE 3 END,
            phone_number
    ''', conn, params=(batch_id,))
    conn.close()
    return df

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
    
    cursor.execute('''
        SELECT id, username, email, company, created_at, total_scrubbed, api_key 
        FROM users ORDER BY created_at DESC
    ''')
    users = cursor.fetchall()
    
    cursor.execute('''
        SELECT batch_id, username, filename, total_numbers, good_count, bad_count, status, start_time
        FROM scrub_batches 
        ORDER BY start_time DESC 
        LIMIT 50
    ''')
    batches = cursor.fetchall()
    
    conn.close()
    
    return {
        'total_users': total_users,
        'total_batches': total_batches,
        'total_numbers': int(total_numbers),
        'total_good': int(total_good),
        'total_bad': int(total_bad),
        'users': users,
        'batches': batches
    }

# ============================================
# PHONE NUMBER UTILITIES
# ============================================
def clean_phone_number(phone):
    """Clean phone number to 10-digit format"""
    if pd.isna(phone):
        return None
    phone = str(phone)
    digits = re.sub(r'\D', '', phone)
    if len(digits) == 11 and digits.startswith('1'):
        digits = digits[1:]
    elif len(digits) == 10:
        pass
    elif len(digits) > 10:
        digits = digits[-10:]
    else:
        return None
    return digits

def format_phone_display(phone):
    """Format as (XXX) XXX-XXXX"""
    if phone and len(phone) == 10:
        return f"({phone[:3]}) {phone[3:6]}-{phone[6:]}"
    return phone

def validate_csv_structure(df):
    """Find phone number column"""
    phone_columns = ['phone', 'phone_number', 'number', 'mobile', 'cell', 
                    'contact', 'phone1', 'phone_1', 'telephone', 'tel']
    
    for col in df.columns:
        col_lower = str(col).lower().strip()
        if col_lower in phone_columns:
            return col
    
    return df.columns[0]

# ============================================
# LOGO HANDLING
# ============================================
def get_logo_html():
    """Get logo HTML - place your logo.png in the project folder"""
    try:
        if os.path.exists("logo.png"):
            with open("logo.png", "rb") as f:
                logo_data = base64.b64encode(f.read()).decode()
            return f'''
            <div class="gators-header">
                <img src="data:image/png;base64,{logo_data}" alt="Gators Scrub Logo">
                <h1>GATORS SCRUB</h1>
            </div>
            '''
    except:
        pass
    
    # Fallback to emoji logo
    return '''
    <div class="gators-header">
        <h1>🐊 GATORS SCRUB</h1>
    </div>
    '''

# ============================================
# PROCESSING FUNCTION - ULTRA-FAST VERSION
# ============================================
def process_file_fast(uploaded_file, user_id, username, api_key):
    """ULTRA-FAST file processing with configurable chunk sizes"""
    
    batch_id = str(uuid.uuid4())
    
    # Save uploaded file
    upload_path = f"uploads/{batch_id}_{uploaded_file.name}"
    os.makedirs('uploads', exist_ok=True)
    
    with open(upload_path, 'wb') as f:
        f.write(uploaded_file.getbuffer())
    
    # Read file
    try:
        if uploaded_file.name.endswith('.csv'):
            df = pd.read_csv(upload_path)
        else:
            df = pd.read_excel(upload_path)
    except Exception as e:
        return None, f"Error reading file: {str(e)}"
    
    phone_col = validate_csv_structure(df)
    total_rows = len(df)
    
    create_batch(batch_id, user_id, username, uploaded_file.name, total_rows)
    
    # Progress tracking
    progress_bar = st.progress(0)
    status_text = st.empty()
    stats_text = st.empty()
    speed_text = st.empty()
    
    # Get speed settings from session state
    chunk_size = st.session_state.get('chunk_size', 2000)
    parallel_workers = st.session_state.get('parallel_workers', 3)
    
    processed = 0
    good_count = 0
    bad_count = 0
    error_count = 0
    
    start_time = time.time()
    
    # Process in optimized chunks
    for i in range(0, len(df), chunk_size):
        chunk_start_time = time.time()
        chunk = df.iloc[i:i+chunk_size]
        raw_phones = chunk[phone_col].astype(str).tolist()
        
        # Clean phones in bulk
        clean_phones = []
        phone_map = {}
        for raw in raw_phones:
            clean = clean_phone_number(raw)
            if clean:
                clean_phones.append(clean)
                phone_map[clean] = raw
        
        if not clean_phones:
            continue
        
        # Progress callback
        def make_callback(current_chunk, total_chunks):
            def update_progress(current, total, message=""):
                status_text.text(f"🐊 Processing - {current}/{total} chunks")
            return update_progress
        
        chunk_num = (i // chunk_size) + 1
        total_chunks = (len(df) + chunk_size - 1) // chunk_size
        progress_callback = make_callback(chunk_num, total_chunks)
        
        # Use user's API key with parallel processing
        lookup_results = bulk_blacklist_lookup(clean_phones, api_key, progress_callback)
        
        # Process results
        chunk_results = []
        for clean_phone in clean_phones:
            status = lookup_results.get(clean_phone, 'UNKNOWN')
            area_code = clean_phone[:3] if len(clean_phone) >= 3 else '000'
            
            if status == 'GOOD':
                detailed = "✅ Clean Number"
                good_count += 1
            elif status == 'BAD':
                detailed = "❌ Blacklisted"
                bad_count += 1
            else:
                detailed = f"⚠️ {status}"
                error_count += 1
            
            chunk_results.append({
                'batch_id': batch_id,
                'user_id': user_id,
                'phone_number': clean_phone,
                'original_number': phone_map.get(clean_phone, clean_phone),
                'status': status,
                'detailed_status': detailed,
                'area_code': area_code
            })
        
        # Bulk insert
        if chunk_results:
            save_scrubbed_numbers(chunk_results)
        
        processed += len(clean_phones)
        update_batch_progress(batch_id, processed, good_count, bad_count, error_count)
        
        # Update progress
        progress = processed / total_rows
        progress_bar.progress(progress)
        
        # Calculate speed
        elapsed = time.time() - start_time
        speed = processed / elapsed if elapsed > 0 else 0
        remaining = (total_rows - processed) / speed if speed > 0 else 0
        
        # Show stats
        stats_text.markdown(f"""
        ### 📊 Live Stats
        | Metric | Count |
        |--------|-------|
        | ✅ GOOD | {good_count:,} |
        | ❌ BAD | {bad_count:,} |
        | ⚠️ UNKNOWN | {error_count:,} |
        | 📊 Progress | {processed:,}/{total_rows:,} |
        """)
        
        speed_text.markdown(f"""
        ### ⚡ Speed
        - **Rate:** {speed:.0f} numbers/sec
        - **ETA:** {remaining/60:.1f} minutes
        - **Chunk:** {chunk_num}/{total_chunks}
        - **Workers:** {parallel_workers}
        """)
    
    complete_batch(batch_id, good_count, bad_count, error_count)
    os.remove(upload_path)
    
    total_time = time.time() - start_time
    st.success(f"✅ Processed {total_rows:,} numbers in {total_time/60:.2f} minutes at {total_rows/total_time:.0f} numbers/sec!")
    
    return batch_id, {
        'total': total_rows,
        'good': good_count,
        'bad': bad_count,
        'error': error_count
    }

# ============================================
# LOGIN PAGE
# ============================================
def login_page():
    # Custom header with logo
    st.markdown(get_logo_html(), unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.markdown("<h2 style='text-align: center; color: #006341;'>Professional Phone Number Scrubbing</h2>", unsafe_allow_html=True)
        st.markdown("---")
        
        tab1, tab2 = st.tabs(["🔐 Login", "📝 Register"])
        
        with tab1:
            username = st.text_input("Username", key="login_username")
            password = st.text_input("Password", type="password", key="login_password")
            
            if st.button("Login", type="primary", use_container_width=True):
                user_id = authenticate_user(username, password)
                if user_id:
                    st.session_state.authenticated = True
                    st.session_state.username = username
                    st.session_state.user_id = user_id
                    st.rerun()
                else:
                    st.error("❌ Invalid username or password")
        
        with tab2:
            new_username = st.text_input("Username*", key="reg_username")
            new_password = st.text_input("Password*", type="password", key="reg_password")
            confirm = st.text_input("Confirm Password*", type="password")
            email = st.text_input("Email")
            company = st.text_input("Company Name")
            
            if st.button("Register", use_container_width=True):
                if not new_username or not new_password:
                    st.error("Username and password required")
                elif new_password != confirm:
                    st.error("Passwords do not match")
                else:
                    success, message = create_user(new_username, new_password, email, company)
                    if success:
                        st.success(message)
                    else:
                        st.error(message)

# ============================================
# API KEY MANAGEMENT PAGE
# ============================================
def api_key_page():
    st.markdown("<h2 style='color: #006341;'>🔑 API Key Management</h2>", unsafe_allow_html=True)
    
    # Get current API key
    current_key = get_user_api_key(st.session_state.user_id)
    
    st.markdown('<div class="api-section">', unsafe_allow_html=True)
    st.markdown("### Enter Your BLA API Key")
    st.markdown("Get your API key from [Blacklist Alliance](https://www.blacklistalliance.com)")
    
    # API key input
    new_api_key = st.text_input(
        "API Key",
        value=current_key,
        type="password",
        placeholder="Enter your BLA API key",
        help="Your API key is stored securely and used for all scrubbing operations"
    )
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("💾 Save API Key", use_container_width=True):
            if new_api_key:
                update_user_api_key(st.session_state.user_id, new_api_key)
                st.success("✅ API Key saved successfully!")
                st.rerun()
            else:
                st.error("Please enter an API key")
    
    with col2:
        if st.button("🔍 Test API Key", use_container_width=True):
            if new_api_key:
                with st.spinner("Testing API key..."):
                    valid, message = test_api_connection(new_api_key)
                    if valid:
                        st.success(message)
                        if new_api_key != current_key:
                            update_user_api_key(st.session_state.user_id, new_api_key)
                    else:
                        st.error(message)
            else:
                st.error("Please enter an API key to test")
    
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Show current status
    if current_key:
        st.markdown("### Current Status")
        if len(current_key) > 5:
            masked_key = current_key[:5] + "*" * (len(current_key) - 8) + current_key[-3:]
            st.info(f"✅ API Key configured: {masked_key}")
        else:
            st.warning("⚠️ No valid API key configured")

# ============================================
# MAIN SCRUBBER PAGE
# ============================================
def scrubber_page():
    with st.sidebar:
        st.markdown("<h2 style='color: #006341;'>🐊 Gators Scrub</h2>", unsafe_allow_html=True)
        st.markdown(f"**👤 User:** {st.session_state.username}")
        st.markdown("---")
        
        # Navigation
        page = st.radio("Menu", ["📤 Scrub Numbers", "🔑 API Settings", "📊 History"])
        
        st.markdown("---")
        
        # Stats
        stats = get_user_stats(st.session_state.user_id)
        
        st.markdown("### Your Stats")
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Total Batches", stats['total_batches'])
            st.metric("✅ GOOD", f"{stats['total_good']:,}")
        with col2:
            st.metric("Today's Batches", stats['today_batches'])
            st.metric("❌ BAD", f"{stats['total_bad']:,}")
        
        st.markdown("---")
        
        # API Key Status
        api_key = get_user_api_key(st.session_state.user_id)
        if api_key:
            st.success("✅ API Key Configured")
        else:
            st.warning("⚠️ No API Key - Go to API Settings")
        
        if st.button("🚪 Logout"):
            st.session_state.authenticated = False
            st.rerun()
    
    # Main content based on navigation
    if page == "🔑 API Settings":
        api_key_page()
    
    elif page == "📊 History":
        st.markdown("<h2 style='color: #006341;'>📊 Scrubbing History</h2>", unsafe_allow_html=True)
        
        stats = get_user_stats(st.session_state.user_id)
        if stats['recent_batches']:
            for batch in stats['recent_batches']:
                batch_id, filename, total, good, bad, error, status, start_time, end_time = batch
                
                with st.expander(f"📁 {filename} - {start_time}"):
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("Total", total)
                    with col2:
                        st.metric("✅ GOOD", good)
                    with col3:
                        if bad > 0:
                            st.metric("❌ BAD", bad, delta="⚠️ Blacklisted", delta_color="inverse")
                        else:
                            st.metric("❌ BAD", bad)
                    with col4:
                        st.metric("⚠️ Unknown", error)
                    
                    if status == 'COMPLETED':
                        if st.button(f"📥 View Results", key=f"view_{batch_id}"):
                            st.session_state.current_batch = batch_id
                            st.rerun()
        else:
            st.info("No scrubbing history yet")
    
    else:  # Scrub Numbers
        st.markdown("<h2 style='color: #006341;'>📤 Upload & Scrub Numbers</h2>", unsafe_allow_html=True)
        
        # Check for API key
        api_key = get_user_api_key(st.session_state.user_id)
        if not api_key:
            st.warning("⚠️ Please configure your BLA API key in the API Settings first")
            if st.button("Go to API Settings"):
                st.session_state.page = "API Settings"
                st.rerun()
            return
        
        # Speed settings
        with st.expander("⚡ Speed Settings", expanded=False):
            st.markdown('<div class="speed-panel">', unsafe_allow_html=True)
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                chunk_size = st.select_slider(
                    "**Chunk Size**",
                    options=[500, 1000, 2000, 5000, 10000],
                    value=2000,
                    help="Larger chunks = faster processing, but may hit API limits"
                )
            
            with col2:
                parallel_workers = st.select_slider(
                    "**Parallel Workers**",
                    options=[1, 2, 3, 4, 5],
                    value=3,
                    help="More workers = faster, but may hit rate limits"
                )
            
            with col3:
                processing_mode = st.radio(
                    "**Processing Mode**",
                    ["🚀 Turbo (Parallel)", "🔒 Safe (Sequential)"],
                    index=0
                )
            
            # Save settings to session state
            st.session_state.chunk_size = chunk_size
            st.session_state.parallel_workers = parallel_workers
            st.session_state.processing_mode = processing_mode
            
            # Show estimated speed
            if processing_mode == "🚀 Turbo (Parallel)":
                est_speed = chunk_size * parallel_workers * 2
            else:
                est_speed = chunk_size
            
            st.markdown(f"""
            <div style="background: #006341; color: white; padding: 10px; border-radius: 5px; margin-top: 10px;">
                <strong>⚡ ESTIMATED PERFORMANCE:</strong><br>
                {est_speed:,} numbers per minute<br>
                {(est_speed * 60):,} numbers per hour
            </div>
            """, unsafe_allow_html=True)
            
            st.markdown('</div>', unsafe_allow_html=True)
        
        uploaded_file = st.file_uploader("Choose CSV or Excel file", type=['csv', 'xlsx', 'xls'])
        
        if uploaded_file and not st.session_state.processing:
            try:
                if uploaded_file.name.endswith('.csv'):
                    df_preview = pd.read_csv(uploaded_file)
                else:
                    df_preview = pd.read_excel(uploaded_file)
                
                phone_col = validate_csv_structure(df_preview)
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.info(f"📁 **File:** {uploaded_file.name}")
                with col2:
                    st.info(f"📊 **Rows:** {len(df_preview):,}")
                with col3:
                    st.info(f"📱 **Phone Column:** {phone_col}")
                
                # Show estimated time
                estimated_batches = (len(df_preview) + chunk_size - 1) // chunk_size
                if processing_mode == "🚀 Turbo (Parallel)":
                    est_time = estimated_batches / parallel_workers * 1.5
                else:
                    est_time = estimated_batches * 2
                
                st.info(f"⏱️ Estimated time: {est_time:.1f} seconds ({estimated_batches} batches)")
                
                with st.expander("📋 Preview"):
                    st.dataframe(df_preview.head(10), use_container_width=True)
                
                if st.button("🚀 START SCRUBBING", type="primary", use_container_width=True):
                    st.session_state.processing = True
                    st.rerun()
                    
            except Exception as e:
                st.error(f"Error: {str(e)}")
        
        if st.session_state.processing and uploaded_file:
            st.markdown("---")
            st.subheader("⚙️ Processing...")
            
            api_key = get_user_api_key(st.session_state.user_id)
            
            # Get speed settings
            chunk_size = st.session_state.get('chunk_size', 2000)
            parallel_workers = st.session_state.get('parallel_workers', 3)
            processing_mode = st.session_state.get('processing_mode', "🚀 Turbo (Parallel)")
            
            # Show current settings
            st.info(f"⚡ Processing with: Chunk Size={chunk_size}, Workers={parallel_workers}, Mode={processing_mode}")
            
            # Choose processing function based on mode
            from utils.bla_api import bulk_blacklist_lookup_parallel, bulk_blacklist_lookup_sequential
            
            if processing_mode == "🚀 Turbo (Parallel)":
                # Use parallel processing
                import utils.bla_api
                original_func = utils.bla_api.bulk_blacklist_lookup
                utils.bla_api.bulk_blacklist_lookup = lambda x, y, z: bulk_blacklist_lookup_parallel(
                    x, y, z, chunk_size=chunk_size, max_workers=parallel_workers
                )
            else:
                # Use sequential processing
                import utils.bla_api
                original_func = utils.bla_api.bulk_blacklist_lookup
                utils.bla_api.bulk_blacklist_lookup = lambda x, y, z: bulk_blacklist_lookup_sequential(
                    x, y, z, chunk_size=chunk_size
                )
            
            try:
                batch_id, final_stats = process_file_fast(
                    uploaded_file, st.session_state.user_id, st.session_state.username, api_key
                )
            finally:
                # Restore original function
                import utils.bla_api
                utils.bla_api.bulk_blacklist_lookup = original_func
            
            if batch_id:
                st.session_state.current_batch = batch_id
                st.session_state.processing = False
                st.balloons()
                st.success("✅ Scrubbing Complete!")
                st.rerun()
        
        if st.session_state.current_batch:
            st.markdown("---")
            st.subheader("📥 Results")
            
            df_results = get_batch_numbers(st.session_state.current_batch)
            
            if not df_results.empty:
                summary = df_results['status'].value_counts()
                bad_count = summary.get('BAD', 0)
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("✅ GOOD", summary.get('GOOD', 0))
                with col2:
                    if bad_count > 0:
                        st.metric("❌ BAD", bad_count, delta="⚠️ Blacklisted Found", delta_color="inverse")
                    else:
                        st.metric("❌ BAD", bad_count)
                with col3:
                    st.metric("⚠️ UNKNOWN", summary.get('UNKNOWN', 0))
                
                if bad_count > 0:
                    st.markdown(f"""
                    <div class="bad-alert">
                        ⚠️ WARNING: Found {bad_count} blacklisted numbers! Do not call these numbers.
                    </div>
                    """, unsafe_allow_html=True)
                    
                    bad_df = df_results[df_results['status'] == 'BAD'].copy()
                    bad_df['Phone'] = bad_df['phone_number'].apply(format_phone_display)
                    st.dataframe(bad_df[['Phone', 'detailed_status']], use_container_width=True)
                
                df_display = df_results.copy()
                df_display['Phone'] = df_display['phone_number'].apply(format_phone_display)
                df_display = df_display[['Phone', 'detailed_status', 'area_code']]
                df_display.columns = ['Phone Number', 'Status', 'Area Code']
                
                with st.expander("📊 All Results"):
                    st.dataframe(df_display, use_container_width=True)
                
                csv = df_display.to_csv(index=False)
                st.download_button("📥 Download CSV", data=csv, 
                                 file_name=f"gators_scrub_{st.session_state.current_batch[:8]}.csv",
                                 mime="text/csv", use_container_width=True)

# ============================================
# ADMIN PAGE
# ============================================
def admin_page():
    st.markdown("<h1 style='color: #006341;'>🔐 Admin Portal</h1>", unsafe_allow_html=True)
    
    if st.session_state.username != 'admin':
        st.error("Access Denied")
        return
    
    stats = get_admin_stats()
    
    st.markdown("<h2 style='color: #006341;'>📊 System Overview</h2>", unsafe_allow_html=True)
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Users", stats['total_users'])
    with col2:
        st.metric("Total Batches", stats['total_batches'])
    with col3:
        st.metric("Numbers Scrubbed", f"{stats['total_numbers']:,}")
    with col4:
        st.metric("BAD Numbers", f"{stats['total_bad']:,}")
    
    st.markdown("---")
    st.markdown("<h2 style='color: #006341;'>👥 Users</h2>", unsafe_allow_html=True)
    
    if stats['users']:
        users_df = pd.DataFrame(stats['users'], 
                               columns=['ID', 'Username', 'Email', 'Company', 'Joined', 'Total Scrubbed', 'API Key'])
        # Mask API keys for security
        users_df['API Key'] = users_df['API Key'].apply(
            lambda x: x[:5] + "..." + x[-3:] if x and len(x) > 8 else "Not Set"
        )
        st.dataframe(users_df, use_container_width=True)
    
    st.markdown("---")
    st.markdown("<h2 style='color: #006341;'>📁 Recent Batches</h2>", unsafe_allow_html=True)
    
    if stats['batches']:
        batches_df = pd.DataFrame(stats['batches'],
                                 columns=['Batch ID', 'User', 'File', 'Total', 'Good', 'Bad', 'Status', 'Started'])
        st.dataframe(batches_df, use_container_width=True)
        
        if st.button("📥 Export All Data"):
            conn = sqlite3.connect(DB_PATH)
            all_data = pd.read_sql_query('''
                SELECT sb.username, sb.filename, sn.phone_number, sn.status, 
                       sn.detailed_status, sn.area_code, sn.scrub_date
                FROM scrubbed_numbers sn
                JOIN scrub_batches sb ON sn.batch_id = sb.batch_id
                ORDER BY sn.scrub_date DESC
            ''', conn)
            conn.close()
            
            csv = all_data.to_csv(index=False)
            st.download_button("💾 Download Database", data=csv,
                             file_name=f"gators_export_{datetime.now().strftime('%Y%m%d')}.csv",
                             mime="text/csv")

# ============================================
# MAIN
# ============================================
def main():
    if 'db_initialized' not in st.session_state:
        init_database()
        st.session_state.db_initialized = True
    
    if not st.session_state.authenticated:
        login_page()
    else:
        if st.session_state.username == 'admin':
            page = st.sidebar.radio("Navigation", ["Scrubber", "Admin"])
            if page == "Admin":
                admin_page()
            else:
                scrubber_page()
        else:
            scrubber_page()

if __name__ == '__main__':
    main()