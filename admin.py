"""
BLA Phone Scrubber - Admin Portal
Separate admin interface for managing all data
"""
import streamlit as st
import pandas as pd
import sqlite3
from datetime import datetime
import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv

load_dotenv()

# Page configuration
st.set_page_config(
    page_title="Admin Portal - BLA Scrubber",
    page_icon="🔐",
    layout="wide"
)

# Admin credentials
ADMIN_USERNAME = os.getenv('ADMIN_USERNAME', 'admin')
ADMIN_PASSWORD = os.getenv('ADMIN_PASSWORD', 'admin123')
DB_PATH = 'data/scrubber.db'

# Session state
if 'admin_auth' not in st.session_state:
    st.session_state.admin_auth = False

def format_phone_display(phone):
    """Format as (XXX) XXX-XXXX"""
    if phone and len(phone) == 10:
        return f"({phone[:3]}) {phone[3:6]}-{phone[6:]}"
    return phone

def get_database_stats():
    """Get comprehensive database stats"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Basic counts
    cursor.execute("SELECT COUNT(*) FROM users")
    total_users = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM scrub_batches")
    total_batches = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM scrubbed_numbers")
    total_records = cursor.fetchone()[0]
    
    # Status counts
    cursor.execute("SELECT status, COUNT(*) FROM scrubbed_numbers GROUP BY status")
    status_counts = dict(cursor.fetchall())
    
    # Today's activity
    today = datetime.now().date()
    cursor.execute('''
        SELECT COUNT(*) FROM scrub_batches 
        WHERE date(start_time) = ?
    ''', (today,))
    today_batches = cursor.fetchone()[0]
    
    # Database size
    db_size = os.path.getsize(DB_PATH) if os.path.exists(DB_PATH) else 0
    
    conn.close()
    
    return {
        'total_users': total_users,
        'total_batches': total_batches,
        'total_records': total_records,
        'good_count': status_counts.get('GOOD', 0),
        'bad_count': status_counts.get('BAD', 0),
        'unknown_count': status_counts.get('UNKNOWN', 0),
        'today_batches': today_batches,
        'db_size_mb': db_size / (1024 * 1024)
    }

def get_all_users():
    """Get all users with their stats"""
    conn = sqlite3.connect(DB_PATH)
    
    query = '''
        SELECT u.id, u.username, u.email, u.company, u.created_at, 
               u.total_scrubbed, COUNT(sb.id) as batch_count
        FROM users u
        LEFT JOIN scrub_batches sb ON u.id = sb.user_id
        GROUP BY u.id
        ORDER BY u.created_at DESC
    '''
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

def get_all_batches(limit=1000):
    """Get all batches with details"""
    conn = sqlite3.connect(DB_PATH)
    
    query = f'''
        SELECT sb.batch_id, sb.username, sb.filename, sb.total_numbers,
               sb.good_count, sb.bad_count, sb.error_count, sb.status,
               sb.start_time, sb.end_time,
               COUNT(sn.id) as verified_count
        FROM scrub_batches sb
        LEFT JOIN scrubbed_numbers sn ON sb.batch_id = sn.batch_id
        GROUP BY sb.batch_id
        ORDER BY sb.start_time DESC
        LIMIT {limit}
    '''
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

def get_all_scrubbed_numbers(limit=10000):
    """Get all scrubbed numbers with user info"""
    conn = sqlite3.connect(DB_PATH)
    
    query = f'''
        SELECT sn.phone_number, sn.status, sn.detailed_status, 
               sn.area_code, sn.scrub_date,
               sb.username, sb.filename
        FROM scrubbed_numbers sn
        JOIN scrub_batches sb ON sn.batch_id = sb.batch_id
        ORDER BY sn.scrub_date DESC
        LIMIT {limit}
    '''
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

# ============================================
# ADMIN INTERFACE
# ============================================
def login():
    """Admin login"""
    st.title("🔐 Admin Login")
    
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        st.markdown("### Enter Admin Credentials")
        
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        
        if st.button("Login", type="primary", use_container_width=True):
            if username == ADMIN_USERNAME and password == ADMIN_PASSWORD:
                st.session_state.admin_auth = True
                st.rerun()
            else:
                st.error("❌ Invalid credentials")

def dashboard():
    """Main admin dashboard"""
    st.title("📊 Admin Dashboard")
    
    # Get stats
    stats = get_database_stats()
    
    # Key metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Users", f"{stats['total_users']:,}")
        st.metric("Today's Batches", stats['today_batches'])
    
    with col2:
        st.metric("Total Batches", f"{stats['total_batches']:,}")
        st.metric("Database Size", f"{stats['db_size_mb']:.2f} MB")
    
    with col3:
        st.metric("Total Records", f"{stats['total_records']:,}")
        st.metric("✅ GOOD", f"{stats['good_count']:,}")
    
    with col4:
        st.metric("❌ BAD", f"{stats['bad_count']:,}")
        st.metric("⚠️ UNKNOWN", f"{stats['unknown_count']:,}")
    
    # Status distribution
    st.markdown("---")
    st.subheader("📊 Status Distribution")
    
    status_data = pd.DataFrame({
        'Status': ['GOOD', 'BAD', 'UNKNOWN'],
        'Count': [stats['good_count'], stats['bad_count'], stats['unknown_count']]
    })
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.bar_chart(status_data.set_index('Status'))
    
    with col2:
        if stats['total_records'] > 0:
            good_pct = (stats['good_count'] / stats['total_records']) * 100
            bad_pct = (stats['bad_count'] / stats['total_records']) * 100
            unknown_pct = (stats['unknown_count'] / stats['total_records']) * 100
            
            st.markdown(f"""
            ### 📈 Percentages
            - ✅ GOOD: {good_pct:.1f}%
            - ❌ BAD: {bad_pct:.1f}%
            - ⚠️ UNKNOWN: {unknown_pct:.1f}%
            """)
    
    # Users table
    st.markdown("---")
    st.subheader("👥 Users")
    
    users_df = get_all_users()
    if not users_df.empty:
        # Format for display
        display_users = users_df[['username', 'email', 'company', 'created_at', 'total_scrubbed', 'batch_count']].copy()
        display_users.columns = ['Username', 'Email', 'Company', 'Joined', 'Numbers Scrubbed', 'Batches']
        
        st.dataframe(display_users, use_container_width=True)
        
        # Export users
        if st.button("📥 Export Users to CSV"):
            csv = users_df.to_csv(index=False)
            st.download_button(
                label="💾 Download Users CSV",
                data=csv,
                file_name=f"users_export_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv"
            )
    
    # Batches table
    st.markdown("---")
    st.subheader("📁 Recent Batches")
    
    batches_df = get_all_batches(100)
    if not batches_df.empty:
        # Format for display
        display_batches = batches_df[['batch_id', 'username', 'filename', 'total_numbers', 
                                     'good_count', 'bad_count', 'status', 'start_time']].copy()
        display_batches['batch_id'] = display_batches['batch_id'].apply(lambda x: x[:8] + '...')
        display_batches.columns = ['Batch ID', 'User', 'File', 'Total', 'Good', 'Bad', 'Status', 'Started']
        
        st.dataframe(display_batches, use_container_width=True)
    
    # Export all data
    st.markdown("---")
    st.subheader("📥 Export All Data")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("📊 Export All Scrubbed Numbers", use_container_width=True):
            all_data = get_all_scrubbed_numbers(100000)
            if not all_data.empty:
                all_data['phone_number'] = all_data['phone_number'].apply(format_phone_display)
                csv = all_data.to_csv(index=False)
                st.download_button(
                    label="💾 Download Complete Database",
                    data=csv,
                    file_name=f"complete_scrub_data_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
                    mime="text/csv"
                )
    
    with col2:
        if st.button("🗑️ Clean Temporary Files", use_container_width=True):
            import shutil
            if os.path.exists('uploads'):
                shutil.rmtree('uploads')
                os.makedirs('uploads')
            st.success("✅ Temporary files cleaned")

def main():
    """Main admin app"""
    
    # Sidebar
    with st.sidebar:
        st.image("https://img.icons8.com/color/96/phone--v1.png", width=50)
        st.title("🔐 Admin Portal")
        
        if st.session_state.admin_auth:
            st.markdown(f"Logged in as: **{ADMIN_USERNAME}**")
            st.markdown("---")
            
            if st.button("🚪 Logout"):
                st.session_state.admin_auth = False
                st.rerun()
    
    # Main content
    if not st.session_state.admin_auth:
        login()
    else:
        dashboard()

if __name__ == '__main__':
    main()