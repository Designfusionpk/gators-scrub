"""
Phone number utilities - NO SQLAlchemy dependency
"""
import re
import pandas as pd

def clean_phone_number(phone):
    """Clean phone number to 10-digit format"""
    if pd.isna(phone):
        return None
    
    phone = str(phone)
    # Remove all non-digits
    digits = re.sub(r'\D', '', phone)
    
    # Handle US numbers
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
    
    # Check first column if it contains numbers
    first_col = df.columns[0]
    sample = df[first_col].head(100).astype(str)
    digit_ratio = sample.apply(lambda x: len(re.sub(r'\D', '', str(x))) >= 10).mean()
    if digit_ratio > 0.5:
        return first_col
    
    return df.columns[0]