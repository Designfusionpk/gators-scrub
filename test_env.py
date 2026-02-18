"""
Test if .env file is being read properly
"""
import os
from dotenv import load_dotenv

# Load .env file
load_dotenv()

# Get API key
api_key = os.getenv('BLA_API_KEY', 'NOT FOUND')

print(f"Current directory: {os.getcwd()}")
print(f".env file exists: {os.path.exists('.env')}")
print(f"BLA_API_KEY: {api_key[:5]}...{api_key[-5:] if len(api_key) > 10 else api_key}")
print(f"Length of API key: {len(api_key)}")

if api_key == 'NOT FOUND':
    print("\n❌ API Key not found in .env file!")
elif api_key == 'YOUR_ACTUAL_BLA_API_KEY_HERE':
    print("\n❌ You haven't replaced the placeholder API key!")
else:
    print("\n✅ API Key is set correctly!")
    print(f"Your API key ({len(api_key)} chars) will be used for BLA requests")