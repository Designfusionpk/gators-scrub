"""
Test if bla_api module can be imported correctly
"""
import sys
import os

print("Current directory:", os.getcwd())
print("Python path:", sys.path)

try:
    from utils.bla_api import test_api_connection, bulk_blacklist_lookup
    print("✅ Successfully imported from utils.bla_api")
    print("Functions available:")
    print("  - test_api_connection:", test_api_connection)
    print("  - bulk_blacklist_lookup:", bulk_blacklist_lookup)
except ImportError as e:
    print("❌ Import error:", e)
    
    # Try alternative import
    try:
        import utils.bla_api
        print("✅ Imported utils.bla_api module")
        print("Available attributes:", dir(utils.bla_api))
    except ImportError as e2:
        print("❌ Alternative import also failed:", e2)