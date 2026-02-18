"""
Simple BLA API Test Script
Run this first to verify your API key works
"""
import requests
import json

# YOUR BLA API KEY - PUT IT HERE
BLA_API_KEY = "Pkcka4f2BbdHh2FhzJtx"  # Replace with your real key
BLA_API_URL = "https://api.blacklistalliance.net/bulk/upload"

def test_single_lookup():
    """Test single phone number lookup"""
    print("\n=== Testing Single Lookup ===")
    
    # Try different formats that BLA might accept
    test_formats = [
        # Format 1: As query parameter
        f"{BLA_API_URL}?phones=5551234567&key={BLA_API_KEY}&ver=v1&resp=phonecode",
        
        # Format 2: Using params dict
        {"url": BLA_API_URL, "params": {
            'phones': '5551234567',
            'key': BLA_API_KEY,
            'ver': 'v1',
            'resp': 'phonecode'
        }},
        
        # Format 3: POST request
        {"method": "POST", "url": BLA_API_URL, "data": {
            'phones': '5551234567',
            'key': BLA_API_KEY,
            'ver': 'v1',
            'resp': 'phonecode'
        }},
    ]
    
    for i, test_format in enumerate(test_formats):
        print(f"\nTest {i+1}:")
        try:
            if isinstance(test_format, str):
                # Direct URL with parameters
                response = requests.get(test_format, timeout=10)
            elif test_format.get('method') == 'POST':
                # POST request
                response = requests.post(test_format['url'], 
                                       data=test_format['data'],
                                       timeout=10)
            else:
                # GET with params dict
                response = requests.get(test_format['url'], 
                                      params=test_format['params'],
                                      timeout=10)
            
            print(f"Status Code: {response.status_code}")
            print(f"Response Headers: {dict(response.headers)}")
            print(f"Response Text: {response.text[:200]}")
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    print(f"JSON Response: {json.dumps(data, indent=2)}")
                except:
                    print("Response is not JSON")
            else:
                print(f"Error: {response.status_code}")
                
        except Exception as e:
            print(f"Exception: {str(e)}")

def test_bulk_lookup():
    """Test bulk lookup with multiple numbers"""
    print("\n=== Testing Bulk Lookup ===")
    
    test_numbers = ['5551234567', '8005551212', '2125550199']
    
    # Try different bulk formats
    test_formats = [
        # Format 1: Comma-separated in URL
        f"{BLA_API_URL}?phones={','.join(test_numbers)}&key={BLA_API_KEY}&ver=v1&resp=phonecode",
        
        # Format 2: Using params dict
        {"url": BLA_API_URL, "params": {
            'phones': ','.join(test_numbers),
            'key': BLA_API_KEY,
            'ver': 'v1',
            'resp': 'phonecode'
        }},
        
        # Format 3: Multiple phone parameters
        {"url": BLA_API_URL, "params": {
            'key': BLA_API_KEY,
            'ver': 'v1',
            'resp': 'phonecode',
            'phone1': test_numbers[0],
            'phone2': test_numbers[1],
            'phone3': test_numbers[2]
        }},
    ]
    
    for i, test_format in enumerate(test_formats):
        print(f"\nBulk Test {i+1}:")
        try:
            if isinstance(test_format, str):
                response = requests.get(test_format, timeout=10)
            else:
                response = requests.get(test_format['url'], 
                                      params=test_format['params'],
                                      timeout=10)
            
            print(f"Status Code: {response.status_code}")
            print(f"Response Text: {response.text[:200]}")
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    print(f"JSON Response: {json.dumps(data, indent=2)}")
                except:
                    print("Response is not JSON")
                    
        except Exception as e:
            print(f"Exception: {str(e)}")

if __name__ == "__main__":
    print("🔍 BLA API Diagnostic Tool")
    print("=" * 50)
    print(f"API Key: {BLA_API_KEY[:5]}...{BLA_API_KEY[-5:] if len(BLA_API_KEY) > 10 else 'too short'}")
    print(f"API URL: {BLA_API_URL}")
    
    test_single_lookup()
    test_bulk_lookup()