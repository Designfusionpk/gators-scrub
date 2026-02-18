"""
Test BLA Bulk Upload API
"""
import requests
import time

# YOUR API KEY HERE
BLA_API_KEY = "YOUR_ACTUAL_API_KEY"  # Replace with your key
BLA_BULK_URL = "https://api.blacklistalliance.net/bulk/upload"

def test_bulk_api():
    """Test the bulk upload API"""
    
    print("🔍 Testing BLA Bulk Upload API")
    print("=" * 50)
    
    # Create test phone numbers
    test_phones = ["5551234567", "8005551212", "2125550199"]
    file_content = "\n".join(test_phones)
    
    # Create boundary
    boundary = "----WebKitFormBoundary" + str(time.time()).replace('.', '')
    
    # Build multipart form data EXACTLY as in your example
    body = f"--{boundary}\r\n"
    body += f'Content-Disposition: form-data; name="filetype"\r\n\r\n'
    body += f"phone\r\n"
    body += f"--{boundary}\r\n"
    body += f'Content-Disposition: form-data; name="file"; filename="test_phones.txt"\r\n'
    body += f"Content-Type: text/plain\r\n\r\n"
    body += f"{file_content}\r\n"
    body += f"--{boundary}--\r\n"
    
    # Headers
    headers = {
        "accept": "application/zip",
        "content-type": f"multipart/form-data; boundary={boundary}"
    }
    
    print(f"\n📤 Uploading {len(test_phones)} test phones...")
    print(f"Boundary: {boundary}")
    
    # Try with API key in URL
    url_with_key = f"{BLA_BULK_URL}?key={BLA_API_KEY}"
    print(f"\nURL: {url_with_key}")
    
    try:
        response = requests.post(
            url_with_key,
            data=body.encode('utf-8'),
            headers=headers,
            timeout=30
        )
        
        print(f"\n📥 Response Status: {response.status_code}")
        print(f"Headers: {dict(response.headers)}")
        
        if response.status_code == 200:
            print(f"✅ Success! Response size: {len(response.content)} bytes")
            
            # Check content type
            content_type = response.headers.get('content-type', '')
            if 'application/zip' in content_type:
                print("📦 Received ZIP file")
                
                # Save ZIP for inspection
                with open('test_response.zip', 'wb') as f:
                    f.write(response.content)
                print("💾 Saved response to test_response.zip")
            else:
                print(f"Response text: {response.text[:500]}")
        else:
            print(f"❌ Error: {response.text}")
            
    except Exception as e:
        print(f"❌ Exception: {str(e)}")

if __name__ == "__main__":
    test_bulk_api()