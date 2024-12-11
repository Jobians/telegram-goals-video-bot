import requests
import json

def playwright_download_url(url):
  api_url = "https://try.playwright.tech/service/control/run"
  headers = {
    "Content-Type": "application/json"
  }
  
  data = {
    "code": f"""from playwright.sync_api import sync_playwright
def extract_custom(url):
  try:
    with sync_playwright() as p:
      browser = p.chromium.launch(headless=True)
      page = browser.new_page()
      page.goto(url, wait_until="load")
      page.wait_for_selector("video")
      video_element = page.query_selector("video")
      if video_element:
        video_url = video_element.get_attribute("src")
        if video_url:
          browser.close()
          return {{'video_url': video_url}}
      source_element = page.query_selector("video source")
      if source_element:
        video_url = source_element.get_attribute("src")
        if video_url:
          browser.close()
          return {{'video_url': video_url}}
      object_element = page.query_selector("object")
      if object_element:
        video_url = object_element.get_attribute("data")
        if video_url:
          browser.close()
          return {{'video_url': video_url}}
      embed_element = page.query_selector("embed")
      if embed_element:
        video_url = embed_element.get_attribute("src")
        if video_url:
          browser.close()
          return {{'video_url': video_url}}
      browser.close()
      return {{}}  
  except Exception as e:
    print(f"Extraction failed: {{e}}")
    return {{}}  

result = extract_custom("{url}")
print(result)
""",
    "language": "python"
  }
  
  response = requests.post(api_url, headers=headers, json=data)
  
  if response.status_code == 200:
    response_data = response.json()
    output = response_data.get('output', '{}')
    try:
      # Convert output string to valid JSON format
      output_dict = json.loads(output.replace("'", "\""))
      # Return only the video URL if it's found
      return output_dict.get('video_url', None)
    except json.JSONDecodeError:
      print("Failed to parse the output")
      return None
  else:
    print(f"Request failed with status code {response.status_code}")
    return None