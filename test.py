import requests

url = "https://server.rakibshahid.com/api/leetcode_ac"
headers = {"leetcode-username": "rakib-shahid"}
response = requests.get(url, headers=headers)

print(response.json())
