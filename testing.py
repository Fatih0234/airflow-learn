import requests

response = requests.get("https://jsonplaceholder.typicode.com/users/1")

if response.status_code == 200:
    user = response.json()
    print(user)
else:
    print(f"Error: {response.status_code}")