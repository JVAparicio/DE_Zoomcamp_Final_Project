import requests
from bs4 import BeautifulSoup

# Define the URL of the page with the files
url = "https://gitlab.com/recommend.games/bgg-ranking-historicals/-/tree/master"

# Make a GET request to the URL
response = requests.get(url)

# Parse the HTML content of the response with BeautifulSoup
soup = BeautifulSoup(response.content, "html.parser")

# Find all the links on the page
links = soup.find_all("a")

print(links)

# Extract the href attribute of each link
file_names = []
for link in links:
    href = link.get("href")
    if href.endswith(".csv"):
        file_names.append(href)

# Print the list of file names
print(file_names)