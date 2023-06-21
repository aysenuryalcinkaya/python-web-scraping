import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor

url = "https://www.kitapyurdu.com/kitap/gece-yarisi-kutuphanesi/{}.html"
bookdata = []

def scrape_book_title(book_id):
    try:
        response = requests.get(url.format(book_id))
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, "html.parser")
            book_title = soup.find("h1", {"class": "pr_header__heading"}).text.strip()
            print(book_title)
            bookdata.append(book_title)
    except:
        pass

with ThreadPoolExecutor(max_workers=30) as executor:
    executor.map(scrape_book_title, range(1, 100001))

print("Toplam kaydedilen kitap sayısı:", len(bookdata))
