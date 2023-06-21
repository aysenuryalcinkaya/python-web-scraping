from decimal import Decimal
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
import json
from datetime import datetime
import mysql.connector
import pyodbc

book_data = []
lock = Lock()

session = requests.Session()
def format_date(date_string):
    try:
        date = datetime.strptime(date_string, "%d.%m.%Y")
        return date.strftime("%Y-%m-%d")  # Format the date as "YYYY-MM-DD"
    except ValueError:
        return None

def get_book_info(url):
    try:
        response = session.get(url)
        session.headers.update({'Accept-Encoding': 'utf-8'})
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, "html.parser")
            title = soup.find("meta", property="og:title")["content"]
            image = soup.find("meta", property="og:image")["content"]
            og_url = soup.find("meta", property="og:url")["content"]
            script_tags = soup.find_all("script", type="application/ld+json")

            author_name = None
            publisher_name = None
            for script_tag in script_tags:
                script_data = json.loads(script_tag.string.strip())
                if script_data.get("@type") == "Book":
                    author_name = script_data["author"]["name"]
                    publisher_name = script_data["publisher"]["name"]
                    break

            description = soup.find("div", id="description_text").find("span").text

            book_info = {
                "title": title,
                "image": image,
                "url": og_url,
                "author": author_name,
                "publisher": publisher_name,
                "description": description,
            }

            categories = soup.find("div", class_="rel-cats")
            if categories:
                category_items = categories.find_all("span")
                if len(category_items) >= 3:
                    category = category_items[1].text.strip()
                    subcategory = category_items[2].text.strip()
                    book_info["Category"] = category
                    book_info["Subcategory"] = subcategory

            attributes = soup.find("div", class_="attributes")
            table_rows = attributes.find_all("tr")
            for row in table_rows:
                cells = row.find_all("td")
                if len(cells) == 2:
                    key = cells[0].text.strip()
                    value = cells[1].text.strip()
                    book_info[key] = value

            # Fiyat bilgisini çekme
            price_div = soup.find("div", class_="price__item")
            if price_div:
                price_text = price_div.text.strip()
                price_text = price_text.replace(",", "").replace(".", "")  # Virgül ve nokta işaretlerini kaldırın
                price = f"{price_text[:-2]}.{price_text[-2:]}"  # Ondalık basamağı ayarlayın
                book_info["price"] = Decimal(price)  # Decimal veri tipine dönüştürün

            # Yayın Tarihi
            publication_date = book_info.get("Yayın Tarihi:")
            if publication_date:
                formatted_date = format_date(publication_date)
                if formatted_date:
                    book_info["Yayın Tarihi:"] = formatted_date

            return book_info

        else:
            print("Hata: {} sayfası çekilirken bir hata oluştu. Status kodu: {}".format(url, response.status_code))
    except Exception as e:
        print("Bir hata oluştu: " + str(e) + " " + url)
        pass

def main():
    base_url = "https://www.kitapyurdu.com"
    category_urls = [
        {
            "url": "https://www.kitapyurdu.com/index.php?route=product/category",
            "category": "&filter_category_all=true&path=1&filter_in_stock=0&sort=purchased_365&order=DESC&limit=100",
        },
    ]

    start_time = datetime.now()

    for category in category_urls:
        category_url = category["url"]
        category_category = category["category"]

        response = session.get(f"{category_url}&page=1{category_category}")
        soup = BeautifulSoup(response.content, "html.parser")
        number = (int(soup.find("div", {"id": "faceted-search-list-total"}).h2.text.split()[0]) // 100) + 1

        pages = range(1, 2)
        for page_num in pages:
            url = f"{category_url}&page={page_num}{category_category}"
            response = session.get(url)
            soup = BeautifulSoup(response.content, "html.parser")
            book_links = [div.a.get('href') for div in soup.find_all('div', {'class': 'name'})]

            with ThreadPoolExecutor(max_workers=50) as executor:
                futures = [executor.submit(get_book_info, link) for link in book_links]

                for future in futures:
                    book_info = future.result()
                    if book_info:
                        with lock:
                            book_data.append(book_info)

    end_time = datetime.now()
    duration = end_time - start_time

    for book_info in book_data:
        print(book_info)

    list_len = len(book_data)
    print(list_len)
    print("Çekme süresi:", duration)

    unique_values = {}

    for book_info in book_data:
        for key, value in book_info.items():
            if key in unique_values:
                unique_values[key].add(value)
            else:
                unique_values[key] = {value}

    for key, values in unique_values.items():
        print(f"{key}: {len(values)} farklı değer")


if __name__ == '__main__':
    main()