import requests
from bs4 import BeautifulSoup
import requests
from concurrent.futures import ThreadPoolExecutor
import pytz
import pyodbc
from datetime import datetime
import re
import csv
import math
import locale
import json



def save_to_sql(book_data, list_name):
  for book in book_data: 
    conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=89.252.135.34;DATABASE=kitaptiryakisi;UID=sa;PWD=AliMertPau1')
    cursor = conn.cursor()
    isbn, book_name, author, publisher, link, image_url, price , category, sitename , tr_now= book
    category = category
    check_query = "SELECT * FROM books WHERE isbn=?"
    cursor.execute(check_query, isbn)
    check_result = cursor.fetchone()


    if check_result:
        check_query_1 = "SELECT * FROM lists WHERE listname=?"
        cursor.execute(check_query_1, list_name)
        check_result_1= cursor.fetchone()

        if check_result_1:
            listid = check_result_1[0]

        else:
            insert_query = "INSERT INTO lists (listname) VALUES(?)"
            cursor.execute(insert_query, list_name)
            cursor.execute("SELECT @@IDENTITY AS listid")
            listid = cursor.fetchone()[0]
        
        check_query = "SELECT COUNT(*) FROM books WHERE isbn=? AND listid=?"
        params = (isbn, listid)
        cursor.execute(check_query, params)
        count = cursor.fetchone()[0]

        if count == 0:
            cursor.execute("UPDATE books SET listid = ? WHERE isbn = ? AND listid IS NULL", (listid, isbn))
        else:
            pass


        check_query = "SELECT * FROM site WHERE sitename=?"
        cursor.execute(check_query, sitename)
        check_result = cursor.fetchone()
    
        check_query = "SELECT COUNT(*) FROM booksinfo WHERE isbn=? AND link=?"
        params = (isbn, link)
        cursor.execute(check_query, params)
        count = cursor.fetchone()[0]

        if count == 0:
            if check_result:
                siteid = check_result[0]
                insert_query = "INSERT INTO booksinfo (isbn, siteid, link, imgurl, price,createtime) VALUES (?, ?, ?, ?, ?, ?)"
                cursor.execute(insert_query, isbn, siteid, link, image_url, price, tr_now)
                

            else:
                insert_query = "INSERT INTO site(sitename) VALUES(?)"
                cursor.execute(insert_query, sitename)
                cursor.execute("SELECT @@IDENTITY AS siteid")
                siteid = cursor.fetchone()[0]
                insert_query = "INSERT INTO booksinfo (isbn, siteid, link, imgurl, price,createtime) VALUES (?, ?, ?, ?, ?, ?)"
                cursor.execute(insert_query, isbn, siteid, link, image_url, price, tr_now)



    else:
        check_query_1 = "SELECT * FROM author WHERE authorname=?"
        cursor.execute(check_query_1, author)
        check_result_1= cursor.fetchone()

        if check_result_1:
            authorid = check_result_1[0]

        else:
            insert_query = "INSERT INTO author (authorname) VALUES(?)"
            cursor.execute(insert_query, author)
            cursor.execute("SELECT @@IDENTITY AS authorid")
            authorid = cursor.fetchone()[0]



        check_query_2 = "SELECT * FROM publisher WHERE publishername=?"
        cursor.execute(check_query_2, publisher)
        check_result_2= cursor.fetchone()

        if check_result_2:
            publisherid = check_result_2[0]

        else:
            insert_query = "INSERT INTO publisher (publishername) VALUES(?)"
            cursor.execute(insert_query, publisher)
            cursor.execute("SELECT @@IDENTITY AS publisherid")
            publisherid = cursor.fetchone()[0]


        check_query_3 = "SELECT * FROM category WHERE categoryname=?"
        cursor.execute(check_query_3, category)
        check_result_3= cursor.fetchone()

        if check_result_3:
            categoryid = check_result_3[0]

        else:
            insert_query = "INSERT INTO category (categoryname) VALUES(?)"
            cursor.execute(insert_query, category)
            cursor.execute("SELECT @@IDENTITY AS categoryid")
            categoryid = cursor.fetchone()[0]

        check_query_4 = "SELECT * FROM lists WHERE listname=?"
        cursor.execute(check_query_4, list_name)
        check_result_4= cursor.fetchone()

        if check_result_4:
            listid = check_result_4[0]

        else:
            insert_query = "INSERT INTO lists (listname) VALUES(?)"
            cursor.execute(insert_query, list_name)
            cursor.execute("SELECT @@IDENTITY AS listid")
            listid = cursor.fetchone()[0]


        
        insert_query = "INSERT INTO books (isbn, bookname, authorid, publisherid, categoryid,createat ) VALUES (?, ?, ?, ?, ?, ?)"
        cursor.execute(insert_query, isbn, book_name, authorid, publisherid, categoryid, tr_now)

        check_query = "SELECT * FROM site WHERE sitename=?"
        cursor.execute(check_query, sitename)
        check_result = cursor.fetchone()
        
        check_query = "SELECT COUNT(*) FROM booksinfo WHERE isbn=? AND link=?"
        params = (isbn, link)
        cursor.execute(check_query, params)
        count = cursor.fetchone()[0]

        if count == 0:
            if check_result:
                siteid = check_result[0]
                insert_query = "INSERT INTO booksinfo (isbn, siteid, link, imgurl, price,createtime) VALUES (?, ?, ?, ?, ?, ?)"
                cursor.execute(insert_query, isbn, siteid, link, image_url, price, tr_now)

            else:
                insert_query = "INSERT INTO site(sitename) VALUES(?)"
                cursor.execute(insert_query, sitename)
                cursor.execute("SELECT @@IDENTITY AS siteid")
                siteid = cursor.fetchone()[0]
                insert_query = "INSERT INTO booksinfo (isbn, siteid, link, imgurl, price,createtime) VALUES (?, ?, ?, ?, ?, ?)"
                cursor.execute(insert_query, isbn, siteid, link, image_url, price, tr_now)
        else:
            # Kayıt varsa işlemleri
            pass
    
    print("KAYIT BAŞARILI")
    # Değişiklikleri kaydetme ve bağlantıyı kapatma
    conn.commit()
    conn.close()

def get_book_info(url , category_name, sitename):
        try:
            response = requests.get(url)
            soup = BeautifulSoup(response.content, "html.parser")

            # ul etiketini bulun ve class adına göre filtreleyin
            # 2. li etiketini seçin
            li_tag = soup.select_one('ul.breadcrumb li:nth-of-type(2)')

            # li etiketinin içindeki span etiketini seçin
            span_tag = li_tag.select_one('span[itemprop="name"]')
            
            script_tag = soup.find('script', {'type': 'application/ld+json'})
            json_data = json.loads(script_tag.string)

            book_title = json_data["name"]
            author = json_data["author"]["name"]
            image = json_data["image"][0]
            # tr_TR bölgesel ayarını yükle
            locale.setlocale(locale.LC_ALL, "tr_TR")
            # "price" değerini çek ve float tipine dönüştür
            price = float(json_data["offers"]["price"].replace(",", "."))
            isbn = json_data["isbn"]
            publisher = json_data["publisher"]["name"]


            timezone_tr = pytz.timezone('Europe/Istanbul')
            tr_now = datetime.now(timezone_tr)
            book_info = {
                "Title": book_title,
                "Author": author,
                "Publisher": publisher,
                "ISBN": isbn,
                "Price": price,
                "İmage": image,
                "Category": span_tag.text,
                "Website": sitename,
                "Url": url,
                "Time": tr_now.strftime("%Y-%m-%d %H:%M:%S")
            }
            return book_info
        except Exception as e:
            print("Bir hata oluştu: " + str(e) + " " + url)
            pass



def main():
    base_url = "https://www.bkmkitap.com"
    category_urls =  [      
         #{"name": "Harry Potter Kitapları", "url": "https://www.bkmkitap.com/harry-potter-kitaplari", "category": ""},
         #{"name": "Yüzüklerin Efendisi Kitapları", "url": "https://www.bkmkitap.com/j-r-r-tolkien", "category": ""},
         {"name": "Kişisel Gelişim Kitapları", "url": "https://www.bkmkitap.com/kisisel-gelisim-kitaplari?", "category": "&stock=1"},
    ]
    book_data = []
    with ThreadPoolExecutor(max_workers=4) as executor:
        for category in category_urls:
            category_name = category["name"]
            category_url = category["url"]
    
            # kategorideki sayfa sayısı
            response = requests.get(category_url)
            soup = BeautifulSoup(response.text, 'html.parser')
            # Find the next link
            next_link = soup.find('a', class_='next')

            # Get the previous link
            try:
                result = next_link.find_previous_sibling('a').text
            except:
                span = soup.find("span", {"class": "text-custom-dark-gray box double fl"})
                # Metni al ve sayıyı çıkar
                text = span.text.strip()
                result = (int(text.split()[1]) // 96) +1

            number = int(result)
            pages = range(0, 1, 1)  
            for page_num in pages: 
                url = f"{category_url}?pg={page_num}"
                response = requests.get(url)
                soup = BeautifulSoup(response.content, "html.parser")
                # Find the a tag and extract its href attribute value
                book_links = [base_url+link['href'] for link in soup.find_all('a', {'class': 'fl col-12 text-description detailLink'})]

               
                with ThreadPoolExecutor(max_workers=96) as executor:
                    futures = []
                    for link in book_links:
                        futures.append(executor.submit(get_book_info, link, category_name, base_url))

                    for future in futures:
                        book_info = future.result()
                        if book_info:
                            book_data.append((book_info["ISBN"], book_info["Title"], book_info["Author"], book_info["Publisher"], book_info["Url"], book_info["İmage"], book_info["Price"], book_info["Category"], book_info["Website"], book_info["Time"]))

        
            
                # Tüm kitap bilgilerini yazdır
                for book_info in book_data:
                    print(book_info)
        save_to_sql(book_data, category_name)   
        list_len = len(book_data)
        print(list_len)         


if __name__ == '__main__':
    main()
