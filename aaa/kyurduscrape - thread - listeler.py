import requests
from bs4 import BeautifulSoup
import requests
from concurrent.futures import ThreadPoolExecutor
import pytz
import pyodbc
from datetime import datetime
import re
import csv



def save_to_sql(book_data, list_name):
  for book in book_data: 
    conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=89.252.135.34;DATABASE=kitaptiryakisi;UID=sa;PWD=AliMertPau1')
    cursor = conn.cursor()
    isbn, book_name, author, publisher, link, image_url, price , category, sitename , tr_now= book
    category = category + " " + "Kitapları"
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
            response.encoding = "utf-8"
            soup = BeautifulSoup(response.content, "html.parser")

            kategori = soup.find('ul', {'class': 'rel-cats__list'}).find_all('span')[1].text
            book_title = soup.find("h1", {"class": "pr_header__heading"}).text.strip()
            try: 
                author = soup.find("div", class_="pr_producers__manufacturer").text.strip() #orjinal author
            except Exception as e:
                try: 
                    producer_div = soup.find("div", {"class": "pr_producers__manufacturer"})
                    author = producer_div.find("a").text.strip() # 2 adet author olduğunda 1.ciyi alıyor    
                except Exception as e:
                    try: 
                        author = soup.find("td", string="Editor:").find_next_sibling("td").get_text(strip=True) #editör
                    except Exception as e:
                        author = soup.find("td", string="Derleyici:").find_next_sibling("td").get_text(strip=True) #derleyici

            try: 
                image = soup.find("a", {"class": "js-jbox-book-cover"})["href"]  
            except Exception as e:
                # Kitap resminin olduğu div etiketini seç
                book_front_div = soup.find("div", {"class": "book-front"})

                # div etiketi altındaki a etiketini seç ve href özelliğinin değerini al
                image = book_front_div.find("a")["href"]
            publisher = soup.find("div", class_="pr_producers__publisher").text.strip()
            isbn = soup.find('meta', {'itemprop': 'gtin13'}).get('content')
            price = soup.find("div", class_="price__item").text.strip()
            price = price.replace(",", ".") # Virgülü noktaya dönüştürme
            price = float(price)


            timezone_tr = pytz.timezone('Europe/Istanbul')
            tr_now = datetime.now(timezone_tr)
            book_info = {
                "Title": book_title,
                "Author": author,
                "Publisher": publisher,
                "ISBN": isbn,
                "Price": price,
                "İmage": image,
                "Category": kategori,
                "Website": "Kitapyurdu.com",
                "Url": url,
                "Time": tr_now.strftime("%Y-%m-%d %H:%M:%S")
            }

            return book_info
        except Exception as e:
            print("Bir hata oluştu: " + str(e) + " " + url)
            pass



def main():
    base_url = "https://www.kitapyurdu.com"
    category_urls =  [      
         {"url": "https://www.kitapyurdu.com/index.php?route=product/category", "category": "&filter_category_all=true&path=1&filter_in_stock=0&sort=purchased_365&order=DESC&limit=100"},
        

    ]

    book_data = []
    for category in category_urls:
        
        category_url = category["url"]
        category_category = category["category"]
        
        response = requests.get(f"{category_url}&page=1{category_category}")
        soup = BeautifulSoup(response.content, "html.parser")
        number = (int(soup.find("div", {"id": "faceted-search-list-total"}).h2.text.split()[0]) // 100) + 1
        response = requests.get(category_url)
        pages = range(1, 2, 1)
        for page_num in pages: 
            url = f"{category_url}&page={page_num}{category_category}"
            response = requests.get(url)
            soup = BeautifulSoup(response.content, "html.parser")
            book_links = []
            for div in soup.find_all('div', {'class': 'name'}):
                link = div.a.get('href')
                book_links.append(link)
            with ThreadPoolExecutor(max_workers=6) as executor:
                futures = []
                for link in book_links:
                    futures.append(executor.submit(get_book_info, link, base_url))

                for future in futures:
                    book_info = future.result()
                    if book_info:
                        book_data.append((book_info["ISBN"], book_info["Title"], book_info["Author"], book_info["Publisher"], book_info["Url"], book_info["İmage"], book_info["Price"], book_info["Category"], book_info["Website"], book_info["Time"]))

    
        
            # Tüm kitap bilgilerini yazdır
            for book_info in book_data:
                print(book_info[7])
        
    list_len = len(book_data)
    print(list_len)         
            


if __name__ == '__main__':
    main()
