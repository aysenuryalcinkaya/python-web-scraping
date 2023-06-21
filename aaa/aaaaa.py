import requests
from bs4 import BeautifulSoup

book_data = []  # Kitap bilgilerini saklamak için liste


def scrape_gazikitabevi(isbn):
    url = 'https://www.bkmkitap.com/arama?q=%09' + isbn 
    baseurl = 'https://www.bkmkitap.com/'
    sitename = 'Bkm Kitap'
    
    # Sayfaya bir istek gönder
    response = requests.get(url)

    # İstek başarılı ise çekmeye devam et
    if response.status_code == 200:
        # Sayfanın HTML içeriğini al
        content = response.text

        # BeautifulSoup kullanarak HTML'i ayrıştır
        soup = BeautifulSoup(content, 'html.parser')

        # Kitapları içeren HTML etiketlerini bul
        results = soup.find_all('div', class_='waw-content-product-list')

        # Kitapları dolaşarak gerekli bilgileri çek
        for result in results:
            price_span = result.find('span', class_='waw-content-basket')
            data_price = price_span.text.strip()

            # Kitabın resim linkini al
            img_tag = result.find('img', loading='lazy')
            src = img_tag['src']

            link_tag = result.find('a', class_='product-img')
            link = baseurl + link_tag['href']

            
            book_data.append((isbn, link, src, data_price, sitename))

        

    else:
        print('Talep başarısız oldu. Hata kodu:', response.status_code)
def scrape_yemkitapevi(isbn):
    url = 'https://yemkitabevi.com/search?type=product&q=' + isbn 
    baseurl = 'https://yemkitabevi.com/'
    sitename = 'Yem Kitapevi'

    # Sayfayı talep et
    response = requests.get(url)
    
    # Talep başarılı olduysa işleme devam et
    if response.status_code == 200:
        # Sayfanın HTML içeriğini al
        content = response.text
        
        # BeautifulSoup kullanarak HTML'i ayrıştır
        soup = BeautifulSoup(content, 'html.parser')

        # Ürün bilgilerini seçin
        results = soup.find_all('div', class_='rowFlex rowFlexMargin')

        # Kitapları dolaşarak gerekli bilgileri çek
        for result in results:
            link_tag = result.find('a', class_='proFeaturedImage')
            if link_tag is not None:
                link = baseurl + link_tag['href']
                src = 'https:'+result.find('img', class_='img-responsive imgFlyCart hidden')['src']
                price = result.find('div', class_='priceProduct priceSale').text
            
                book_data.append((isbn, link, src, price))
        
    else:
        print('Talep başarısız oldu. Hata kodu:', response.status_code)
    
    return book_data


isbn = '9789750721571'
# scrape_kitapsec(isbn)
# scrape_idefix(isbn)
# # scrape_dr(isbn)
# scrape_canyayinlari(isbn)
# scrape_yemkitapevi(isbn)
scrape_gazikitabevi(isbn)


for book in book_data:
    print('ISBN:', book[0])
    print('Sayfa Linki:', book[1])
    print('Resim Linki:', book[2])
    print('Fiyat:', book[3])
    print('---')