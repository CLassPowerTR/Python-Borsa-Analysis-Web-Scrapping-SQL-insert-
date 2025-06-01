from supabase import create_client, Client
import asyncio
from datetime import datetime
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import threading
import time
import pytz
import math
import concurrent.futures

# supabase_detail.txt dosyasını açma ve içeriğini okuma
with open('supabase_detail.txt', 'r') as file:
    lines = file.readlines()

# Verileri sözlük şeklinde ayırma
credentials = {}
for line in lines:
    # Satırdaki boşluklardan temizleyip eşitlik işaretiyle ayırarak key-value çiftlerini oluşturuyoruz
    key, value = line.strip().split('=')
    credentials[key] = value

# Verileri yazdırma
SUPABASE_URL = credentials.get('SUPABASE_URL')
SUPABASE_KEY = credentials.get('SUPABASE_KEY')
email = credentials.get('email')
password = credentials.get('password')

# Supabase Bağlantısı
SUPABASE_URL = SUPABASE_URL  # Supabase Dashboard > Settings > Project URL
SUPABASE_KEY = SUPABASE_KEY
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Kimlik Doğrulama
try:
    user = supabase.auth.sign_in_with_password({
        "email": email,
        "password": password
    })
except Exception as auth_error:
    print(f"❌ Kimlik doğrulama hatası: {str(auth_error)}")
    exit()

# Türkiye Saati
turkey_tz = pytz.timezone('Europe/Istanbul')

# WebDriver'ın durumunu kontrol eden bir fonksiyon
def is_driver_alive(driver):
    try:
        driver.title  # Basit bir komut çalıştır
        return True
    except:
        return False
        
class DriverManager:
    _instance = None
    
    @classmethod
    def get_driver(cls):
        try:
            print("DriverManager.get_driver() çağrıldı")
            # Eğer _instance mevcut değilse ya da mevcut driver artık çalışmıyorsa, yeni bir driver başlat
            if not cls._instance or not is_driver_alive(cls._instance):
                if cls._instance:  # Eğer eski driver varsa, onu kapat
                    print("Eski driver kapatılıyor...")
                    cls._instance.quit()
                print("Yeni driver başlatılıyor...")
                cls._instance = start_driver()  # Yeni driver başlat
            else:
                print("Var olan driver kullanılıyor")
            return cls._instance
        except Exception as e:
            print(f"DriverManager.get_driver() hata: {e}")


# Web driver başlatma
def start_driver():
    options = webdriver.ChromeOptions()
    chrome_prefs = {"profile.default_content_setting_values": {"images": 2}}
    options.experimental_options["prefs"] = chrome_prefs

    options.add_argument("--headless=new")  # Gizli mod
    options.add_argument("--disable-gpu")
    options.add_argument("--log-level=3")
    options.add_experimental_option("detach", True)
    options.add_argument("--disable-dev-shm-usage")  # Bellek kullanımını optimize et
    options.add_experimental_option("excludeSwitches",["enable-logging"])
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36")
    driver = webdriver.Chrome(options=options)
    driver.get("https://www.garantibbva.com.tr/borsa-hisse-senetleri")
    driver.set_page_load_timeout(20)  # Sayfa yükleme zaman aşımı süresi

    
    #time.sleep(3)
    # Wait for dropdown and select XUTUM
    WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.CLASS_NAME, "dd-selected")))
    while True:
        driver.execute_script("window.scrollTo(0, 200);")
        dd_selected_value = driver.find_element(By.CSS_SELECTOR, "input.dd-selected-value")
        if dd_selected_value.get_attribute("value") != "XUTUM":
            dropdown_pointer = WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.CLASS_NAME, "dd-pointer")))
            dropdown_pointer.click()
            #time.sleep(5)
            dropdown_list = WebDriverWait(driver, 20).until(EC.visibility_of_element_located((By.CSS_SELECTOR, "ul.dd-options.dd-click-off-close")))
            for li in dropdown_list.find_elements(By.TAG_NAME, "li"):
                input_element = li.find_element(By.TAG_NAME, "a").find_element(By.TAG_NAME, "input")
                if input_element.get_attribute("value") == "XUTUM":
                    ActionChains(driver).move_to_element(li).click().perform()
                    break
        else:
            break

    # Tablo elemanlarının yüklenmesini bekle
    WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.TAG_NAME, "table")))
    return driver

# Hafta içi ve saat kontrolü
def is_market_open():
    # 10:00 ile 18:30 arasında olup olmadığını kontrol et
    return datetime.now(tz=turkey_tz).weekday() < 5 and ((10 <= int(datetime.now(tz=turkey_tz).strftime("%H:%M:%S").split(':')[0]) < 18) or (int(datetime.now(tz=turkey_tz).strftime("%H:%M:%S").split(':')[0]) == 18 and int(datetime.now(tz=turkey_tz).strftime("%H:%M:%S").split(':')[1]) <= 30))
    #return True

async def get_last_update_times():
    """Tüm hisselerin son güncelleme zamanlarını tek sorguda al"""
    try:
        response = supabase.table('hisse_verileri').select('hisse_adi,zaman').execute()
        return {item['hisse_adi']: item['zaman'] for item in response.data}
    except Exception as e:
        print(f"Zaman bilgisi alınamadı: {e}")
        return {}
        

async def fetch_borsa_data_and_upsert(driver):
    # Son güncelleme zamanlarını al
    last_updates = await get_last_update_times()
    start_scraping_time = time.time()


    batch_data = []
    if driver.find_element(By.ID, "tbl"):
        # Doğrudan 'tbl' elementinden tabloyu ve satırları al
        try:
            table_element = driver.find_element(By.ID, "tbl").find_element(By.TAG_NAME, "table")
            rows = table_element.find_elements(By.TAG_NAME, "tr")
        except Exception as e:
            print(f"❌ Hata: Tablo veya satırlar alınamadı: {e}")
            return

    # Her bir satırı gezerek verileri işle
    for row in rows:
        veriler = row.text.strip().split()
        # Eğer satır yeterli sütun içermiyorsa veya başlık satırıysa atla
        if len(veriler) <= 7 or veriler[0] == "Hisse":
            continue

        hisse_adi = veriler[0]
        web_time = veriler[7]
        # Daha önce çekilmiş veriyle aynıysa atla
        if hisse_adi in last_updates and last_updates[hisse_adi] == web_time:
            continue

        try:
            # Sayısal dönüşüm ve fark hesaplaması
            son_fiyat = float(veriler[1].replace('.', '').replace(',', '.'))
            son_kapanis = float(veriler[3].replace('.', '').replace(',', '.'))
            fark_tl = round(son_fiyat - son_kapanis, 2)
            formatted_fark_tl = f"{fark_tl:.2f}".replace('.', ',')
        except Exception as conv_err:
            print(f"⚠️ Dönüşüm hatası ({hisse_adi}): {conv_err}")
            formatted_fark_tl = "0,00"

        # Her hisse için kayıt sözlüğü oluştur
        record = {
            'hisse_adi': hisse_adi,
            'son_fiyat': veriler[1],
            'fark': veriler[2],
            'fark_tl': formatted_fark_tl,
            'son_kapanis': veriler[3],
            'gunluk_en_dusuk': veriler[4],
            'gunluk_en_yuksek': veriler[5],
            'hacim_tl': veriler[6],
            'zaman': web_time,
            'garanti_data': True,
        }
        batch_data.append(record)

    scraping_time = time.time() - start_scraping_time
    print(f"✅ Web scraping süresi: {scraping_time:.2f} saniye")
    
    # Eğer veri varsa, Supabase'e toplu güncelleme yap
    if batch_data:
        start_db_time = time.time()
        await bulk_upsert(batch_data)
        db_time = time.time() - start_db_time
        print(f"✅ Supabase'e veri kaydetme süresi: {db_time:.2f} saniye")



        
async def bulk_upsert(data):
    try:
        response = await asyncio.wait_for(
            asyncio.get_event_loop().run_in_executor(
                None,
                lambda: supabase.table("hisse_verileri").upsert(
                    data,
                    on_conflict="hisse_adi"
                ).execute()
            ),
            timeout=15
        )
        print(f"Toplu güncelleme başarılı: {len(data)} kayıt")
    except Exception as e:
        print(f"Toplu güncelleme hatası: {str(e)}")
        
# Semaphore optimizasyonu
semaphore = asyncio.Semaphore(20)  # Eşzamanlı işlem sayısını artır

# WebDriver'ı asenkron yönetim
async def async_get_driver():
    try:
        print("async_get_driver() fonksiyonu çağrıldı")
        loop = asyncio.get_event_loop()
        driver = await loop.run_in_executor(None, DriverManager.get_driver)
        print("Driver başarıyla alındı")
        return driver
    except Exception as e:
        print(f"async_get_driver() hata: {e}")
        return None
        
# Dinamik polling aralığı
dynamic_interval = 5  # Varsayılan 5 saniye

async def adjust_polling_interval():
    global dynamic_interval
    response_time = await measure_response_time()
    
    if response_time < 2:
        dynamic_interval = max(2, dynamic_interval - 1)
    else:
        dynamic_interval = min(30, dynamic_interval + 3)
        
# Basit önbellek yapısı
cache = {}
CACHE_TTL = 30  # saniye

def is_cache_valid(hisse_adi):
    return hisse_adi in cache and \
           (datetime.now() - cache[hisse_adi]['timestamp']).seconds < CACHE_TTL
  
import time

async def measure_response_time():
    """Web sayfasının yanıt süresini ölçer."""
    start_time = time.time()
    
    # Örneğin, sayfa yükleniyor veya veri çekiliyorsa, o işlemi buraya ekleyebilirsiniz.
    # Bu örnekte sayfanın bir elemanının yüklenmesini bekliyoruz:
    try:
        driver = DriverManager.get_driver()
        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.TAG_NAME, "table")))  # Tabloyu bekle
        end_time = time.time()
        return end_time - start_time  # Yanıt süresi
    except Exception as e:
        print(f"Yanıt süresi ölçülürken hata: {e}")
        return None


  
async def main():
    try:
        print("Ana program başlatılıyor")
        while True:
            if is_market_open():
                print("Piyasa açık, kontrol ediliyor...")
                # Driver mevcutsa ve sağlamsa, kullan; aksi halde yeniden oluştur.
                driver = DriverManager.get_driver()
                if driver is None or not is_driver_alive(driver):
                    print("Driver geçerli değil, yeniden oluşturuluyor...")
                    driver = DriverManager.get_driver()
                # Piyasa açık olduğu sürece veri çekme döngüsü
                while is_market_open():
                    try:
                        print("Piyasa açık, veri çekiliyor...")
                        start_time = time.time()
                        # Paralel veri işleme
                        await asyncio.gather(
                            fetch_borsa_data_and_upsert(driver),
                            #adjust_polling_interval()
                        )
                        
                        print(f"Döngü süresi: {time.time()-start_time:.2f}s")
                        
                    except Exception as e:
                        print(f"Kritik hata: {str(e)}")
                        await asyncio.sleep(5)
                # Piyasa kapandıysa; mevcut driver'ı kapat ve örneği sıfırla
                if driver:
                    print("Piyasa kapandı, driver kapatılıyor...")
                    try:
                        driver.quit()
                    except Exception as e:
                        print(f"Driver kapatılırken hata: {e}")
                    DriverManager._instance = None
            else:
                print("Piyasa kapalı, bekleniyor...")
                # Piyasa kapalı olduğu sürece belirli aralıkla kontrol et
                await asyncio.sleep(300)
    except Exception as e:
        print(f"Ana programda hata: {e}")
    finally:
        # Program kapanırken varsa driver'ı kapat
        if DriverManager._instance:
            try:
                DriverManager._instance.quit()
            except Exception as e:
                print(f"Final driver kapatma hatası: {e}")
asyncio.run(main())
