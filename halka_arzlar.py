import requests
from supabase import create_client, Client
from bs4 import BeautifulSoup
import time
import pytz
import asyncio
from datetime import datetime, timedelta

# Supabase Bağlantısı
SUPABASE_URL = ""  # Supabase Dashboard > Settings > Project URL
SUPABASE_KEY = ""  # Supabase Dashboard > Settings > API > anon public
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Kimlik Doğrulama
try:
    user = supabase.auth.sign_in_with_password({
        "email": "",
        "password": ""
    })
except Exception as auth_error:
    print(f"❌ Kimlik doğrulama hatası: {str(auth_error)}")
    exit()
# Türkiye Saati
turkey_tz = pytz.timezone('Europe/Istanbul')

# Hafta içi ve saat kontrolü
def is_market_open():
    # 10:00 ile 18:30 arasında olup olmadığını kontrol et
    return datetime.now(tz=turkey_tz).weekday() < 5 and ((10 <= int(datetime.now(tz=turkey_tz).strftime("%H:%M:%S").split(':')[0]) < 18) or (int(datetime.now(tz=turkey_tz).strftime("%H:%M:%S").split(':')[0]) == 18 and int(datetime.now(tz=turkey_tz).strftime("%H:%M:%S").split(':')[1]) <= 30))
    #return True

def save_data_to_supabase(data):
    bist_kod = data['bist_kod']
    hisse_adi = data['bist_kod']
    
    # Hisse adına göre kayıt var mı kontrol et
    response = supabase.table("hisse_verileri").select("*").eq("hisse_adi", hisse_adi).execute()
    existing_data = response.data if response.data else None
    
    if data['hisse_sonuc']== "Halka Arz Sonuçları Açıklandı":
        if not existing_data:
            print(bist_kod)
            supabase.table("hisse_verileri").insert({
                "hisse_adi": bist_kod,
                "img_url": data["img_url"],
                'son_fiyat':"-",
                'son_kapanis':"-",
                'gunluk_en_dusuk':"-",
                'gunluk_en_yuksek':"-",
                'fark_tl':"-",
                'fark':"-",
                'hacim_tl':"-",
                'zaman':"-",
                'garanti_data':False,
            }).execute()
    
    if not existing_data:
        pass
    else:
        # Eğer veri varsa, farklılık kontrolü yap
        update_fields = {}
        for key, value in data.items():
            if existing_data[0].get(key) != value and value is not None:
                update_fields[key] = value
        
        if update_fields:
            supabase.table("halka_arz_takvim").update(update_fields).eq("bist_kod", hisse_adi).execute()
            print(f"🔄 Veri güncellendi: {hisse_adi}")
        else:
            print(f"✅ {hisse_adi} için güncellenecek yeni veri yok.")
    
    # Halka arz takviminde hisse kodu var mı kontrol et
    arz_response = supabase.table("halka_arz_takvim").select("*").eq("bist_kod", bist_kod).execute()
    existing_arz = arz_response.data if arz_response.data else None
    

    if not existing_arz:
        # Eğer hisse halka arz tablosunda yoksa, ekleyelim
        supabase.table("halka_arz_takvim").insert({
            "bist_kod": bist_kod,
            "hisse_sirket": data["hisse_sirket"],
            "arz_tarih": data["arz_tarih"],
            "img_url": data["img_url"],
            "hisse_statu": data["hisse_statu"],
            "hisse_sonuc": data["hisse_sonuc"],
            "referans_var": bool(existing_data),
            "img_url": data["img_url"],
        }).execute()
        print(f"✅ Halka arz takvimine yeni kayıt eklendi: {bist_kod}")
    else:
        if data['arz_tarih'] == "Hazırlanıyor...":
            data['created_at'] = datetime.now(tz=turkey_tz).strftime('%Y-%m-%d %H:%M:%S')
        # Halka arz verileri güncellenecek mi kontrol et
        arz_update_fields = {}
        for key, value in data.items():
            if existing_arz[0].get(key) != value and value is not None:
                arz_update_fields[key] = value
        
        if arz_update_fields:
            supabase.table("halka_arz_takvim").update(arz_update_fields).eq("bist_kod", bist_kod).execute()
            print(f"🔄 Halka arz verisi güncellendi: {bist_kod}")
        else:
            print(f"✅ {bist_kod} için güncellenecek yeni halka arz verisi yok.")


def get_cleaned_data(url, headers):
    response = requests.get(url, headers=headers)
    count = 0
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')

        # Tüm <article class="index-list"> elemanlarını bul
        tab_items = soup.find_all('article', class_='index-list')

        
        for index, tab_item in enumerate(tab_items, start=1):
            if count == 15:
                break
            # Hisse Kodu (BIST Kodu)
            bist_kod_elem = tab_item.find('span', class_='il-bist-kod')
            bist_kod = bist_kod_elem.text.strip() if bist_kod_elem else "null"

            # Hisse Şirket Adı
            sirket_elem = tab_item.find('h3', class_='il-halka-arz-sirket')
            hisse_sirket = sirket_elem.text.strip() if sirket_elem else "null"

            # Halka Arz Tarihi
            arz_tarih_elem = tab_item.find('span', class_='il-halka-arz-tarihi')
            arz_tarih = arz_tarih_elem.text.strip() if arz_tarih_elem else "null"

            # Görsel URL
            img_elem = tab_item.find('img', class_='slogo')
            img_url = img_elem['src'] if img_elem else "null"

            # Hisse Statü ve Hisse Sonuç
            il_badge_elem = tab_item.find('div', class_='il-badge')
            if il_badge_elem:
                hisse_statu_elem = il_badge_elem.find('div', class_='il-new')
                hisse_statu = hisse_statu_elem.text.strip() if hisse_statu_elem else 'Null'
                
                hisse_sonuc_elem = il_badge_elem.find('i')
                hisse_sonuc = hisse_sonuc_elem['title'] if hisse_sonuc_elem and hisse_sonuc_elem.has_attr('title') else 'Null'

                hisse_talep_elem = il_badge_elem.find('div', class_='il-tt')
                #hisse_talep = True if hisse_talep_elem else False
                hisse_talep = bool(hisse_talep_elem)
                hisse_gong_elem = il_badge_elem.find('div', class_='il-gonk')
                hisse_gong = bool(hisse_gong_elem)
                

            else:
                hisse_statu = 'Null'
                hisse_sonuc = 'Null'

            # Eğer hisse_statu ve hisse_sonuc hala "Null" ise, il-ert kontrol et
            if hisse_statu == 'Null' and hisse_sonuc == 'Null':
                il_ert_elem = tab_item.find('div', class_='il-ert')
                if il_ert_elem:
                    il_ert_link = il_ert_elem.find('a')
                    if il_ert_link and il_ert_link.has_attr('title'):
                        hisse_sonuc = il_ert_link['title']
                        hisse_statu = il_ert_link.text.strip()

            # Temizlenen verileri bir sözlük halinde sakla
            data = {
                "bist_kod": bist_kod,
                "hisse_sirket": hisse_sirket,
                "arz_tarih": arz_tarih,
                "img_url": img_url,
                "hisse_statu": hisse_statu,
                "hisse_sonuc": hisse_sonuc,
                'hisse_talep':hisse_talep,
                'hisse_gong':hisse_gong,
            }
            count+=1
            # Supabase'e kaydet
            save_data_to_supabase(data)
    else:
        print(f"Error: {response.status_code}")

async def wait_until_target_time(target_time: datetime) -> bool:
    global minutes_left
    """
    Verilen target_time datetime nesnesine kadar kalan süreyi hesaplar.
    Kalan süre 60 saniyeden az ise hedef zamana ulaşıldığını kabul eder.
    """
    now = datetime.now(tz=turkey_tz)
    remaining = target_time - now
    remaining_seconds = remaining.total_seconds()
    
    if remaining_seconds <= 60:
        print(f"⏳ Hedef saat ({target_time.hour}:{target_time.minute}) geldi! Kod çalıştırılıyor...")
        return True
    else:
        minutes_left = int(remaining_seconds // 60)
        print(f"⏳ Hedef saat {target_time.hour}:{target_time.minute} için {minutes_left} dakika kaldı...")
        return False
        
        
url = "https://halkarz.com"  # Gerçek URL'yi buraya ekleyin
headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
active_hours = ['09:01','10:01','17:01','18:01']

# Çalışması gereken saat ve dakikalar (Saat, Dakika)
TARGET_TIMES = [(9, 1), (10, 1), (14, 45), (18, 1)]
async def main():
    try:
        while True:
            now = datetime.now(tz=turkey_tz)
            # Bugün için TARGET_TIMES listesindeki her bir hedef zamanı, datetime nesnesine dönüştürülüyor.
            upcoming_times = []
            for h, m in TARGET_TIMES:
                candidate = now.replace(hour=h, minute=m, second=0, microsecond=0)
                # Eğer candidate (bugünkü hedef) şu andan önceyse, ertesi günün aynı saati kabul et
                if candidate < now:
                    candidate += timedelta(days=1)
                upcoming_times.append(candidate)
            
            # En erken (en yakın) hedef zamanı seçilir
            next_target_time = min(upcoming_times)
            print(f"⏳ {next_target_time.hour}:{next_target_time.minute} için bekleniyor...")
            
            # Hedef zamana ulaşıldı mı kontrol et
            if await wait_until_target_time(next_target_time):
                print(f"⏰ {next_target_time.hour}:{next_target_time.minute} saati geldi! Veri çekiliyor...")
                get_cleaned_data(url, headers)  # Veri çekme fonksiyonunu çağırın
            else:
                print("Hedef saat henüz gelmedi, bekleniyor...")
            if minutes_left>0:
                time_sleep = minutes_left*60
            else: 
                time_sleep = 20
            # 20 saniye bekleyip yeniden kontrol et
            await asyncio.sleep(time_sleep)
    except Exception as e:
        print(f"Bir hata oluştu: {e}")
    except KeyboardInterrupt:
        print("Program durduruluyor...  ", datetime.now(tz=turkey_tz).strftime('%Y-%m-%d'),' / ',datetime.now(tz=turkey_tz).strftime("%H:%M:%S"))
    finally:
        print('Program Durdu!')             
# Çalıştır
asyncio.run(main())
