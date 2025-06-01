from tradingview_ta import TA_Handler, Interval, Exchange
from pprint import pprint
from datetime import datetime, timedelta
from supabase import create_client, Client
import time
import asyncio
import time
import threading
from itertools import islice
import pytz
from math import ceil

#🔄❌📊🎉✅

# Supabase Bağlantısı
SUPABASE_URL = "API_URL"  # Supabase Dashboard > Settings > Project URL
SUPABASE_KEY = "API_KEY"  # Supabase Dashboard > Settings > API > anon public
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Kimlik Doğrulama
try:
    user = supabase.auth.sign_in_with_password({
        "email": "", # Supabase account email
        "password": "" # Supabase account password
    })
except Exception as auth_error:
    print(f"❌ Kimlik doğrulama hatası: {str(auth_error)}")
    exit()

# Firebase'deki mevcut verileri almak için yardımcı bir fonksiyon
def get_existing_hisse_data():
    db_response = supabase.table('hisse_verileri') \
    .select('hisse_adi') \
    .execute()

    return [item['hisse_adi'] for item in db_response.data]
   
def get_existing_analiz_data():
    db_response = supabase.table('hisse_analiz') \
    .select('hisse_adi') \
    .execute()

    return [item['hisse_adi'] for item in db_response.data]
    

def update_data(liste):
    db.collection('hisse_analizleri').document('veriler').set({'analiz_list': liste}, merge=True)

def is_empty(value):
    return value is None or (isinstance(value, str) and value.strip() == '')

def hisse_analizleri_baslat(hisse_adi):
        # BIST hisse analizi için bir örnek
        analysis = TA_Handler(
            symbol= str(hisse_adi),  # Hisse kodu (örnek: THYAO)
            screener="turkey",  # Borsa İstanbul için "turkey" screener'ını kullanın
            exchange="BIST",  # Borsa İstanbul
            interval=Interval.INTERVAL_1_DAY  # Günlük zaman aralığı
        )
        # Teknik analiz sonuçları
        try:
            analysis_result = analysis.get_analysis()

            # Hareketli Ortalamalardan Trend Belirleme
            ma_recommendation = analysis_result.moving_averages["RECOMMENDATION"]

            # Osilatörlerden Trend Belirleme
            osc_recommendation = analysis_result.oscillators["RECOMMENDATION"]
            
            # Trend Yönü Belirleme
            if ma_recommendation in ["BUY", "STRONG_BUY"] and osc_recommendation in ["BUY", "STRONG_BUY"]:
                trend = "UPTREND"
            elif ma_recommendation in ["SELL", "STRONG_SELL"] and osc_recommendation in ["SELL", "STRONG_SELL"]:
                trend = "DOWNTREND"
            else:
                trend = "SIDEWAYS"

            
            result = {
                'hisse_adi': hisse_adi if not is_empty(hisse_adi) else "-",
                'trend': trend if not is_empty(trend) else "-",
                'ma_recommendation': ma_recommendation if not is_empty(ma_recommendation) else "-",
                'osc_recommendation': osc_recommendation if not is_empty(osc_recommendation) else "-",
                'buy_analysis': analysis_result.summary.get("BUY", "-"),
                'sell_analysis': analysis_result.summary.get("SELL", "-"),
                'neutral_analysis': analysis_result.summary.get("NEUTRAL", "-"),
                'buy/sell_recommendation': analysis_result.summary.get("RECOMMENDATION", "-"),
                'ma_buy_recommendation': analysis_result.moving_averages.get('BUY', "-"),
                'ma_sell_recommendation': analysis_result.moving_averages.get('SELL', "-"),
                'ma_neutral_recommendation': analysis_result.moving_averages.get('NEUTRAL', "-"),
                'osc_buy_recommendation': analysis_result.oscillators.get('BUY', "-"),
                'osc_sell_recommendation': analysis_result.oscillators.get('SELL', "-"),
                'osc_neutral_recommendation': analysis_result.oscillators.get('NEUTRAL', "-"),
                'update_at': datetime.now(tz=turkey_tz).strftime("%Y-%m-%d %H:%M:%S")
                }

            # Sayısal değerleri formatla
            def format_number(value):
                if isinstance(value, float):
                    return round(value, 2)
                return value

            # Moving averages COMPUTE
            for key, value in analysis_result.moving_averages.get("COMPUTE", {}).items():
                result[key + '_ma_recommendation'] = format_number(value) if not is_empty(value) else "-"

            # Oscillators COMPUTE
            for key, value in analysis_result.oscillators.get("COMPUTE", {}).items():
                result[key + '_osc_recommendation'] = format_number(value) if not is_empty(value) else "-"

            # Indicators
            for key, value in analysis_result.indicators.items():
                result[key] = format_number(value) if not is_empty(value) else "-"

            # Tüm değerleri son kontrol
            for k, v in result.items():
                if isinstance(v, float):
                    result[k] = round(v, 2)
                elif is_empty(v):
                    result[k] = "-"

            all_analysis_results.append(result)

        except Exception as e:
            print(f"Hisse Adı {hisse_adi} Hata: {e}")



# Verileri parçalamak için yardımcı bir fonksiyon
def slice_list(data, chunk_size):
    for i in range(0, len(data), chunk_size):
        yield data[i:i + chunk_size]

semaphore = asyncio.Semaphore(10)  # Aynı anda en fazla 10 istek
async def upsert_database(veriler):
    max_retries = 3  # Maksimum yeniden deneme sayısı
    # Mevcut verileri çek (hisse_adi ve zaman değerleri için)
    try:
        for veri in veriler:
            if veri['hisse_adi'] in get_existing_analiz_data():
                for attempt in range(max_retries):
                    try:
                        # Zaman aşımı ekleyerek isteği sınırla
                        async with semaphore:  # Semaphore ile sınırla
                            await asyncio.wait_for(
                                asyncio.get_event_loop().run_in_executor(
                                    None,
                                    lambda: supabase.table("hisse_analiz").upsert(
                                        veri,
                                        on_conflict="hisse_adi"
                                    ).execute()
                                ),
                                timeout=10
                            )
                            break
                    except asyncio.TimeoutError:
                        print(f"❌Zaman aşımı (Deneme {attempt + 1}/{max_retries}): {veri['hisse_adi']}")
                    except Exception as e:
                        print(f"🔄Hata (Deneme {attempt + 1}/{max_retries}): {veri['hisse_adi']}", e)
                    else:
                        print(f"❌{veri['hisse_adi']} için maksimum yeniden deneme sayısı aşıldı.")
            else:
                print('hisse_analiz için mevcut değil!  - ',veri['hisse_adi'])
                
                try:
                    (
                    supabase.table("hisse_analiz")
                    .insert(veri)
                    .execute()
                    )
                    print(f"🎉Veri başarıyla eklendi: {veri[hisse_adi]}")
                except Exception as e:
                    print(f"❌Hata: {e}")
                
    except Exception as e:
        print(f"❌Hata ({veri['hisse_adi']}):", e)
    
    
async def hisseler():
    global all_analysis_results,hisse_list
    # Analiz sonuçlarını toplayacak bir liste
    all_analysis_results = []

    # Her kategori için işlem
    hisse_list = get_existing_hisse_data()

    chunk_size = 200  # Her parçadaki eleman sayısı
    batch_size = 50  # Her thread'de işlenecek kayıt sayısı
    
    chunks = list(slice_list(hisse_list, chunk_size))  # Hisseleri 100'erli parçalara ayır
    # Thread listesi
    tasks  = []
    for index, chunk in enumerate(chunks):
        print(f"Çalıştırılıyor: {index * chunk_size}-{(index + 1) * chunk_size} arası hisseler")
        # Thread'leri takip etmek için liste
        threads = []
        for hisse in chunk:
            # Thread oluştur ve başlat
            thread = threading.Thread(target=hisse_analizleri_baslat, args=(hisse,))
            thread.start()
            threads.append(thread)
        # Tüm thread'lerin bitmesini bekle
        for thread in threads:
            thread.join()  # Her thread'in bitmesini bekle
    for i in range(0, len(all_analysis_results), batch_size):
        batch = all_analysis_results[i:i + batch_size]
        tasks.append(upsert_database(batch))
        

    # Run all tasks asynchronously
    await asyncio.gather(*tasks)
    print(f"✅Hisse Analiz verileri güncellendi (Zaman: {datetime.now(tz=turkey_tz).strftime("%H:%M:%S")})")
    
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
    
# Türkiye Saati
turkey_tz = pytz.timezone('Europe/Istanbul')

active_hours = ['10:20','18:29','14:45','12:01']
# Çalışması gereken saat ve dakikalar (Saat, Dakika)
TARGET_TIMES = [(10, 20),(11, 1),(12, 1),(13, 1),(14, 1),(15, 1),(16, 1),(17, 1), (18, 1), (18, 30)]
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
                    # Saat geldiğinde veri çekme işlemi yapılacak
                    print(f"⏰ {next_target_time.hour}:{next_target_time.minute} saati geldi! Veri çekiliyor...")
                    await hisseler()  # Burada veri çekme fonksiyonunu çağırın
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
        print("Program durduruldu.  ", datetime.now(tz=turkey_tz).strftime('%Y-%m-%d'), ' / ', datetime.now(tz=turkey_tz).strftime("%H:%M:%S"))
    finally:
        print('Program Durdu!')

# Çalıştır
asyncio.run(main())
