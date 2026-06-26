import os
import requests
import time
from datetime import datetime, timezone, timedelta
from dateutil import parser
from supabase import create_client, Client
import subprocess

def eksekusi_pipeline_analitik(kredensial):
    # ==========================================
    # FASE 1: PERSIAPAN & KREDENSIAL
    # ==========================================
    try:
        supabase: Client = create_client(kredensial['supabase_url'], kredensial['supabase_key'])
    except Exception as e:
        print(f"[GAGAL SISTEM] Kredensial Supabase Eror: {e}")
        return []

    zona_wita = timezone(timedelta(hours=8))
    waktu_sekarang = datetime.now(zona_wita)

    # ==========================================
    # FASE 2: PENARIKAN & FILTRASI SUPABASE
    # ==========================================
    print("Mengecek antrean konten di Supabase...")
    BATAS_MAKSIMAL = 5

    try:
        respons_db = supabase.table('content_queue') \
            .select('*') \
            .eq('is_analyzed', False) \
            .is_('published_at', 'not_null') \
            .limit(BATAS_MAKSIMAL) \
            .execute()
        antrean = respons_db.data
    except Exception as e:
        print(f"[GAGAL DATABASE] Tidak bisa membaca Supabase: {e}")
        return []

    if not antrean:
        print("Antrean kosong. Sistem dihentikan.")
        return []

    video_siap_audit = []
    print(f"Menganalisis umur {len(antrean)} video (Waktu Sistem: {waktu_sekarang.strftime('%Y-%m-%d %H:%M:%S')} WITA)")

    for video in antrean:
        try:
            waktu_publikasi = parser.parse(video['published_at'])
            if waktu_publikasi.tzinfo is None:
                waktu_publikasi = waktu_publikasi.replace(tzinfo=zona_wita)
            else:
                waktu_publikasi = waktu_publikasi.astimezone(zona_wita)

            selisih_waktu = waktu_sekarang - waktu_publikasi

            if selisih_waktu >= timedelta(hours=24):
                video_siap_audit.append(video)
                print(f" -> [LOLOS] ID: {video['post_id']} | Publikasi: {waktu_publikasi.strftime('%Y-%m-%d %H:%M')} WITA")
            else:
                print(f" -> [DITUNDA] ID: {video['post_id']} baru berumur {selisih_waktu}")

        except Exception as e:
            print(f" -> [EROR WAKTU] Melewati ID: {video['post_id']}. Masalah: {e}")

    # ==========================================
    # FASE 3: META API, AUDIO EXTRACTION & GROQ TIMESTAMPS
    # ==========================================
    if not video_siap_audit:
        print("\nTidak ada video yang melewati batas 24 jam. Operasi selesai.")
        return []

    print(f"\nMemulai ekstraksi Meta API & Transkripsi untuk {len(video_siap_audit)} Reels...")

    direktori_audio = "audio_output"
    os.makedirs(direktori_audio, exist_ok=True)
    versi_api = "v20.0"
    hasil_analitik = []
    payload_insert_db = []
    id_sukses_diproses = []

    for video in video_siap_audit:
        media_id = video['post_id']
        url_node = f"https://graph.facebook.com/{versi_api}/{media_id}"
        parameter_node = {'fields': "media_url", 'access_token': kredensial['meta_token']}
        url_insights = f"https://graph.facebook.com/{versi_api}/{media_id}/insights"
        parameter_insights = {
            'metric': "views,reach,saved,likes,shares,ig_reels_avg_watch_time",
            'access_token': kredensial['meta_token']
        }

        try:
            requests_node = requests.get(url_node, params=parameter_node).json()
            if 'error' in requests_node:
                print(f" -> [GAGAL API MEDIA] ID: {media_id} | Eror: {requests_node['error']['message']}")
                continue

            media_url_link = requests_node.get('media_url')
            if not media_url_link:
                print(f" -> [GAGAL MEDIA] Tautan video tidak ditemukan.")
                continue

            path_mp4 = f"{direktori_audio}/{media_id}.mp4"
            path_mp3 = f"{direktori_audio}/{media_id}.mp3"

            respons_video = requests.get(media_url_link, stream=True)
            with open(path_mp4, 'wb') as f:
                for chunk in respons_video.iter_content(chunk_size=1024):
                    if chunk:
                        f.write(chunk)

            # Eksekusi FFPROBE & FFMPEG (Wajib terinstal di OS Server)
            hasil_ffprobe = subprocess.run(
                ['ffprobe', '-v', 'error', '-show_entries', 'format=duration', '-of', 'default=noprint_wrappers=1:nokey=1', path_mp4],
                capture_output=True, text=True
            )
            durasi_detik_raw = hasil_ffprobe.stdout.strip()
            durasi_video_ms = int(float(durasi_detik_raw) * 1000) if durasi_detik_raw else 0

            subprocess.run(['ffmpeg', '-i', path_mp4, '-q:a', '0', '-map', 'a', path_mp3, '-y'],
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            
            if os.path.exists(path_mp4):
                os.remove(path_mp4)
            print(f" -> [SUKSES LOKAL] Durasi terdeteksi: {durasi_video_ms} ms")

            respons_meta = requests.get(url_insights, params=parameter_insights).json()
            metrik_raw = {}
            if 'data' in respons_meta:
                for item in respons_meta['data']:
                    metrik_raw[item['name']] = item['values'][0]['value']

            print(f" -> [PROSES] Mengirim audio ID {media_id} ke Groq AI...")
            url_groq = "https://api.groq.com/openai/v1/audio/transcriptions"
            headers_groq = {"Authorization": f"Bearer {kredensial['groq_token']}"}

            with open(path_mp3, "rb") as file_audio:
                files_groq = {"file": (f"{media_id}.mp3", file_audio, "audio/mpeg")}
                data_groq = {
                    "model": "whisper-large-v3",
                    "language": "id",
                    "response_format": "verbose_json",
                    "timestamp_granularities[]": "word"
                }
                respons_groq = requests.post(url_groq, headers=headers_groq, files=files_groq, data=data_groq)

            hasil_transkripsi = respons_groq.json()
            teks_naskah = ""
            if 'words' in hasil_transkripsi:
                list_kata_timestamp = []
                for w in hasil_transkripsi['words']:
                    kata = w.get('word', '')
                    w_start = w.get('start', 0.0)
                    menit = int(w_start // 60)
                    detik = w_start % 60
                    list_kata_timestamp.append(f"{kata} [{menit:02d}:{detik:04.1f}]")
                teks_naskah = " ".join(list_kata_timestamp)
            else:
                teks_naskah = hasil_transkripsi.get('text', '')

            if teks_naskah:
                print(f" -> [SUKSES TRANSKRIPSI] Berhasil mengekstrak naskah.")
            
            if os.path.exists(path_mp3):
                os.remove(path_mp3)

            judul_video = video.get('title', f"Reels_{media_id}")
            metrik_lokal = {
                'post_id': media_id,
                'title': judul_video,
                'views': metrik_raw.get('views', 0),
                'reach': metrik_raw.get('reach', 0),
                'saved': metrik_raw.get('saved', 0),
                'likes': metrik_raw.get('likes', 0),
                'shares': metrik_raw.get('shares', 0),
                'avg_watch_time': metrik_raw.get('ig_reels_avg_watch_time', 0),
                'duration_ms': durasi_video_ms,
                'transcript': teks_naskah
            }
            hasil_analitik.append(metrik_lokal)

            data_sql = {
                'content_id': video['id'],
                'post_id': media_id,
                'title': judul_video,
                'views': metrik_raw.get('views', 0),
                'reach': metrik_raw.get('reach', 0),
                'saved': metrik_raw.get('saved', 0),
                'likes': metrik_raw.get('likes', 0),
                'shares': metrik_raw.get('shares', 0),
                'avg_watch_time': metrik_raw.get('ig_reels_avg_watch_time', 0),
                'duration_ms': durasi_video_ms,
                'transcript': teks_naskah
            }
            payload_insert_db.append(data_sql)
            id_sukses_diproses.append(video['id'])

        except Exception as e:
            print(f" -> [SISTEM EROR] Gagal memproses ID {media_id}: {e}")

    # ==========================================
    # FASE 4: PENYIMPANAN KE TABEL ANALITIK & KUNCI STATUS ANTRIAN
    # ==========================================
    if payload_insert_db:
        print(f"\nMemulai sinkronisasi ke tabel 'content_analytics' Supabase...")
        try:
            supabase.table('content_analytics').insert(payload_insert_db).execute()
            print(f"[SUKSES DATABASE] Berhasil menyimpan {len(payload_insert_db)} baris data analitik.")

            if id_sukses_diproses:
                supabase.table('content_queue').update({'is_analyzed': True}).in_('id', id_sukses_diproses).execute()
                print(f"[STATUS TERKUNCI] {len(id_sukses_diproses)} antrean ditandai selesai.")
        except Exception as e:
            print(f"[GAGAL DATABASE] Eror saat operasi tabel: {e}")

    return hasil_analitik

# ==========================================
# FASE 5: TRIGGER DIFY ORCHESTRATION HUB
# ==========================================
def eksekusi_ai_analyzer(hasil_analitik, token_dify):
    if not hasil_analitik:
        print("\n[BATAL ANALISIS] Tidak ada data untuk dikirim ke Dify.")
        return

    print(f"\nMemulai Fase 5: Mengirim {len(hasil_analitik)} data ke Dify...")
    url_dify = "https://api.dify.ai/v1/workflows/run"
    headers = {
        "Authorization": f"Bearer {token_dify}",
        "Content-Type": "application/json"
    }

    for data in hasil_analitik:
        payload = {
            "inputs": {
                "post_id": str(data['post_id']),
                "title": str(data['title']),
                "views": int(data['views']),
                "reach": int(data['reach']),
                "saved": int(data['saved']),
                "likes": int(data['likes']),
                "shares": int(data['shares']),
                "avg_watch_time": int(data['avg_watch_time']),
                "duration_ms": int(data['duration_ms']),
                "transcript": str(data['transcript'])
            },
            "response_mode": "blocking",
            "user": "sistem-anabion-worker"
        }

        try:
            respons = requests.post(url_dify, json=payload, headers=headers).json()
            if 'data' in respons and respons['data']['status'] == 'succeeded':
                print(f" -> [SUKSES DIFY] Data ID {data['post_id']} berhasil diproses.")
            else:
                print(f" -> [PENOLAKAN DIFY] Gagal memproses. Respons: {respons}")
        except Exception as e:
            print(f" -> [EROR DIFY] Gagal mengirim ID {data['post_id']}: {e}")

if __name__ == "__main__":
    print("Inisialisasi Worker Otomatis Anabion...")
    kredensial_sistem = {
        'supabase_url': os.environ.get('SUPABASE_URL'),
        'supabase_key': os.environ.get('SUPABASE_KEY'),
        'meta_token': os.environ.get('META_ACCESS_TOKEN'),
        'groq_token': os.environ.get('GROQ_API_KEY'),
        'dify_token': os.environ.get('DIFY_API_KEY')
    }

    if not all(kredensial_sistem.values()):
        print("[SISTEM BERHENTI] Variabel lingkungan (Environment Variables) belum dikonfigurasi di server.")
    else:
        data_akhir = eksekusi_pipeline_analitik(kredensial_sistem)
        eksekusi_ai_analyzer(data_akhir, kredensial_sistem['dify_token'])