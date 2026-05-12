"""
src/utils_telegram.py
Telegram notification utility
"""
import requests

def send_telegram_message(token, chat_id, message):
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        'chat_id': chat_id,
        'text': message,
        'parse_mode': 'Markdown'
    }
    try:
        resp = requests.post(url, json=payload, timeout=10)
        return resp.json()
    except Exception as e:
        print(f"Telegram notification failed: {e}")
        return None

def send_telegram_photo(token, chat_id, photo_path, caption=None):
    url = f"https://api.telegram.org/bot{token}/sendPhoto"
    try:
        with open(photo_path, 'rb') as photo:
            files = {'photo': photo}
            data = {'chat_id': chat_id}
            if caption:
                data['caption'] = caption
                data['parse_mode'] = 'Markdown'
            resp = requests.post(url, data=data, files=files, timeout=20)
            return resp.json()
    except Exception as e:
        print(f"Telegram photo failed: {e}")
        return None
