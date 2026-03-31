from flask import Flask, request, jsonify
import os
import requests
import json

app = Flask(__name__)

TOKEN = "8635930527:AAHPRDuYQK1SQRK6V6G2WpjcYA-fu_O3VAY"
TELEGRAM_API = f"https://api.telegram.org/bot{TOKEN}"

@app.route('/')
def home():
    return "🎮 Mafia Bot is running! 🚀"

@app.route('/webhook', methods=['POST'])
def webhook():
    data = request.get_json()
    
    try:
        # Basic message handling
        if 'message' in data:
            chat_id = data['message']['chat']['id']
            text = data['message'].get('text', '')
            
            if text == '/start':
                response_text = "🎮 Welcome to Mafia Bot!\n\nCommands:\n/start - Start bot\n/help - Get help\n/game - Start game\n/profile - View profile"
                send_message(chat_id, response_text)
            elif text == '/help':
                response_text = "📚 Mafia Bot Help:\n\n/start - Start bot\n/help - Get help\n/game - Start game in group\n/profile - View your profile\n/shop - Buy items\n/roles - View roles"
                send_message(chat_id, response_text)
            else:
                response_text = f"🎮 You said: {text}\n\nUse /help for commands"
                send_message(chat_id, response_text)
        
        return jsonify({"status": "ok"})
    
    except Exception as e:
        print(f"Error: {e}")
        return jsonify({"status": "error", "message": str(e)})

def send_message(chat_id, text):
    url = f"{TELEGRAM_API}/sendMessage"
    payload = {
        'chat_id': chat_id,
        'text': text,
        'parse_mode': 'HTML'
    }
    requests.post(url, json=payload)

@app.route('/health')
def health():
    return jsonify({"status": "healthy", "bot": "Mafia Bot"})

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8443))
    app.run(host='0.0.0.0', port=port, debug=True)
