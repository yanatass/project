#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import serial
import json
import time
import csv
import os
from datetime import datetime, timedelta
from flask import Flask, render_template, jsonify, send_file, request
from flask_socketio import SocketIO
from collections import deque
import threading
import sqlite3
import pandas as pd
from io import BytesIO

app = Flask(__name__)
app.config['SECRET_KEY'] = 'air-quality-secret'
app.config['DATA_FOLDER'] = 'data'
app.config['DATABASE'] = 'air_quality.db'
socketio = SocketIO(app, cors_allowed_origins="*")

# Создаем папку для данных
os.makedirs(app.config['DATA_FOLDER'], exist_ok=True)

# Очередь для хранения данных
data_history = deque(maxlen=168)  # 7 дней * 24 часа = 168 часов
current_data = {}

# Настройки Serial порта
SERIAL_PORT = '/dev/cu.usbserial-120'
BAUD_RATE = 115200

# Настройки эксперимента - 1-часовой интервал
SAMPLE_INTERVAL = 3600  # 1 час в секундах

# Переменные для накопления данных
accumulated_data = []
last_sample_time = None
accumulation_start_time = None  # Время начала накопления данных

def init_database():
    """Инициализация базы данных"""
    conn = sqlite3.connect(app.config['DATABASE'])
    c = conn.cursor()
    
    # Таблица для сырых данных
    c.execute('''CREATE TABLE IF NOT EXISTS sensor_data
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                  pm1 REAL,
                  pm25 REAL,
                  pm10 REAL,
                  temperature REAL,
                  humidity REAL,
                  aqi INTEGER,
                  quality TEXT)''')
    
    # Таблица для часовых образцов
    c.execute('''CREATE TABLE IF NOT EXISTS hourly_data
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  timestamp DATETIME,
                  pm1 REAL,
                  pm25 REAL,
                  pm10 REAL,
                  temperature REAL,
                  humidity REAL,
                  sample_count INTEGER)''')
    
    # Упрощенная таблица метаданных
    c.execute('''CREATE TABLE IF NOT EXISTS experiment_meta
                 (id INTEGER PRIMARY KEY,
                  experiment_name TEXT DEFAULT 'Air Quality Experiment',
                  start_time DATETIME DEFAULT CURRENT_TIMESTAMP,
                  total_samples INTEGER DEFAULT 0,
                  sampling_interval TEXT DEFAULT '1 hour',
                  last_sample_time DATETIME)''')
    
    conn.commit()
    conn.close()
    print("✅ Database initialized")

def calculate_aqi(pm25):
    """Расчет AQI на основе PM2.5"""
    if pm25 <= 12.0:
        return int((50.0 / 12.0) * pm25)
    elif pm25 <= 35.4:
        return int(50 + (50.0 / 23.4) * (pm25 - 12.1))
    elif pm25 <= 55.4:
        return int(100 + (50.0 / 20.0) * (pm25 - 35.5))
    elif pm25 <= 150.4:
        return int(150 + (50.0 / 94.9) * (pm25 - 55.5))
    elif pm25 <= 250.4:
        return int(200 + (100.0 / 99.9) * (pm25 - 150.5))
    else:
        return int(300 + (200.0 / 249.9) * (min(pm25, 500.4) - 250.5))

def get_aqi_level(aqi):
    """Определение уровня AQI"""
    if aqi <= 50:
        return "Good"
    elif aqi <= 100:
        return "Moderate"
    elif aqi <= 150:
        return "Unhealthy"
    else:
        return "Hazardous"

def get_air_quality_color(aqi):
    """Определение цвета качества воздуха на основе AQI"""
    if aqi <= 50:
        return '#4CAF50'
    elif aqi <= 100:
        return '#FFC107'
    elif aqi <= 150:
        return '#FF9800'
    else:
        return '#F44336'

def save_hourly_sample(sample_data):
    """Сохранение часового усредненного образца"""
    try:
        conn = sqlite3.connect(app.config['DATABASE'])
        c = conn.cursor()
        
        c.execute('''INSERT INTO hourly_data 
                     (timestamp, pm1, pm25, pm10, temperature, humidity, sample_count)
                     VALUES (?, ?, ?, ?, ?, ?, ?)''',
                  (sample_data['timestamp'],
                   sample_data['pm1_avg'],
                   sample_data['pm25_avg'],
                   sample_data['pm10_avg'],
                   sample_data['temp_avg'],
                   sample_data['hum_avg'],
                   sample_data['sample_count']))
        
        aqi = calculate_aqi(sample_data['pm25_avg'])
        quality = get_aqi_level(aqi)
        
        c.execute('''INSERT INTO sensor_data 
                     (pm1, pm25, pm10, temperature, humidity, aqi, quality)
                     VALUES (?, ?, ?, ?, ?, ?, ?)''',
                  (sample_data['pm1_avg'],
                   sample_data['pm25_avg'],
                   sample_data['pm10_avg'],
                   sample_data['temp_avg'],
                   sample_data['hum_avg'],
                   aqi,
                   quality))
        
        c.execute('''UPDATE experiment_meta 
                     SET total_samples = total_samples + 1,
                         last_sample_time = ?
                     WHERE id = 1''',
                  (sample_data['timestamp'],))
        
        conn.commit()
        conn.close()
        
        print(f"✅ Hourly sample saved: PM2.5={sample_data['pm25_avg']:.1f}, AQI={aqi}, Quality={quality}")
        return True
        
    except Exception as e:
        print(f"❌ Error saving sample: {e}")
        return False

def process_accumulated_data():
    """Обработка накопленных данных и создание часового образца"""
    global accumulated_data, accumulation_start_time
    
    if not accumulated_data:
        print("⚠️ No accumulated data to process")
        return None
    
    sample_count = len(accumulated_data)
    
    pm1_sum = sum(d.get('pm1', 0) for d in accumulated_data)
    pm25_sum = sum(d.get('pm25', 0) for d in accumulated_data)
    pm10_sum = sum(d.get('pm10', 0) for d in accumulated_data)
    temp_sum = sum(d.get('temperature', 0) for d in accumulated_data)
    hum_sum = sum(d.get('humidity', 0) for d in accumulated_data)
    
    sample_data = {
        'timestamp': datetime.now().isoformat(),
        'pm1_avg': pm1_sum / sample_count if sample_count > 0 else 0,
        'pm25_avg': pm25_sum / sample_count if sample_count > 0 else 0,
        'pm10_avg': pm10_sum / sample_count if sample_count > 0 else 0,
        'temp_avg': temp_sum / sample_count if sample_count > 0 else 0,
        'hum_avg': hum_sum / sample_count if sample_count > 0 else 0,
        'sample_count': sample_count
    }
    
    print(f"📊 Processed {sample_count} samples over 1 hour: PM2.5={sample_data['pm25_avg']:.1f}")
    
    # Очищаем накопленные данные и устанавливаем новое время начала накопления
    accumulated_data = []
    accumulation_start_time = time.time()
    
    return sample_data

def read_serial_data():
    """Чтение данных с Arduino с автоподключением"""
    global current_data, accumulated_data, last_sample_time, accumulation_start_time
    
    while True:
        try:
            print(f"🔌 Connecting to Arduino on {SERIAL_PORT}...")
            ser = serial.Serial(SERIAL_PORT, BAUD_RATE, timeout=1)
            print(f"✅ Connected to Arduino on {SERIAL_PORT}")
            
            time.sleep(2)
            ser.reset_input_buffer()
            
            raw_data_count = 0
            
            while True:
                try:
                    if ser.in_waiting > 0:
                        line = ser.readline().decode('utf-8', errors='ignore').strip()
                        
                        if line and line.startswith('{') and line.endswith('}'):
                            try:
                                data = json.loads(line)
                                raw_data_count += 1
                                
                                if raw_data_count % 30 == 0:
                                    print(f"📊 Raw #{raw_data_count}: PM2.5={data.get('pm25', 'N/A')}, Temp={data.get('temperature', 'N/A')}")
                                
                                accumulated_data.append(data)
                                
                                current_data = {
                                    'pm1': data.get('pm1', 0),
                                    'pm25': data.get('pm25', 0),
                                    'pm10': data.get('pm10', 0),
                                    'temperature': data.get('temperature', 0),
                                    'humidity': data.get('humidity', 0),
                                    'timestamp': datetime.now().isoformat(),
                                    'time': datetime.now().strftime('%H:%M:%S'),
                                    'date': datetime.now().strftime('%Y-%m-%d'),
                                    'raw_data_count': raw_data_count,
                                    'accumulated_count': len(accumulated_data),
                                    'status': 'connected'
                                }
                                
                                socketio.emit('sensor_data', current_data)
                                
                                current_time = time.time()
                                if current_time - accumulation_start_time >= SAMPLE_INTERVAL:
                                    print(f"⏰ 1 hour passed. Processing {len(accumulated_data)} samples...")
                                    
                                    sample_data = process_accumulated_data()
                                    if sample_data:
                                        save_hourly_sample(sample_data)
                                        
                                        aqi = calculate_aqi(sample_data['pm25_avg'])
                                        quality = get_aqi_level(aqi)
                                        color = get_air_quality_color(aqi)
                                        
                                        dashboard_data = {
                                            'pm1': round(sample_data['pm1_avg'], 1),
                                            'pm25': round(sample_data['pm25_avg'], 1),
                                            'pm10': round(sample_data['pm10_avg'], 1),
                                            'temperature': round(sample_data['temp_avg'], 1),
                                            'humidity': round(sample_data['hum_avg'], 1),
                                            'aqi': aqi,
                                            'quality': quality,
                                            'color': color,
                                            'timestamp': sample_data['timestamp'],
                                            'time': datetime.now().strftime('%H:%M:%S'),
                                            'date': datetime.now().strftime('%Y-%m-%d'),
                                            'sample_count': sample_data['sample_count'],
                                            'is_hourly_sample': True
                                        }
                                        
                                        data_history.append(dashboard_data)
                                        socketio.emit('hourly_sample', dashboard_data)
                                        print(f"📈 Hourly avg: PM2.5={dashboard_data['pm25']}, AQI={aqi}, Quality={quality}")
                                    
                                    last_sample_time = current_time
                                
                            except json.JSONDecodeError:
                                print(f"⚠️ JSON error in line: {line[:50]}...")
                            except Exception as e:
                                print(f"⚠️ Data processing error: {e}")
                    
                    time.sleep(0.1)
                    
                except (OSError, serial.SerialException) as e:
                    print(f"⚠️ Serial error: {e}")
                    break
            
            ser.close()
            print("🔄 Connection lost. Reconnecting in 5 seconds...")
            
            current_data = {
                'status': 'disconnected',
                'message': 'Arduino disconnected. Reconnecting...',
                'time': datetime.now().strftime('%H:%M:%S'),
                'date': datetime.now().strftime('%Y-%m-%d')
            }
            socketio.emit('sensor_data', current_data)
            
            time.sleep(5)
            
        except serial.SerialException as e:
            print(f"❌ Connection failed: {e}")
            
            current_data = {
                'status': 'error',
                'message': f'Connection failed: {str(e)}',
                'time': datetime.now().strftime('%H:%M:%S'),
                'date': datetime.now().strftime('%Y-%m-%d')
            }
            socketio.emit('sensor_data', current_data)
            
            time.sleep(10)

def calculate_progress(start_time, interval):
    """Расчет прогресса накопления данных"""
    if start_time is None:
        return 0
    elapsed = time.time() - start_time
    progress = min(100, int((elapsed / interval) * 100))
    return progress

def get_remaining_time(start_time, interval):
    """Получение оставшегося времени до следующего семпла"""
    if start_time is None:
        return interval
    elapsed = time.time() - start_time
    remaining = max(0, interval - elapsed)
    return remaining

# ========== Flask Routes ==========

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/hourly_samples')
def get_hourly_samples():
    """Получение всех часовых образцов с AQI"""
    try:
        conn = sqlite3.connect(app.config['DATABASE'])
        
        query = """
        SELECT 
            hd.*,
            CASE 
                WHEN hd.pm25 <= 12.0 THEN ROUND((50.0 / 12.0) * hd.pm25)
                WHEN hd.pm25 <= 35.4 THEN ROUND(50 + (50.0 / 23.4) * (hd.pm25 - 12.1))
                WHEN hd.pm25 <= 55.4 THEN ROUND(100 + (50.0 / 20.0) * (hd.pm25 - 35.5))
                WHEN hd.pm25 <= 150.4 THEN ROUND(150 + (50.0 / 94.9) * (hd.pm25 - 55.5))
                WHEN hd.pm25 <= 250.4 THEN ROUND(200 + (100.0 / 99.9) * (hd.pm25 - 150.5))
                ELSE ROUND(300 + (200.0 / 249.9) * (hd.pm25 - 250.5))
            END as aqi,
            CASE 
                WHEN hd.pm25 <= 12.0 THEN 'Good'
                WHEN hd.pm25 <= 35.4 THEN 'Moderate'
                WHEN hd.pm25 <= 55.4 THEN 'Unhealthy'
                ELSE 'Hazardous'
            END as quality_level
        FROM hourly_data hd
        ORDER BY hd.timestamp DESC 
        LIMIT 168
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        if not df.empty and 'aqi' in df.columns:
            df['aqi'] = df['aqi'].astype(int)
        
        return jsonify(df.to_dict('records'))
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/statistics')
def get_statistics():
    """Статистика эксперимента"""
    try:
        conn = sqlite3.connect(app.config['DATABASE'])
        
        stats_query = """
        SELECT 
            COUNT(*) as total_hours,
            MIN(timestamp) as first_sample,
            MAX(timestamp) as last_sample,
            AVG(pm25) as avg_pm25,
            MAX(pm25) as max_pm25,
            MIN(pm25) as min_pm25,
            AVG(temperature) as avg_temp,
            AVG(humidity) as avg_humidity
        FROM hourly_data
        """
        
        df_stats = pd.read_sql_query(stats_query, conn)
        
        last7d_query = """
        SELECT * FROM hourly_data 
        WHERE timestamp >= datetime('now', '-7 days')
        ORDER BY timestamp
        """
        
        df_7d = pd.read_sql_query(last7d_query, conn)
        
        current_progress = 0
        remaining_time = SAMPLE_INTERVAL
        samples_collected = len(accumulated_data)
        
        if accumulation_start_time:
            current_progress = calculate_progress(accumulation_start_time, SAMPLE_INTERVAL)
            remaining_time = get_remaining_time(accumulation_start_time, SAMPLE_INTERVAL)
        
        conn.close()
        
        return jsonify({
            'statistics': df_stats.to_dict('records')[0] if not df_stats.empty else {},
            'last_7days': df_7d.to_dict('records'),
            'current_data': current_data,
            'accumulation': {
                'progress': current_progress,
                'remaining_time': remaining_time,
                'samples_collected': samples_collected,
                'next_sample_in': remaining_time
            }
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/export/last_7days')
def export_last_7days():
    """Экспорт данных за последние 7 дней"""
    try:
        conn = sqlite3.connect(app.config['DATABASE'])
        
        query = """
        SELECT 
            hd.*,
            CASE 
                WHEN hd.pm25 <= 12.0 THEN ROUND((50.0 / 12.0) * hd.pm25)
                WHEN hd.pm25 <= 35.4 THEN ROUND(50 + (50.0 / 23.4) * (hd.pm25 - 12.1))
                WHEN hd.pm25 <= 55.4 THEN ROUND(100 + (50.0 / 20.0) * (hd.pm25 - 35.5))
                WHEN hd.pm25 <= 150.4 THEN ROUND(150 + (50.0 / 94.9) * (hd.pm25 - 55.5))
                WHEN hd.pm25 <= 250.4 THEN ROUND(200 + (100.0 / 99.9) * (hd.pm25 - 150.5))
                ELSE ROUND(300 + (200.0 / 249.9) * (hd.pm25 - 250.5))
            END as aqi,
            CASE 
                WHEN hd.pm25 <= 12.0 THEN 'Good'
                WHEN hd.pm25 <= 35.4 THEN 'Moderate'
                WHEN hd.pm25 <= 55.4 THEN 'Unhealthy'
                ELSE 'Hazardous'
            END as quality_level
        FROM hourly_data hd
        WHERE timestamp >= datetime('now', '-7 days')
        ORDER BY timestamp DESC
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        output = BytesIO()
        df.to_csv(output, index=False)
        output.seek(0)
        
        filename = f"air_quality_hourly_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        return send_file(
            output,
            mimetype='text/csv',
            as_attachment=True,
            download_name=filename
        )
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/current_progress')
def get_current_progress():
    """Получение текущего прогресса накопления данных"""
    global accumulation_start_time, accumulated_data
    
    if accumulation_start_time:
        progress = calculate_progress(accumulation_start_time, SAMPLE_INTERVAL)
        remaining = get_remaining_time(accumulation_start_time, SAMPLE_INTERVAL)
        
        return jsonify({
            'progress': progress,
            'remaining': remaining,
            'samples_collected': len(accumulated_data),
            'next_sample_in': remaining
        })
    else:
        return jsonify({
            'progress': 0,
            'remaining': SAMPLE_INTERVAL,
            'samples_collected': 0,
            'next_sample_in': SAMPLE_INTERVAL
        })

if __name__ == '__main__':
    init_database()
    
    conn = sqlite3.connect(app.config['DATABASE'])
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM experiment_meta")
    if c.fetchone()[0] == 0:
        c.execute("INSERT INTO experiment_meta (id) VALUES (1)")
        conn.commit()
        print("✅ Experiment record created")
    
    c.execute("UPDATE experiment_meta SET sampling_interval = '1 hour' WHERE id = 1")
    conn.commit()
    conn.close()
    
    # Инициализируем время накопления
    accumulation_start_time = time.time()
    last_sample_time = time.time()
    
    serial_thread = threading.Thread(target=read_serial_data, daemon=True)
    serial_thread.start()
    
    print("\n" + "="*60)
    print("🌡️  Air Quality Experiment Dashboard")
    print("="*60)
    print(f"📁 Data folder: {os.path.abspath(app.config['DATA_FOLDER'])}")
    print(f"💾 Database: {app.config['DATABASE']}")
    print(f"⏱️  Sampling interval: 1 HOUR")
    print(f"📊 Samples shown: Last 7 days (168 hours)")
    print(f"🌐 Dashboard: http://localhost:5001")
    print("="*60)
    print(f"🕐 First hourly sample will be ready in 1 hour")
    print("="*60)
    
    socketio.run(app, host='0.0.0.0', port=5001, debug=False, allow_unsafe_werkzeug=True)
