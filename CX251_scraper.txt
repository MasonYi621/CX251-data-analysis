
"""
CX251航班2025年全年延误数据爬取脚本
部分代码由AI辅助生成
作者：易鑫杰
用途：论文《基于动态规划与概率建模的超长程航班燃油装载量优化策略》数据收集模块
日期：2026年1月21日
依赖：pip install requests beautifulsoup4 pandas
"""
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta
import time

# 核心配置参数
AIRLINE_CODE = "CX"  # 国泰航空IATA代码
FLIGHT_NUMBER = "251"  # 航班号
BASE_URL = f"https://www.flightradar24.com/data/flights/{AIRLINE_CODE}{FLIGHT_NUMBER}"  # FR24公开数据页面
START_DATE = datetime(2025, 1, 1)  # 数据起始日期
END_DATE = datetime(2025, 12, 31)  # 数据结束日期
OUTPUT_CSV = "cx251_2025_delay_data.csv"  # 输出文件名称
REQUEST_DELAY = 1  # 请求间隔（秒），遵循网站robots.txt规范
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

# 初始化数据存储列表
flight_delay_data = []

# 遍历日期范围
current_date = START_DATE
while current_date <= END_DATE:
    # 格式化日期为YYYY-MM-DD格式（FR24页面要求）
    date_str = current_date.strftime("%Y-%m-%d")
    print(f"正在爬取 {date_str} CX251航班数据...")
    
    # 构造当日航班数据请求URL
    request_url = f"{BASE_URL}/{date_str}"
    
    # 构造请求头，模拟浏览器访问
    headers = {"User-Agent": USER_AGENT}
    
    try:
        # 发送GET请求获取页面内容
        response = requests.get(request_url, headers=headers, timeout=30)
        response.raise_for_status()  # 抛出HTTP请求错误（如404、500）
        
        # 解析HTML页面
        soup = BeautifulSoup(response.text, "html.parser")
        
        # 提取计划起飞时间（Scheduled Departure）
        scheduled_dep = None
        scheduled_elements = soup.find_all(
            "div", 
            class_=lambda c: c and any(keyword in c.lower() for keyword in ["scheduled", "planned", "departure"])
        )
        for elem in scheduled_elements:
            if elem.text.strip() and ":" in elem.text.strip():
                scheduled_dep = elem.text.strip()
                break
        
        # 提取实际起飞时间（Actual Departure）
        actual_dep = None
        actual_elements = soup.find_all(
            "div", 
            class_=lambda c: c and any(keyword in c.lower() for keyword in ["actual", "departed", "real"])
        )
        for elem in actual_elements:
            if elem.text.strip() and ":" in elem.text.strip():
                actual_dep = elem.text.strip()
                break
        
        # 计算延误时长（分钟）：实际起飞时间 - 计划起飞时间
        delay_min = None
        if scheduled_dep and actual_dep:
            try:
                # 解析时间格式（HH:MM）
                scheduled_time = datetime.strptime(scheduled_dep, "%H:%M")
                actual_time = datetime.strptime(actual_dep, "%H:%M")
                
                # 计算时间差（处理跨日情况，如实际起飞为次日00:30，计划为23:50）
                time_diff = (actual_time - scheduled_time).total_seconds() / 60
                delay_min = max(0, time_diff)  # 延误为负时视为0（提前起飞）
            except ValueError:
                # 时间格式解析失败（如页面显示"Canceled"）
                delay_min = None
        
        # 存储当日数据
        flight_delay_data.append({
            "flight_date": date_str,
            "scheduled_departure": scheduled_dep,
            "actual_departure": actual_dep,
            "delay_minutes": delay_min,
            "data_source": request_url  # 记录数据来源URL，便于溯源
        })
        
        # 礼貌性延时，避免高频请求导致IP封禁
        time.sleep(REQUEST_DELAY)
    
    except requests.exceptions.RequestException as e:
        # 处理请求异常（如网络错误、超时、HTTP错误）
        print(f"爬取 {date_str} 数据失败：{str(e)}")
        flight_delay_data.append({
            "flight_date": date_str,
            "scheduled_departure": None,
            "actual_departure": None,
            "delay_minutes": None,
            "data_source": request_url,
            "error_info": str(e)
        })
    
    # 日期迭代
    current_date += timedelta(days=1)

# 转换为DataFrame并保存为CSV文件
df_delay = pd.DataFrame(flight_delay_data)
df_delay.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
print(f"\n数据爬取完成！共获取 {len(df_delay)} 条记录，已保存至 {OUTPUT_CSV}")
print(f"有效延误数据：{df_delay['delay_minutes'].notna().sum()} 条")