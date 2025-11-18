from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import time
import os
import datetime
import csv
import re
import subprocess
import json
from helper_logger import call_import_date_dim_procedure, update_status_by_id,get_latest_today_process_log
from mail  import send_email

#1.1.0 Lấy ngày hiện tại để đặt tên file
today = datetime.datetime.now().strftime("%Y-%m-%d")

def startJar():
    #1.1.3 Chạy file jar và nhận kết quả trả về 
    jar_path = r"D:\DW\file_jar\LoadConfig1-1.0-SNAPSHOT.jar"
    args = ["6","D:\\DW\\configs.json"]
    result = subprocess.run(
        ["java", "-jar", jar_path] + args,
        capture_output=True,
        text=True
    )
    stdout = result.stdout.strip()
    if not stdout:
        #1.2.4 Gửi email thông báo lỗi không kết nối được database
        send_email(f'không kết nối được vô database control craw_detail_game {today}')
    else:
        try:
            #1.1.4 Chuyển đổi chuỗi JSON sang đối tượng Python
            job = json.loads(stdout)
        except json.JSONDecodeError as e:
            #1.3.5 Gửi email thông báo lỗi không parse được dữ liệu từ json sang dữ liệu bên python
            send_email(f'không parse được dữ liệu từ json sang dữ liệu bên python {today}')
    return job


def crawl(job,process_id):
    
    
    folder = job['jobConfig']['sourcePath']
    output_folder = job['jobConfig']['pathSaveFile']
    os.makedirs(output_folder, exist_ok=True)
    expected_filename = f"nintendo_links_{today}.csv"
    file_path = os.path.join(folder, expected_filename)
    #1.1.8 Kiểm tra coi hôn nay có file link để cào game không
    if os.path.exists(file_path):
        print(f"Đã tìm thấy file hôm nay: {expected_filename}")
        list_links = []
        with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                next(reader)
                for row in reader:
                    if row:  
                        list_links.append(row[0])   
        scraped_data = []  
        batch_size = 10
        #1.1.9 Bắt đầu cào dữ liệu chi tiết từ các link trong file csv
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            context = browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36")
            page = context.new_page()
            # Chia batch 10 link/lần
            for i in range(0, len(list_links[:300]), batch_size):
                batch_links = list_links[i:i+batch_size]
                print(f"Đang xử lý batch {i//batch_size + 1}: {len(batch_links)} link")
                for idx, link in enumerate(batch_links, start=i+1):
                    try:
                        page.goto(link, wait_until="load", timeout=360000)
                    except PlaywrightTimeoutError:
                        print(f"Timeout khi truy cập {link}, tiếp tục thôi")
                        continue
                    # Lấy name
                    name_element = page.query_selector(".s954l._3TUsN._39p7O")
                    name = name_element.inner_text().strip() if name_element else "Chưa có"
                    # Lấy price
                    price_element = page.query_selector(".W990N")
                    if price_element:
                        price = price_element.inner_text().replace("Regular Price:", "").strip()
                    else:
                        # fallback dùng regex để đảm bảo nếu không có .W990N vẫn lấy được
                        html = page.content()
                        match = re.search(r"\$\d+\.\d{2}", html)
                        price = match.group(0) if match else "Chưa có"
                    # Scroll xuống cuối trang
                    page.evaluate("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(1)           
                    # Khởi tạo tất cả thuộc tính mặc định "Chưa có"
                    developer_list,genre_list,size_list,support_list, of_player_list, system_list, publisher_list, language_list, release_date_list = [], [], [], [], [], [],[],[],[]
                    developers=genres=size=support = of_plays = systems = publishers = languages = release_date = "Chưa có"

                    for parent in page.query_selector_all(".sc-1237z5p-2.fjIvYK"):
                        h3 = parent.query_selector('.s954l.cjYUi._39p7O')
                        if not h3:
                            continue
                        h3_content = h3.inner_text().strip()
                        div_4s = parent.query_selector_all(".sc-1237z5p-4.fHqHTF")
                        if h3_content == "Game file size (estimated)" or h3_content == "Game file size":
                            for item in div_4s:
                                div = item.query_selector('div')
                                if div:
                                    size_list.append(div.inner_text().strip())
                            size=" , ".join(size_list) if size_list else "Chưa có"
                        elif h3_content == "Supported play modes":
                            for item in div_4s:
                                div = item.query_selector('div')
                                if div:
                                    support_list.append(div.inner_text().strip())
                            support = " , ".join(support_list) if support_list else "Chưa có"
                        elif h3_content == "No. of players":
                            for item in div_4s:
                                div = item.query_selector('div')
                                if div:
                                    of_player_list.append(div.inner_text().strip())
                            of_plays = " , ".join(of_player_list) if of_player_list else "Chưa có"
                        elif h3_content == "Genre":
                            for item in div_4s:
                                div = item.query_selector('div')
                                if div:
                                    genre_list.append(div.inner_text().strip())
                            genres = " , ".join(genre_list) if genre_list else "Chưa có"    
                        elif h3_content == "System":
                            for item in div_4s:
                                div = item.query_selector('div')
                                if div:
                                    system_list.append(div.inner_text().strip())
                            systems = " , ".join(system_list) if system_list else "Chưa có"
                        elif h3_content == "Publisher":
                            for item in div_4s:
                                div = item.query_selector('div')
                                if div:
                                    publisher_list.append(div.inner_text().strip())
                            publishers = " , ".join(publisher_list) if publisher_list else "Chưa có"
                        elif h3_content == "Developer":
                            for item in div_4s:
                                div = item.query_selector('div')
                                if div:
                                    developer_list.append(div.inner_text().strip())
                            developers = " , ".join(publisher_list) if publisher_list else "Chưa có"
                        elif h3_content == "Supported languages":
                            for item in div_4s:
                                div = item.query_selector('div')
                                if div:
                                    language_list.append(div.inner_text().strip())
                            languages = " , ".join(language_list) if language_list else "Chưa có"
                        elif h3_content == "Release date":
                            for item in div_4s:
                                div = item.query_selector('div')
                                if div:
                                    release_date_list.append(div.inner_text().strip())
                            release_date = " , ".join(release_date_list) if release_date_list else "Chưa có"
                    scraped_data.append({
                         "stt": idx,
                         "name": name,
                         "price": price,
                         "size":size,
                         "support": support,
                         "of_players": of_plays,
                         "genre":genres,
                         "system": systems,
                         "publisher": publishers,
                         "developer": developers,
                         "languages": languages,
                         "release_date": release_date
                        })
                # Thêm delay sau mỗi batch để nhẹ nhàng
                time.sleep(2)
            context.close()
            browser.close()
        # --- Ghi CSV ---
            #1.1.10 nếu cào có dữ liệu thì lưu vô file csv và cập nhập trạng thái process_log trong database và gửi mail báo thành công
            if scraped_data:
                output_file = os.path.join(output_folder, f"nintendo_details_{today}.csv")
                fieldnames = ['stt', 'name', 'price','size', 'support', 'of_players','genre','system', 'publisher','developer','languages', 'release_date']
                with open(output_file, 'w', newline='', encoding='utf-8-sig') as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(scraped_data)
                update_status_by_id(job,process_id,'DONE')
                send_email(f'Đã cào được dữ liệu thành công {today}')
            else:
            #1.7.10 nếu cào không có dữ liệu thì báo lỗi và cập nhập trạng thái process_log trong database
                send_email(f'không cào được dữ liệu chi tiết từ các đường link vào ngày {today}')
                update_status_by_id(job,process_id,'ERROR')
    else:
        #1.6.9 Gửi mail báo không có file và cập nhập status của tiến trình trong dataset
        send_email(f'ngày {today} không có file CSV nào trong thư mục!')
        update_status_by_id(job,process_id,'ERROR')
    
    
def main():
    #1.1.2 bắt đầu chạy method startJar() 
    job=startJar()
    
    #1.1.5 Bắt đầu chạy hàm get_latest_today_process_log(job) để kiểm tra coi thử có thằng nào đang chạy không
    exist_object=get_latest_today_process_log(job)
    if exist_object and exist_object.get("status") == "PROCESSING":
        #1.4.6 gửi báo lỗi nếu không insert vô được database
       send_email(f'Có một tiến trình khác đang chạy nên không chạy được {today}') 
       return
    else:
        try:
           #1.1.6 bắt đầu chạy method call_import_date_dim_procedure() với các tham số truyền vào
           #là tenTienTrinh, mota, status, job
           process_id=call_import_date_dim_procedure('Bắt đầu tiến trình 6','Bắt đầu tiến trình 6','PROCESSING',job)
        except:
           #1.5.7 gửi báo lỗi nếu không insert vô được database
           send_email(f'Không insert dữ liệu được vào database {today}') 
           return 
       
    #1.1.7 chạy hàm startCrawl với hai tham số job và process_id    
    crawl(job,process_id)    
                
    
    
#1.1.1 Bắt đầu chạy hàm main         
main()
      