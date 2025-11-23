from playwright.sync_api import sync_playwright
from urllib.parse import urljoin
import time
import os
import datetime
import subprocess
import json
from helper_logger import call_import_date_dim_procedure, update_status_by_id,get_latest_today_process_log
from mail import send_email


#1.1.0 Lấy ngày hiện tại để đặt tên file
today = datetime.datetime.now().strftime("%Y-%m-%d")


def startJar():
    #1.1.3 Chạy file jar và nhận kết quả trả về 
    jar_path = r"D:\DW\file_jar\LoadConfig1-1.0-SNAPSHOT.jar"
    args = ["5","D:\\DW\\configs.json"]
    result = subprocess.run(
        ["java", "-jar", jar_path] + args,
        capture_output=True,
        text=True
    )
    stdout = result.stdout.strip()
    if not stdout:
        #1.2.4 Gửi email thông báo lỗi không kết nối được database
        send_email(f'không kết nối được vô database control crawl_links {today}')
    else:
        try:
        #1.1.4 Chuyển đổi chuỗi JSON sang đối tượng Python    
            job = json.loads(stdout)
            print(job)
        except json.JSONDecodeError as e:
        #1.3.5 Gửi email thông báo lỗi không parse được dữ liệu từ json sang dữ liệu bên python    
            send_email(f'không parse được dữ liệu từ json sang dữ liệu bên python {today}')
            return          
    return job
2
def startCrawl(job, process_id):
    #1.1.8 Bắt đầu cào link games
    url = job['jobConfig']['sourcePath']
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        page = browser.new_page(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                           "AppleWebKit/537.36 (KHTML, like Gecko) "
                                           "Chrome/118.0.0.0 Safari/537.36")
        page.goto(url, wait_until="domcontentloaded", timeout=360000)
        try:
            span_stop = page.wait_for_selector('.Mc7qX.Vporl.Q5X10', timeout=5000)
            if span_stop:
                print("Tìm thấy popup, click để đóng...")
                span_stop.click(force=True)
                time.sleep(1)
                print("Popup đã được đóng!")
        except Exception:
            print("Không thấy popup, tiếp tục crawl...")

        list_games = []
        try:
            while True:
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                time.sleep(2)
                games = page.query_selector_all("div.y83ib")
                current_count = len(games)
                print(f"Đã load được {current_count} game...")
                button = page.query_selector(".MFcmt.sc-88j5gv-0.YALb._3LMnG.xN-5A")
                if not button:
                    print("Hết nút load!")
                    break
                button.click()
                time.sleep(3)
            for game in games:
                a = game.query_selector('a.VoZI3._9kt1Z')
                if a:
                    href = a.get_attribute('href')
                    if href:
                        link_full = urljoin(url, href)
                        list_games.append(link_full)
            folder = job['jobConfig']['pathSaveFile']
            os.makedirs(folder, exist_ok=True)
            save_path = os.path.join(folder, f"nintendo_links_{today}.csv")
            #1.1.9 Cào thành công thì cập nhập lại trang thái trong process_log của tiến tình và gửi mail và lưu vô file.csv
            with open(save_path, 'w', encoding='utf-8') as f:
                f.write("link\n")
                for link in list_games:
                    f.write(link + '\n')
            
            update_status_by_id(job, process_id, 'DONE')
            send_email(f'Links đã được lưu vào: {save_path} và Tổng số game load được: {len(games)} vào ngày {today}')

        except Exception as e:
            #1.6.9 Cào thất bại và gửi mail và cập nhập dữ liệu trong database
            update_status_by_id(job, process_id, 'ERROR')
            send_email(f'Cào dữ liệu thất bại {today}')
            return 

        finally:
            browser.close()


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
             process_id=call_import_date_dim_procedure('Bắt đầu tiến trình 5','Bắt đầu tiến trình 5','PROCESSING',job)
        except:
             #1.5.7 gửi báo lỗi nếu không insert vô được database
             send_email(f'Không insert dữ liệu được vào database {today}')
             return
        
   #1.1.7 chạy hàm startCrawl với hai tham số job và process_id
   startCrawl(job,process_id)
   
#1.1.1 Bắt đầu chạy hàm main   
main()


