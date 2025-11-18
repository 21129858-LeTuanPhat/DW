
import smtplib                           
from email.mime.text import MIMEText     
from email.mime.multipart import MIMEMultipart  


def send_email(text):
    sender_email = "ltphat240103@gmail.com"         
    sender_password = "uivp qpmf audj xqea"         
    receiver_email = "ltphat240103@gmail.com"      
    msg = MIMEMultipart()
    msg["From"] = sender_email          
    msg["To"] = receiver_email          
    msg["Subject"] = "Test email từ Python" 
    body = text 
    msg.attach(MIMEText(body, "plain"))
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(sender_email, sender_password)
            server.send_message(msg)
            print(" Gửi mail thành công!")

    except Exception as e:
        print(" Lỗi khi gửi mail:", e)
