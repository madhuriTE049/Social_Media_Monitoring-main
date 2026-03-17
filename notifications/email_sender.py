import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from core.config import SMTP_SERVER, SMTP_PORT, SMTP_USER, SMTP_PASSWORD


# def send_email(recipients, subject, html_body):

#     msg = MIMEMultipart()
#     #msg["From"] = "NoReply <{}>".format(SMTP_USER)
#     msg["From"] = formataddr(("NoReply", SMTP_USER))
#     msg["Subject"] = subject

#     msg.attach(MIMEText(html_body, "html"))

#     server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
#     server.starttls()
#     server.login(SMTP_USER, SMTP_PASSWORD)

#     for email in recipients:
#         msg["To"] = email
#         server.sendmail(SMTP_USER, email, msg.as_string())

#     server.quit()

def send_email(recipients, subject, html_body):

    msg = MIMEMultipart()
    msg["From"] = "NoReply <{}>".format(SMTP_USER)
    msg["To"] = ", ".join(recipients)
    msg["Subject"] = subject

    msg.attach(MIMEText(html_body, "html"))

    server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
    server.starttls()
    server.login(SMTP_USER, SMTP_PASSWORD)

    server.sendmail(SMTP_USER, recipients, msg.as_string())

    server.quit()