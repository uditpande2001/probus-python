import smtplib
from os.path import basename
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
import logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(module)s - %(lineno)d - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')


def send_mail(path, hour):
    try:
        from_addr = 'uditpande2001@gmail.com'
        to_addr = 'adityamittal@probus.io'
        subject = 'All data for nodes'
        content = 'PFA'

        msg = MIMEMultipart()
        msg['From'] = from_addr
        msg['To'] = to_addr
        msg['Subject'] = subject
        body = MIMEText(content, 'plain')
        msg.attach(body)

        filename = f'{hour}.csv'
        with open(path, 'r') as f:
            pass
            part = MIMEApplication(f.read(), Name=basename(filename))
            part['Content-Disposition'] = 'attachment; filename="{}"'.format(basename(filename))
        msg.attach(part)
        logging.info("Sending Email")

        # __________use this for sending multiple files_____________ #
        # csv_files = ['path_to_csv_1', 'path_to_csv_2']
        # for file in csv_files:
        #     with open(file, 'rb') as file:
        #         # Attach the file with filename to the email
        #         msg.attach(MIMEApplication(file.read(), Name=FILE_NAME))

        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(from_addr, 'rrdunudhfgdfwfol')
        server.send_message(msg, from_addr=from_addr, to_addrs=[to_addr])
        logging.info('Email sent')

    except Exception as error:
        print(error)
        print('Email not sent')
