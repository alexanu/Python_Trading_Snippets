from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage

import boto3

class Email(object):

    def __init__(self,subject, to=['mark.refermat@gmail.com']):
        self.to = to
        self.subject = subject
        self.text = None
        self.attachment = None
        self.msg = MIMEMultipart()
        
    def add_text(self, text):
        msgText = MIMEText(text,'html')
        self.msg.attach(msgText)

    def add_attachment(self, file_name):
        msgText = MIMEText('<br><img src="cid:%s"><br>' %  file_name, 'html')  
        self.msg.attach(msgText) 
        fp=open(file_name,'rb')
        self.attachment = MIMEImage(fp.read())
        fp.close()
        self.attachment.add_header('Content-ID', '<{}>'.format(file_name))
        self.msg.attach(self.attachment)
        
    def add_attachments(self, list_of_files):
        m=''
        for file_name in list_of_files:
            m=m+'<br><img src="cid:%s"><br>' %  file_name       
        msgText = MIMEText(m, 'html')  
        self.msg.attach(msgText) 
        for file_name in list_of_files:    
            fp=open(file_name,'rb')
            self.attachment = MIMEImage(fp.read())
            fp.close()
            self.attachment.add_header('Content-ID', '<{}>'.format(file_name))
            self.msg.attach(self.attachment)
        
    def send(self):
        from_addr='mrefermat@hotmail.com'
        connection = boto3.client('ses',region_name="us-east-1")
        self.msg['Subject'] = self.subject
        self.msg['From'] = from_addr
        #self.msg['To'] = self.to
        self.msg['To'] = ','.join(self.to)

        return connection.send_raw_email(RawMessage={'Data': self.msg.as_string()},
                                         Source=from_addr,
                                         Destinations=self.to)

