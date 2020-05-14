import sendgrid
import os
from sendgrid.helpers.mail import Email, Content, Mail


class Mailer(object):
    def __init__(self):

        if os.environ["SENDGRID_API_KEY"] is None:
            raise EnvironmentError("Missing env SENDGRID_API_KEY")

        self.sendgrid_client = sendgrid.SendGridAPIClient(
            apikey=os.environ["SENDGRID_API_KEY"])

    def send_mail(self, from_address, to_address, subject, body, content_type="text/plain"):
        from_email = Email(from_address)
        to_email = Email(to_address)
        subject = subject
        content = Content(content_type, body)

        mail = Mail(from_email, subject, to_email, content)
        response = self.sendgrid_client.client.mail.send.post(
            request_body=mail.get())
        return response


if __name__ == '__main__':
    mailer = Mailer()
    response = mailer.send_mail(
        from_address="test@example.com",
        to_address="mulanimonty@gmail.com",
        subject="Subject - Test mail",
        body="Test mail ABC")

    print(response.status_code)
    print(response.body)
    print(response.headers)
