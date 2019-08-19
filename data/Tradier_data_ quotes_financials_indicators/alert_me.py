'''Module for SMS and Email alert functions'''
#pylint: disable = line-too-long, too-many-lines, no-name-in-module, import-error, multiple-imports, pointless-string-statement, wrong-import-order

import boto3
from botocore.exceptions import ClientError
import os

#Config
AWS_ACCESS_KEY_ID = os.environ['ALERT_ME_AAKI']
AWS_SECRET_ACCESS_KEY = os.environ['ALERT_ME_ASAK']
AWS_DEFAULT_REGION = os.environ['ALERT_ME_ADR']
SNS_TOPIC_ARN = os.environ['ALERT_ME_STA']


def sms(number: str, message: str) -> str:
    '''Send SMS'''
    try:
        client = boto3.client("sns",
                              aws_access_key_id=AWS_ACCESS_KEY_ID,
                              aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                              region_name=AWS_DEFAULT_REGION)
        topicarn = SNS_TOPIC_ARN
        client.subscribe(TopicArn=topicarn, Protocol='sms', Endpoint=number)
        client.publish(Message=message, TopicArn=topicarn)
        return 'Alert created'
    except Exception as exception:
        raise exception


def email(email_id: str,
          message: str,
          sender: str = "alerts@kloudtrader.com",
          subject: str = "KloudTrader Narwhal Alerts") -> str:
    '''Send Email'''
    client = boto3.client('ses', region_name=AWS_DEFAULT_REGION)
    try:
        client.send_email(
            Destination={
                'ToAddresses': [
                    email_id,
                ],
            },
            Message={
                'Body': {
                    'Text': {
                        'Charset': "UTF-8",
                        'Data': message,
                    },
                },
                'Subject': {
                    'Charset': "UTF-8",
                    'Data': subject,
                },
            },
            Source=sender,
        )
        return 'Alert created'
    except ClientError as exception:
        raise exception.response['Error']['Message']


def sms_and_email(number: str, email_id: str, message: str) -> str:
    '''Send both SMS and Email at once'''
    try:
        sms(number, message)
        email(email_id, message)
        return 'Alert created'
    except Exception as exception:
        raise exception
