import os
import boto3
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

# Function to send email
def send_email(sender, receiver, subject, body, attachment_path):
    # Initialize SES client
    ses_client = boto3.client('ses', region_name='ap-south-1')

    # Create email message
    msg = MIMEMultipart()
    msg['From'] = sender
    msg['To'] = receiver
    msg['Subject'] = subject

    # Attach text body
    msg.attach(MIMEText(body, 'plain'))

    # Attach report file
    with open(attachment_path, 'rb') as file:
        attachment = MIMEApplication(file.read(), Name=os.path.basename(attachment_path))
    attachment['Content-Disposition'] = f'attachment; filename="{os.path.basename(attachment_path)}"'
    msg.attach(attachment)

    # Send email
    response = ses_client.send_raw_email(
        Source=sender,
        Destinations=[receiver],
        RawMessage={'Data': msg.as_string()}
    )

    return response

if __name__ == "__main__":
    # Developer email addresses
    developer_emails = {
        "DMDOMITINV00091125.py": "kumard25101995@gmail.com",
        "DMDOMITINV00091126.py": "kumard25101995@gmail.com",
        "DMDOMITINV00091127.py": "kumard25101995@gmail.com",
    }

    # Path to the JUnit XML report
    report_path = "report.xml"  # Update this path as per your report location

    # Sender information
    sender_email = "kumard25101995@gmail.com"
    subject = "Test Report"

    # Iterate over the developer emails and send emails
    for script_name, email in developer_emails.items():
        body = f"Hello, please find the attached report for {script_name}."

        response = send_email(sender_email, email, subject, body, report_path)
        print(f"Email sent to {script_name}: {response}")
