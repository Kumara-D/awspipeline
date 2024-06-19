import boto3
import os
from xml.etree import ElementTree as ET

# Initialize SES client
ses = boto3.client('ses', region_name='your-region')  # Specify your region

# Developer emails configuration
developers = {
    "DMDOMITINV00091125.py": "Kumara.Devegowda@ibsplc.com",
    "DMDOMITINV00091126.py": "Kumara.Devegowda@ibsplc.com",
    "DMDOMITINV00091127.py": "Kumara.Devegowda@ibsplc.com",
    # Add more mappings as needed
}

def parse_junit_report(report_path):
    results = {}
    tree = ET.parse(report_path)
    root = tree.getroot()
    for testcase in root.iter('testcase'):
        script_name = testcase.attrib['name']
        failure = testcase.find('failure')
        result = 'PASS' if failure is None else f'FAIL: {failure.text}'
        results[script_name] = result
    return results

def send_email(to_email, script_name, result):
    subject = f"Test Result for {script_name}"
    body = f"The result for {script_name} is:\n\n{result}"
    response = ses.send_email(
        Source='your-email@example.com',  # Replace with your verified SES email
        Destination={
            'ToAddresses': [
                to_email,
            ],
        },
        Message={
            'Subject': {
                'Data': subject,
                'Charset': 'UTF-8'
            },
            'Body': {
                'Text': {
                    'Data': body,
                    'Charset': 'UTF-8'
                }
            }
        }
    )
    return response

def main():
    report_path = 'report.xml'
    
    # Parse the JUnit report
    results = parse_junit_report(report_path)
    
    # Send emails
    for script, result in results.items():
        if script in developers:
            send_email(developers[script], script, result)

if __name__ == "__main__":
    main()
