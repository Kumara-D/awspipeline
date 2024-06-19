import boto3
import os
import xml.etree.ElementTree as ET

# Initialize SES client
ses = boto3.client('ses', region_name='ap-south-1')  # Specify your region

# Developer emails configuration
developers = {
    "DMDOMITINV00091125.py": "Kumara.Devegowda@ibsplc.com",
    "DMDOMITINV00091126.py": "Kumara.Devegowda@ibsplc.com",
    "DMDOMITINV00091127.py": "Kumara.Devegowda@ibsplc.com"
    # Add more mappings as needed
}

def parse_results(report_path):
    results = {}
    tree = ET.parse(report_path)
    root = tree.getroot()

    for testcase in root.findall('.//testcase'):
        script_name = testcase.get('name')
        failure = testcase.find('failure')
        if failure is not None:
            results[script_name] = f"FAIL: {failure.text.strip()}"
        else:
            results[script_name] = "PASS"

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
    results = parse_results(report_path)
    
    for script, result in results.items():
        if script in developers:
            send_email(developers[script], script, result)

if __name__ == "__main__":
    main()
