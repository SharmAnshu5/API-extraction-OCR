# email_service.py

import os
import smtplib
import logging
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.utils import formatdate

SMTP_HOST = "10.59.50.13"
SMTP_PORT = 587
logger = logging.getLogger("ocr_email")
logger = logging.getLogger("booking_logic")


def run_booking_logic(fields, conn=None):

    try:
        # Example business validations

        if float(fields.get("RO_AMOUNT") or 0) <= 0:
            logger.error("Invalid RO Amount")
            return False

        if fields.get("CLIENT_CODE") is None:
            logger.error("Client code missing")
            return False

        # You can add:
        # - Rate validation
        # - Package validation
        # - Duplicate RO check
        # - Agency blacklist check

        logger.info(f"Booking Logic Passed | RO {fields.get('RO_NO')}")
        return True

    except Exception as e:
        logger.error(f"Booking Logic Exception: {e}", exc_info=True)
        return False



def send_issue_email_adops(adbook, issue, conn=None):

    try:
        sender = "noreply@amarujala.com"
        recipient = "anshu.sharma@mrt.amarujala.com"
        cc_list = ["deal-support@mrt.amarujala.com", "anshusharma5.as@gmail.com"]
        if isinstance(issue, (list, tuple)):
            reasons = [str(x) for x in issue]
        else:
            reasons = [str(issue)]
        subject = (
            f"[OCR Automation Alert] "
            f"{adbook.get('RO_NO', 'UNKNOWN')} | "
            f"{', '.join(reasons)}"
        )
        msg = MIMEMultipart()
        msg["From"] = sender
        msg["To"] = recipient
        msg["Cc"] = ",".join(cc_list)
        msg["Subject"] = subject
        msg["Date"] = formatdate(localtime=True)
        body = f"""
Dear Team,

An issue occurred in OCR Automation.

Issue Type:
{chr(10).join(['- ' + r for r in reasons])}

Agency        : {adbook.get('AGENCY_NAME', 'N/A')}
Client        : {adbook.get('CLIENT_NAME', 'N/A')}
Client Code   : {adbook.get('CLIENT_CODE', 'N/A')}
RO Number     : {adbook.get('RO_NO', 'N/A')}
RO Date       : {adbook.get('RO_DATE', 'N/A')}
Insert Date   : {adbook.get('INSERT_DATE', 'N/A')}

Please review the attached RO.

Regards,
IT Automation System
"""
        msg.attach(MIMEText(body, "plain"))
        pdf_path = adbook.get("PDF_PATH")
        if pdf_path and os.path.exists(pdf_path):
            with open(pdf_path, "rb") as f:
                attach = MIMEApplication(f.read(), _subtype="pdf")
                attach.add_header(
                    "Content-Disposition",
                    "attachment",
                    filename=os.path.basename(pdf_path)
                )
                msg.attach(attach)

        server = smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=15)
        server.ehlo()
        server.sendmail(sender, [recipient] + cc_list, msg.as_string())
        server.quit()
        logger.info(f"Issue Email Sent | RO: {adbook.get('RO_NO')}")

        return True

    except Exception as e:
        logger.error(f"Issue Email Failed: {e}", exc_info=True)
        return False


def send_booking_mail(adbook, conn=None):

    try:
        sender = "noreply@amarujala.com"
        recipient = "anshu.sharma@mrt.amarujala.com"
        cc_list = ["deal-support@mrt.amarujala.com", "anshusharma5.as@gmail.com"]

        subject = (
            f"[OCR Booking Success] "
            f"RO {adbook.get('RO_NO', 'N/A')} | "
            f"{adbook.get('AGENCY_NAME', '')}"
        )

        msg = MIMEMultipart()
        msg["From"] = sender
        msg["To"] = recipient
        msg["Cc"] = ",".join(cc_list)
        msg["Subject"] = subject
        msg["Date"] = formatdate(localtime=True)

        body = f"""
Dear Team,

The OCR extraction and booking process completed successfully.

Agency        : {adbook.get('AGENCY_NAME', 'N/A')}
Client        : {adbook.get('CLIENT_NAME', 'N/A')}
Client Code   : {adbook.get('CLIENT_CODE', 'N/A')}
RO Number     : {adbook.get('RO_NO', 'N/A')}
RO Date       : {adbook.get('RO_DATE', 'N/A')}
Insert Date   : {adbook.get('INSERT_DATE', 'N/A')}
Package       : {adbook.get('PACKAGE_NAME', 'N/A')}
Amount        : {adbook.get('RO_AMOUNT', 'N/A')}

Regards,
IT Automation System
"""

        msg.attach(MIMEText(body, "plain"))

        # Attach PDF
        pdf_path = adbook.get("PDF_PATH")
        if pdf_path and os.path.exists(pdf_path):
            with open(pdf_path, "rb") as f:
                attach = MIMEApplication(f.read(), _subtype="pdf")
                attach.add_header(
                    "Content-Disposition",
                    "attachment",
                    filename=os.path.basename(pdf_path)
                )
                msg.attach(attach)

        server = smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=15)
        server.ehlo()
        server.sendmail(sender, [recipient] + cc_list, msg.as_string())
        server.quit()

        return True

    except Exception as e:
        logger.error(f"Success Email Failed: {e}", exc_info=True)
        return False



