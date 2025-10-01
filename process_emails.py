import os
import json
import base64
import csv
import re
import io
from datetime import datetime, timezone
from typing import Optional, Dict, List, Tuple
import requests

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from notion_client import Client


class EmailProcessor:
    def __init__(self):
        self.gmail_service = self._init_gmail()
        self.notion = Client(auth=os.environ['NOTION_API_KEY'])
        self.database_id = os.environ['NOTION_DATABASE_ID']
        self.alert_email = os.environ.get('ALERT_EMAIL')
        
        # Quiet mode for production (minimal logging for public repos)
        self.quiet_mode = os.environ.get('QUIET_MODE', 'true').lower() == 'true'
        
        # Email search queries from environment
        self.email1_query = os.environ.get('EMAIL1_SEARCH_QUERY')
        self.email2_query = os.environ.get('EMAIL2_SEARCH_QUERY')
    
    def log(self, message: str, level: str = 'info'):
        """Log message with minimal detail for public logs"""
        if self.quiet_mode:
            # In quiet mode, only log errors and summary
            if level in ['error', 'summary']:
                print(message)
        else:
            print(message)
    
    def _init_gmail(self):
        """Initialize Gmail API service"""
        # Get access token if provided, otherwise will use refresh token
        access_token = os.environ.get('GMAIL_ACCESS_TOKEN', '')
        
        token_data = {
            'refresh_token': os.environ['GMAIL_REFRESH_TOKEN'],
            'token_uri': 'https://oauth2.googleapis.com/token',
            'client_id': os.environ['GMAIL_CLIENT_ID'],
            'client_secret': os.environ['GMAIL_CLIENT_SECRET'],
            'scopes': [
                'https://www.googleapis.com/auth/gmail.readonly',
                'https://www.googleapis.com/auth/gmail.modify',
                'https://www.googleapis.com/auth/gmail.send'
            ]
        }
        
        # Only add access token if it exists
        if access_token:
            token_data['token'] = access_token
        
        creds = Credentials.from_authorized_user_info(token_data)
        
        # Refresh token to get valid access token
        if not creds.valid:
            try:
                creds.refresh(Request())
            except Exception as e:
                self.log(f"Authentication error: {e}", 'error')
                raise
        
        return build('gmail', 'v1', credentials=creds)
    
    def search_email(self, query: str) -> Optional[str]:
        """Search for email using Gmail query and return message ID"""
        try:
            results = self.gmail_service.users().messages().list(
                userId='me',
                q=query,
                maxResults=1
            ).execute()
            
            messages = results.get('messages', [])
            return messages[0]['id'] if messages else None
            
        except HttpError as error:
            self.log(f"Email search error", 'error')
            return None
    
    def get_email_details(self, msg_id: str) -> Dict:
        """Get full email details including attachments and body"""
        try:
            message = self.gmail_service.users().messages().get(
                userId='me',
                id=msg_id,
                format='full'
            ).execute()
            return message
            
        except HttpError as error:
            self.log(f"Error retrieving email", 'error')
            return {}
    
    def extract_attachment(self, msg_id: str, message: Dict) -> Optional[str]:
        """Extract CSV attachment from email"""
        try:
            parts = message.get('payload', {}).get('parts', [])
            
            for part in parts:
                if part.get('filename', '').endswith('.csv'):
                    attachment_id = part['body'].get('attachmentId')
                    
                    if attachment_id:
                        attachment = self.gmail_service.users().messages().attachments().get(
                            userId='me',
                            messageId=msg_id,
                            id=attachment_id
                        ).execute()
                        
                        data = attachment['data']
                        file_data = base64.urlsafe_b64decode(data)
                        return file_data.decode('utf-8')
            
            return None
            
        except Exception as error:
            self.log(f"Attachment extraction error", 'error')
            return None
    
    def extract_csv_link(self, message: Dict) -> Optional[str]:
        """Extract CSV link from email body"""
        try:
            parts = message.get('payload', {}).get('parts', [])
            body_html = ''
            body_text = ''
            
            def extract_body(part):
                nonlocal body_html, body_text
                mime_type = part.get('mimeType', '')
                
                if mime_type == 'text/html':
                    data = part.get('body', {}).get('data', '')
                    if data:
                        body_html += base64.urlsafe_b64decode(data).decode('utf-8', errors='ignore')
                elif mime_type == 'text/plain':
                    data = part.get('body', {}).get('data', '')
                    if data:
                        body_text += base64.urlsafe_b64decode(data).decode('utf-8', errors='ignore')
                
                # Recursively check sub-parts
                if 'parts' in part:
                    for subpart in part['parts']:
                        extract_body(subpart)
            
            # Extract all body content
            for part in parts:
                extract_body(part)
            
            # If no parts, try direct body
            if not body_html and not body_text:
                data = message.get('payload', {}).get('body', {}).get('data', '')
                if data:
                    decoded = base64.urlsafe_b64decode(data).decode('utf-8', errors='ignore')
                    # Check if it's HTML
                    if '<html' in decoded.lower() or '<a ' in decoded.lower():
                        body_html = decoded
                    else:
                        body_text = decoded
            
            # Try HTML first (most reliable for embedded links)
            if body_html:
                # Extract href attributes from <a> tags
                href_pattern = r'<a[^>]+href=["\']([^"\']+)["\'][^>]*>'
                hrefs = re.findall(href_pattern, body_html, re.IGNORECASE)
                
                # Look for CSV or S3 links
                for href in hrefs:
                    if '.csv' in href.lower() or 's3.amazonaws.com' in href.lower():
                        return href
                
                # If no CSV-specific links, try general URL extraction from HTML
                url_pattern = r'https?://[^\s<>"\']+(?:\.csv|s3\.amazonaws\.com[^\s<>"\']*)'
                urls = re.findall(url_pattern, body_html, re.IGNORECASE)
                if urls:
                    return urls[0]
            
            # Try plain text as fallback
            if body_text:
                csv_links = re.findall(r'https?://[^\s<>"]+\.csv[^\s<>"]*', body_text, re.IGNORECASE)
                if csv_links:
                    return csv_links[0]
                
                # Look for S3 links even without .csv extension
                s3_links = re.findall(r'https?://[^\s<>"]*s3\.amazonaws\.com[^\s<>"]+', body_text, re.IGNORECASE)
                if s3_links:
                    return s3_links[0]
                
                # Generic links as last resort
                links = re.findall(r'https?://[^\s<>"]+', body_text)
                for link in links:
                    if 'csv' in link.lower() or 'export' in link.lower():
                        return link
            
            return None
            
        except Exception as error:
            self.log(f"Link extraction error", 'error')
            return None
    
    def download_csv(self, url: str) -> Optional[str]:
        """Download CSV from URL"""
        try:
            # Some URLs may require authentication or have expired
            # Try with a timeout and handle various HTTP errors
            response = requests.get(url, timeout=30, allow_redirects=True)
            response.raise_for_status()
            return response.text
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 403:
                self.log(f"CSV download error: Access forbidden (link may have expired)", 'error')
            elif e.response.status_code == 404:
                self.log(f"CSV download error: File not found", 'error')
            else:
                self.log(f"CSV download error: HTTP {e.response.status_code}", 'error')
            return None
        except Exception as error:
            self.log(f"CSV download error", 'error')
            return None
    
    def parse_csv_source1(self, csv_content: str) -> List[Dict]:
        """Parse first CSV source and extract relevant fields"""
        reader = csv.DictReader(io.StringIO(csv_content))
        entries = []
        
        # Get field mappings from environment
        source_name = os.environ.get('CSV1_SOURCE_NAME', 'Source1')
        amount_field = os.environ.get('CSV1_AMOUNT_FIELD')
        id_field = os.environ.get('CSV1_ID_FIELD')
        date_field = os.environ.get('CSV1_DATE_FIELD')
        
        for row in reader:
            try:
                order_id = row.get(id_field, '').strip()
                date_str = row.get(date_field, '').strip()
                
                # Skip rows with empty order ID or date (likely total/summary rows)
                if not order_id or not date_str:
                    continue
                
                # Skip rows where order_id is not a number (like "Total", "Summary", etc)
                if not order_id.replace('.', '', 1).isdigit():
                    continue
                
                entry = {
                    'source': source_name,
                    'order_amount': float(row.get(amount_field, 0)),
                    'order_id': order_id,
                    'order_date': self._parse_date(date_str)
                }
                entries.append(entry)
                
            except Exception as e:
                continue
        
        return entries
    
    def parse_csv_source2(self, csv_content: str) -> List[Dict]:
        """Parse second CSV source (skip first row) and extract relevant fields"""
        lines = csv_content.strip().split('\n')
        
        # Skip first row if configured
        skip_rows = int(os.environ.get('CSV2_SKIP_ROWS', 0))
        if skip_rows > 0 and len(lines) > skip_rows:
            lines = lines[skip_rows:]
        
        reader = csv.DictReader(io.StringIO('\n'.join(lines)))
        entries = []
        current_year = datetime.now().year
        
        # Get field mappings from environment
        source_name = os.environ.get('CSV2_SOURCE_NAME', 'Source2')
        amount_field = os.environ.get('CSV2_AMOUNT_FIELD')
        id_field = os.environ.get('CSV2_ID_FIELD')
        date_field = os.environ.get('CSV2_DATE_FIELD')
        
        for row in reader:
            try:
                date_str = row.get(date_field, '').strip()
                order_id = row.get(id_field, '').strip()
                
                # Skip rows with empty order ID or date (likely total/summary rows)
                if not order_id or not date_str:
                    continue
                
                # Skip rows where order_id is not a number (like "Total", "Summary", etc)
                if not order_id.replace('.', '', 1).isdigit():
                    continue
                
                # Additional check: Skip rows where the first column contains "Total" or "Summary" (case-insensitive)
                first_col_value = list(row.values())[0].strip().lower() if row.values() else ''
                if first_col_value in ['total', 'summary', 'subtotal', 'grand total']:
                    continue
                
                order_date = self._parse_date_with_year(date_str, current_year)
                
                # Skip if date parsing returned empty or invalid date
                if not order_date or order_date.strip() == '':
                    continue
                
                entry = {
                    'source': source_name,
                    'order_amount': float(row.get(amount_field, 0)),
                    'order_id': order_id,
                    'order_date': order_date
                }
                entries.append(entry)
                
            except Exception as e:
                continue
        
        return entries
    
    def _parse_date(self, date_str: str) -> str:
        """Parse date string to ISO format"""
        if not date_str or date_str.strip() == '':
            # Return today's date if empty
            return datetime.now().strftime('%Y-%m-%d')
            
        try:
            formats = [
                '%Y-%m-%d',
                '%m/%d/%Y',
                '%d/%m/%Y',
                '%Y-%m-%d %H:%M:%S',
                '%m/%d/%Y %H:%M:%S',
                '%d-%m-%Y',
                '%Y/%m/%d',
            ]
            
            for fmt in formats:
                try:
                    dt = datetime.strptime(date_str.strip(), fmt)
                    return dt.strftime('%Y-%m-%d')
                except ValueError:
                    continue
            
            # If no format matches, return today's date
            return datetime.now().strftime('%Y-%m-%d')
            
        except Exception:
            return datetime.now().strftime('%Y-%m-%d')
    
    def _parse_date_with_year(self, date_str: str, year: int) -> str:
        """Parse date string without year and add current year"""
        try:
            formats = [
                '%m/%d',
                '%d/%m',
                '%b %d',
                '%B %d',
            ]
            
            for fmt in formats:
                try:
                    dt = datetime.strptime(date_str, fmt)
                    dt = dt.replace(year=year)
                    return dt.strftime('%Y-%m-%d')
                except ValueError:
                    continue
            
            # Try with year already
            return self._parse_date(date_str)
            
        except Exception:
            return f"{year}-01-01"
    
    def check_notion_entry_exists(self, order_id: str) -> Optional[str]:
        """Check if order already exists in Notion, return page ID if found"""
        try:
            response = self.notion.databases.query(
                database_id=self.database_id,
                filter={
                    "property": "Order ID",
                    "number": {
                        "equals": int(order_id) if order_id.isdigit() else 0
                    }
                }
            )
            
            results = response.get('results', [])
            return results[0]['id'] if results else None
            
        except Exception as e:
            return None
    
    def create_or_update_notion_entry(self, entry: Dict):
        """Create or update entry in Notion database"""
        try:
            # Skip entries with empty order_id
            if not entry.get('order_id') or str(entry['order_id']).strip() == '':
                return
            
            # Check if entry exists
            existing_page_id = self.check_notion_entry_exists(entry['order_id'])
            
            # Get additional property names from environment
            source_property = os.environ.get('NOTION_SOURCE_PROPERTY', 'Source')
            amount_property = os.environ.get('NOTION_AMOUNT_PROPERTY', 'Order Amount')
            id_property = os.environ.get('NOTION_ID_PROPERTY', 'Order ID')
            date_property = os.environ.get('NOTION_DATE_PROPERTY', 'Order Date')
            checkbox_property = os.environ.get('NOTION_CHECKBOX_PROPERTY', 'Sum-er')
            
            properties = {
                source_property: {
                    "title": [{"text": {"content": entry['source']}}]
                },
                amount_property: {
                    "number": entry['order_amount']
                },
                id_property: {
                    "number": int(entry['order_id']) if entry['order_id'].isdigit() else 0
                },
                date_property: {
                    "date": {
                        "start": entry['order_date'] if entry['order_date'] and entry['order_date'].strip() else datetime.now().strftime('%Y-%m-%d'),
                        "end": None,
                        "time_zone": None
                    }
                },
                checkbox_property: {
                    "checkbox": True
                }
            }
            
            if existing_page_id:
                self.notion.pages.update(
                    page_id=existing_page_id,
                    properties=properties
                )
            else:
                self.notion.pages.create(
                    parent={"database_id": self.database_id},
                    properties=properties
                )
                
        except Exception as e:
            self.log(f"Notion error", 'error')
    
    def archive_or_delete_email(self, msg_id: str):
        """Archive email from Gmail inbox (or trash as fallback)"""
        try:
            # Try to archive (remove from inbox) - requires gmail.modify scope
            self.gmail_service.users().messages().modify(
                userId='me',
                id=msg_id,
                body={'removeLabelIds': ['INBOX']}
            ).execute()
            
        except HttpError as error:
            # If archive fails, try moving to trash
            if error.resp.status == 403:
                try:
                    self.gmail_service.users().messages().trash(
                        userId='me',
                        id=msg_id
                    ).execute()
                except HttpError as trash_error:
                    self.log(f"Cannot archive or trash email", 'error')
            else:
                self.log(f"Email archiving error", 'error')
    
    def send_alert_email(self, missing_reports: List[str]):
        """Send alert email for missing reports"""
        if not self.alert_email:
            return
            
        try:
            missing = ' and '.join(missing_reports)
            
            # Create RFC 2822 compliant message
            message_text = f"To: {self.alert_email}\r\n"
            message_text += f"Subject: Report Processing Alert\r\n"
            message_text += f"\r\n"
            message_text += f"Reports not found: {missing}\n\n"
            message_text += f"Automated processing could not complete due to missing email reports.\n"
            
            message = {
                'raw': base64.urlsafe_b64encode(message_text.encode()).decode()
            }
            
            self.gmail_service.users().messages().send(
                userId='me',
                body=message
            ).execute()
            
        except HttpError as error:
            self.log(f"Alert email error", 'error')
        except Exception as error:
            self.log(f"Alert email error", 'error')
    
    def process(self):
        """Main processing logic"""
        self.log("Starting processing", 'summary')
        
        missing_reports = []
        processed_emails = []
        entries_created = 0
        
        # Get source names for reporting
        source1_name = os.environ.get('CSV1_SOURCE_NAME', 'Source1')
        source2_name = os.environ.get('CSV2_SOURCE_NAME', 'Source2')
        
        # Process first email (CSV link in body)
        self.log(f"\nProcessing {source1_name}")
        email1_id = self.search_email(self.email1_query)
        
        if email1_id:
            self.log(f"Email found")
            
            message = self.get_email_details(email1_id)
            csv_link = self.extract_csv_link(message)
            
            if csv_link:
                self.log(f"CSV link found")
                csv_content = self.download_csv(csv_link)
                
                if csv_content:
                    entries = self.parse_csv_source1(csv_content)
                    self.log(f"Parsed {len(entries)} entries")
                    
                    for entry in entries:
                        self.create_or_update_notion_entry(entry)
                        entries_created += 1
                    
                    processed_emails.append((source1_name, email1_id))
                else:
                    self.log("CSV download failed", 'error')
                    missing_reports.append(source1_name)
            else:
                self.log("CSV link not found", 'error')
                missing_reports.append(source1_name)
        else:
            self.log("Email not found", 'error')
            missing_reports.append(source1_name)
        
        # Process second email (CSV attachment)
        self.log(f"\nProcessing {source2_name}")
        email2_id = self.search_email(self.email2_query)
        
        if email2_id:
            self.log(f"Email found")
            
            message = self.get_email_details(email2_id)
            csv_content = self.extract_attachment(email2_id, message)
            
            if csv_content:
                self.log("CSV attachment extracted")
                entries = self.parse_csv_source2(csv_content)
                self.log(f"Parsed {len(entries)} entries")
                
                for entry in entries:
                    self.create_or_update_notion_entry(entry)
                    entries_created += 1
                
                processed_emails.append((source2_name, email2_id))
            else:
                self.log("CSV attachment not found", 'error')
                missing_reports.append(source2_name)
        else:
            self.log("Email not found", 'error')
            missing_reports.append(source2_name)
        
        # Cleanup
        self.log("\nCleanup")
        for source, msg_id in processed_emails:
            self.archive_or_delete_email(msg_id)
        
        # Send alert if needed
        if missing_reports:
            self.log(f"\nMissing reports: {', '.join(missing_reports)}", 'error')
            self.send_alert_email(missing_reports)
        
        self.log("\nProcessing complete", 'summary')
        self.log(f"Processed: {len(processed_emails)} emails", 'summary')
        self.log(f"Entries synced: {entries_created}", 'summary')
        self.log(f"Missing: {len(missing_reports)} reports", 'summary')


if __name__ == "__main__":
    try:
        processor = EmailProcessor()
        processor.process()
        
    except Exception as e:
        print(f"Fatal error occurred")
        import traceback
        traceback.print_exc()
        exit(1)
