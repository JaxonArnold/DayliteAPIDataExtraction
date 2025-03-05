#!/usr/bin/env python3

import requests
import re
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
import threading
from typing import List, Dict
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DayliteAPI:
    def __init__(self, token: str, max_workers: int = 100):
        """
        Initialize the Daylite API client with threading support
        
        Args:
            token: API authentication token
            max_workers: Maximum number of concurrent threads
        """
        self.headers = {
            "accept": "application/json",
            "Authorization": f"Bearer {token}"
        }
        self.base_url = "https://api.marketcircle.net"
        self.max_workers = max_workers
        
        # Create a session for connection pooling
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        
        # Thread-safe storage
        self._lock = threading.Lock()
        self._results = Queue()
        
        # Custom mapping of extra field keys to human-readable names
        self.extra_field_names = {
            "com.marketcircle.daylite/extra1": "Model #'s",
            "com.marketcircle.daylite/extra2": "",
            "com.marketcircle.daylite/extra3": "DCA/Distrib",
            "com.marketcircle.daylite/extra4": "Client Level",
            "com.marketcircle.daylite/extra5": "Meeting Freq",
            "com.marketcircle.daylite/extra6": "Fee Rate",
            "com.marketcircle.daylite/extra7": "Spouse/Kids",
            "com.marketcircle.daylite/extra8": "Referred By",
            "com.marketcircle.daylite/extra9": "Expected Rev",
            "com.marketcircle.daylite/extra10": "His FGA Risk",
            "com.marketcircle.daylite/extra11": "Her FGA Risk",
            # Add more mappings as you discover them
        }

    def _make_request(self, url: str) -> Dict:
        """Thread-safe method to make API requests with error handling"""
        try:
            response = self.session.get(url)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logging.error(f"Error making request to {url}: {str(e)}")
            return None

    def extract_ids(self) -> List[str]:
        """Extract all contact IDs using multiple threads"""
        response = self._make_request(f"{self.base_url}/v1/contacts")
        if not response:
            return []
            
        ids = []
        for contact in response:
            burl = contact.get("self", "")
            match = re.search(r"/v1/contacts/(\d+)", burl)
            if match:
                ids.append(match.group(1))
        return ids

    def _process_contact(self, contact_id: str) -> None:
        """Process a single contact's information"""
        url = f"{self.base_url}/v1/contacts/{contact_id}"
        data = self._make_request(url)
        if not data:
            return

        name = data.get('full_name', f'MissingName{contact_id}')
        name = name.translate({ord('/'): '', ord('\\'): ''})
        filename = f"{name[:20].strip()}.txt"

        contact_info = []
        contact_info.append(name)
        contact_info.append(data.get('details', ''))
        contact_info.append(f"Keywords: {' '.join(data.get('keywords', ''))}")
        contact_info.append(f"Birthday: {data.get('birthday', '')}")
        contact_info.append(f"Anniversary: {data.get('anniversary', '')}")
        
        # Process address
        try:
            address = data['addresses'][0]
            contact_info.append(f"{address['city']}, {address['state']}")
        except (KeyError, IndexError):
            contact_info.append("No location found")

        # Process company information
        try:
            company = data['companies'][0]
            company_data = self._make_request(f"{self.base_url}{company['company']}")
            company_name = company_data['name'] if company_data else "Unknown Company"
            contact_info.append(f"Job: {company['role']} at {company_name}")
        except (KeyError, IndexError):
            contact_info.append("No job information found")

        # Process extra fields
        extra_fields = data.get('extra_fields', {})
        if extra_fields:
            contact_info.append("\nExtra Fields:")
            for key, field in extra_fields.items():
                if field.get('value'):  # Only add non-empty fields
                    # Use custom mapping if available, fall back to display name, then key
                    display_name = (
                        self.extra_field_names.get(key) or 
                        field.get('display_name', key)
                    )
                    contact_info.append(f" - {display_name}: {field['value']}")    
            
            
        # Process forms
        try:
            form_ids = [form['form'].split('/')[-1] for form in data['forms']]
            forms_data = self._process_forms(form_ids)
            contact_info.append(forms_data)
        except KeyError:
            contact_info.append("No forms found")

        # Process notes
        try:
            note_ids = [note['note'].split('/')[-1] for note in data['notes']]
            notes_data = self._process_notes(note_ids)
            contact_info.append(notes_data)
        except KeyError:
            contact_info.append("No notes found")

        # Thread-safe file writing
        with self._lock:
            with open(filename, 'w') as file:
                file.write('\n'.join(filter(None, contact_info)))
            logging.info(f"Completed processing for: {name}")

    def _process_forms(self, form_ids: List[str]) -> str:
        """Process multiple forms concurrently"""
        forms_data = []
        with ThreadPoolExecutor(max_workers=min(len(form_ids), self.max_workers)) as executor:
            future_to_form = {
                executor.submit(self._process_single_form, form_id): form_id 
                for form_id in form_ids
            }
            for future in as_completed(future_to_form):
                if future.result():
                    forms_data.append(future.result())
        return '\n'.join(forms_data)

    def _process_single_form(self, form_id: str) -> str:
        """Process a single form"""
        data = self._make_request(f"{self.base_url}/v1/forms/{form_id}")
        if not data:
            return ""
            
        form_name = data.get("name", "Unknown Form Name")
        values = data.get("values", [])
        if 'Geo Coordinates' not in str(form_name):
            result = [f"\nForm Name: {form_name}", "\n"]
            for value in values:
                name = value.get("name")
                value_str = value.get("value")
                if value_str is not None and '+' not in str(value_str):
                    result.append(f" - {name}: {value_str}")
            
            return "\n".join(result)

    def _process_notes(self, note_ids: List[str]) -> str:
        """Process multiple notes concurrently"""
        notes_data = []
        with ThreadPoolExecutor(max_workers=min(len(note_ids), self.max_workers)) as executor:
            future_to_note = {
                executor.submit(self._process_single_note, note_id): note_id 
                for note_id in note_ids
            }
            for future in as_completed(future_to_note):
                if future.result():
                    notes_data.append(future.result())
        return '\n'.join(notes_data)

    def _process_single_note(self, note_id: str) -> str:
        """Process a single note"""
        data = self._make_request(f"{self.base_url}/v1/notes/{note_id}")
        if not data:
            return ""
            
        title = data.get('title', 'No title found')
        content = data.get('details', 'No content found')
        return f"\n{title}\n{content}"

    def process_all_contacts(self):
        """Main method to process all contacts using thread pool"""
        ids = self.extract_ids()
        logging.info(f"Found {len(ids)} contacts to process")
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(self._process_contact, id) for id in ids]
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logging.error(f"Error processing contact: {str(e)}")



def main():
    # Your API token
    token = ""
    
    # Initialize API client with 10 worker threads
    api = DayliteAPI(token, max_workers=10)
    
    # Process all contacts
    api.process_all_contacts()
    
    logging.info("Script execution completed successfully!")

if __name__ == "__main__":
    main()