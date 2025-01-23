import requests
import json
from bs4 import BeautifulSoup
from typing import List, Dict, Optional
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
from threading import Lock

# Configure logging
logging.basicConfig(
    filename="scraping.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

BASE_URL = "https://www.idp.com/find-a-course/"
MAX_WORKERS = 5  # Number of parallel workers

# Thread-safe session creation
session_store = Queue()
json_lock = Lock()

def create_session():
    """Create a new session with proper headers."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive"
    })
    return session

# Initialize session pool
for _ in range(MAX_WORKERS):
    session_store.put(create_session())

def get_session():
    """Get a session from the pool."""
    return session_store.get()

def return_session(session):
    """Return a session to the pool."""
    session_store.put(session)

def make_request(url: str, session) -> requests.Response:
    """Make an HTTP request to the specified URL using the provided session."""
    response = session.get(url)
    response.raise_for_status()
    return response

def parse_course_info(course_soup: BeautifulSoup) -> Dict:
    """Extract basic course information from the course page."""
    for script_or_style in course_soup(["script", "style"]):
        script_or_style.decompose()
        
    course_small_info = {}
    divs = course_soup.find_all("div", class_="flex flex-col")
    for div in divs:
        key_tag = div.find("p", class_="block mb-[4px] c-lg:mb-[8px] font-semibold")
        value_tag = div.find("p", class_="text-heading-6")
        if key_tag and value_tag:
            key = key_tag.get_text(strip=True)
            value = value_tag.get_text(strip=True)
            course_small_info[key] = value
    return course_small_info

def get_entry_requirements(course_soup: BeautifulSoup) -> str:
    """Extract entry requirements from the course page."""
    how_to_apply_content = "N/A"
    accordion_divs = course_soup.find_all('div', class_="accordion")
    for accordion in accordion_divs:
        entry_requirements_header = accordion.find('h4')
        if entry_requirements_header and "Entry requirements" in entry_requirements_header.get_text():
            content_div = entry_requirements_header.find_next('div')
            if content_div:
                how_to_apply_content = content_div.get_text(separator="\n", strip=True)
                break
    return how_to_apply_content

def create_program_dict(course_name: str, about_course: str, course_small_info: Dict, how_to_apply_content: str) -> Dict:
    """Create a dictionary containing program details."""
    return {
        "course_title": course_name,
        "course_detail": about_course,
        "qualification": course_small_info.get('Qualification', 'N/A'),
        "duration": course_small_info.get('Duration', 'N/A'),
        "next_intake": course_small_info.get('Next intake', 'N/A'),
        "entry_score": course_small_info.get('Entry Score', 'N/A'),
        "course_fee": course_small_info.get('Fees', 'N/A'),
        "how_to_apply": how_to_apply_content
    }

def get_university_details(university_soup: BeautifulSoup) -> Dict:
    """Extract university information from the university page."""
    course_small_info_university = {}
    uni_divs = university_soup.find_all("div", class_="flex flex-col")
    for div in uni_divs:
        key_tag = div.find("p", class_="block mb-[4px] c-lg:mb-[8px] font-semibold")
        value_tag = div.find("p", class_="text-heading-6")
        if key_tag and value_tag:
            key = key_tag.get_text(strip=True)
            value = value_tag.get_text(strip=True)
            course_small_info_university[key] = value
    return course_small_info_university

def get_university_entry_requirements(university_soup: BeautifulSoup) -> List[str]:
    """Extract entry requirements from university page."""
    entry_req_list = []
    for accordion in university_soup.find_all('div', class_="accordion"):
        heading = accordion.find('span', class_="flex-1 text-left")
        if heading and 'Entry requirements' in heading.text:
            entry_req_content = accordion.get_text(separator='\n', strip=True)
            entry_req_list.append(entry_req_content)
            break
    if not entry_req_list:
        entry_req_list.append("N/A")
    return entry_req_list

def get_scholarships_info(university_soup: BeautifulSoup) -> str:
    """Extract scholarships information from university page."""
    for accordion in university_soup.find_all('div', class_="accordion"):
        heading = accordion.find('span', class_="flex-1 text-left")
        if heading and 'Scholarships & funding' in heading.text:
            return accordion.get_text(separator='\n', strip=True)
    return "N/A"

def process_scholarship_page(scholarship_link: str, session) -> Dict:
    """Process a single scholarship page and extract details."""
    response = make_request(scholarship_link, session)
    scholar_soup = BeautifulSoup(response.content, 'html.parser')
    
    scholar_title = scholar_soup.find("h1").get_text(strip=True) if scholar_soup.find("h1") else "N/A"
    scholar = scholar_soup.find("div", class_="accordion")
    
    formatted_output = {}
    if scholar:
        keys = [
            "Awarding institution:",
            "Qualification:",
            "Funding details:",
            "Funding type:",
            "Eligible intake:",
            "Study mode:",
            "Course/offer application deadline:"
        ]
        for key in keys:
            start = scholar.text.find(key)
            if start != -1:
                start += len(key)
                end = scholar.text.find(keys[keys.index(key) + 1]) if keys.index(key) + 1 < len(keys) else None
                formatted_output[key.strip()] = scholar.text[start:end].strip() if end else scholar.text[start:].strip()
    
    return {
        "title": scholar_title,
        "qualification": formatted_output.get("Qualification:", "N/A"),
        "funding_type": formatted_output.get("Funding type:", "N/A"),
        "funding_details": formatted_output.get("Funding details:", "N/A"),
        "deadline": formatted_output.get("Course/offer application deadline:", "N/A"),
        "eligible_intake": formatted_output.get("Eligible intake:", "N/A"),
        "study_mode": formatted_output.get("Study mode:", "N/A")
    }

def get_scholarship_links(course_soup: BeautifulSoup, session) -> List[str]:
    """Get links to scholarship pages with dynamic pagination."""
    scholarship_links_div = course_soup.find("div", class_="accordion")
    if not scholarship_links_div:
        return []
    
    base_links = [
        f"https://www.idp.com{a['href']}" 
        for a in scholarship_links_div.find_all('a', href=True) 
        if "/find-a-scholarship/" in a['href']
    ]
    
    if not base_links:
        return []
    
    all_scholarship_links = set()
    base_url = base_links[0]
    
    # Get total number of pages
    total_pages = get_total_scholarship_pages(base_url, session)
    logging.info(f"Found {total_pages} scholarship pages")
    
    # Iterate through all pages
    for page in range(1, total_pages + 1):
        try:
            paginated_url = f"{base_url}&page={page}"
            logging.info(f"Scraping scholarship page {page}/{total_pages}")
            
            response = make_request(paginated_url, session)
            scholar_soup = BeautifulSoup(response.content, 'html.parser')
            
            page_links = [
                f"https://www.idp.com{a['href']}" 
                for a in scholar_soup.find_all('a', href=True) 
                if "/scholarship/" in a['href']
            ]
            
            all_scholarship_links.update(page_links)
            
        except Exception as e:
            logging.error(f"Error processing scholarship page {page}: {e}")
            continue
    
    return list(all_scholarship_links)

def get_total_scholarship_pages(scholarship_url: str, session) -> int:
    """Get the total number of scholarship pages available."""
    try:
        response = make_request(scholarship_url, session)
        soup = BeautifulSoup(response.content, 'html.parser')
        
        pagination = soup.find_all('a', {'data-testid': 'paginationClick'})
        if not pagination:
            return 1
            
        pages = []
        for page in pagination:
            try:
                pages.append(int(page.get_text(strip=True)))
            except ValueError:
                continue
                
        return max(pages) if pages else 1
    except Exception as e:
        logging.error(f"Error getting total pages: {e}")
        return 1

def process_university(course_url: str, university_cache: Dict, session) -> Optional[Dict]:
    """Process a single university and its courses."""
    try:
        response = make_request(course_url, session)
        course_soup = BeautifulSoup(response.content, 'html.parser')

        course_name = course_soup.find("h1").get_text(strip=True) if course_soup.find("h1") else "N/A"
        university_name = course_soup.find("h2").get_text(strip=True) if course_soup.find("h2") else "N/A"
        university_name = university_name.replace("At", "") if "At" in university_name else university_name

        about_course_div = course_soup.find("div", class_="accordion")
        about_course = about_course_div.get_text(strip=True).replace("ScholarshipsView all scholarshipsInternships", "") if about_course_div else "N/A"

        course_small_info = parse_course_info(course_soup)
        how_to_apply_content = get_entry_requirements(course_soup)
        if "Entry Requirements for" in how_to_apply_content:
            how_to_apply_content = how_to_apply_content.replace(f"Entry Requirements for {university_name}", "")

        new_program = create_program_dict(course_name, about_course, course_small_info, how_to_apply_content)

        # Get scholarship data
        available_scholarships_list = []
        scholarship_links = get_scholarship_links(course_soup, session)
        
        # Process scholarships in parallel
        with ThreadPoolExecutor(max_workers=3) as executor:  # Using fewer workers for scholarships
            future_to_scholarship = {
                executor.submit(process_scholarship_page, link, session): link 
                for link in scholarship_links
            }
            
            for future in as_completed(future_to_scholarship):
                try:
                    scholarship_data = future.result()
                    available_scholarships_list.append(scholarship_data)
                except Exception as e:
                    logging.error(f"Error processing scholarship: {e}")

        with university_cache.get(university_name, {}) as cached_uni_data:
            if university_name in university_cache:
                logging.info(f"[CACHE HIT] Using cached data for {university_name}")
                
                # Update cached scholarships with new ones
                existing_scholarship_titles = {s["title"] for s in cached_uni_data["available_scholarships"]}
                new_scholarships = [s for s in available_scholarships_list if s["title"] not in existing_scholarship_titles]
                cached_uni_data["available_scholarships"].extend(new_scholarships)
                
                if "programs" not in cached_uni_data:
                    cached_uni_data["programs"] = []
                cached_uni_data["programs"].append(new_program)
                
                return {
                    "location": course_small_info.get('Location', 'N/A'),
                    "universities": {
                        "university_name": university_name,
                        "overview": cached_uni_data["overview"],
                        "world_rank": cached_uni_data["world_rank"],
                        "entry_requirements": cached_uni_data["entry_requirements"],
                        "scholarships_and_funding": cached_uni_data["scholarships_and_funding"],
                        "programs": cached_uni_data["programs"],
                        "available_scholarships": cached_uni_data["available_scholarships"]
                    }
                }

        h2_tag = course_soup.find('h2')
        university_link = h2_tag.find('a')['href'] if h2_tag and h2_tag.find('a') else None
        if not university_link:
            return None

        university_link = f"https://www.idp.com{university_link}/"
        university_response = make_request(university_link, session)
        university_soup = BeautifulSoup(university_response.content, 'html.parser')

        course_small_info_university = get_university_details(university_soup)
        overview_div = university_soup.find("div", class_="accordion")
        overview_text = overview_div.get_text(strip=True) if overview_div else "N/A"

        entry_req_list = get_university_entry_requirements(university_soup)
        scholarships_funding_content = get_scholarships_info(university_soup)

        university_cache[university_name] = {
            "overview": overview_text,
            "world_rank": course_small_info_university.get('THE World Ranking', 'N/A'),
            "entry_requirements": {"details": entry_req_list},
            "scholarships_and_funding": scholarships_funding_content,
            "programs": [new_program],
            "available_scholarships": available_scholarships_list
        }

        return {
            "location": course_small_info.get('Location', 'N/A'),
            "universities": {
                "university_name": university_name,
                "overview": overview_text,
                "world_rank": course_small_info_university.get('THE World Ranking', 'N/A'),
                "entry_requirements": {
                    "details": entry_req_list
                },
                "scholarships_and_funding": scholarships_funding_content,
                "programs": [new_program],
                "available_scholarships": available_scholarships_list
            }
        }

    except Exception as e:
        logging.error(f"Error processing university: {e}")
        return None

def process_university_parallel(args):
    """Process a single university with proper session handling."""
    course_url, university_cache = args
    session = get_session()
    try:
        university_data = process_university(course_url, university_cache, session)
        return university_data
    finally:
        return_session(session)

def save_university_data(university_data: Dict):
    """Thread-safe function to save university data to JSON file."""
    if not university_data:
        return

    country_name = university_data.get('location', 'Unknown Country')
    university_name = university_data['universities']['university_name']

    with json_lock:
        try:
            with open('universities_data.json', 'r', encoding='utf-8') as f:
                existing_data = json.load(f)
        except FileNotFoundError:
            existing_data = {}

        if country_name not in existing_data:
            existing_data[country_name] = {}

        if university_name not in existing_data[country_name]:
            existing_data[country_name][university_name] = {
                "Overview": university_data['universities'].get('overview', 'N/A'),
                "World Ranking": university_data['universities'].get('world_rank', 'N/A'),
                "Entry Requirements": university_data['universities'].get('entry_requirements', {}),
                "ScholarShips & Funding": university_data['universities'].get('scholarships_and_funding', 'N/A'),
                "Programs": university_data['universities'].get('programs', []),
                "available_scholarships": university_data['universities'].get('available_scholarships', [])
            }
        else:
            existing_university = existing_data[country_name][university_name]
            
            # Merge programs
            existing_programs = existing_university.get('Programs', [])
            new_programs = university_data['universities'].get('programs', [])
            merged_programs = existing_programs + [
                prog for prog in new_programs
                if prog not in existing_programs
            ]
            existing_university['Programs'] = merged_programs
            
            # Merge scholarships
            existing_scholarships = existing_university.get('available_scholarships', [])
            new_scholarships = university_data['universities'].get('available_scholarships', [])
            merged_scholarships = existing_scholarships + [
                schol for schol in new_scholarships
                if schol not in existing_scholarships
            ]
            existing_university['available_scholarships'] = merged_scholarships

        with open('universities_data.json', 'w', encoding='utf-8') as f:
            json.dump(existing_data, f, indent=4, ensure_ascii=False)

        logging.info(f"Updated data for {university_name} in {country_name}")

def scrape_universities():
    """Main function to scrape university data with parallel processing."""
    Base_page = 15201
    university_cache = {}
    
    while Base_page < 17101:
        session = get_session()
        try:
            paginated_url = f"{BASE_URL}?page={Base_page}"
            response = make_request(paginated_url, session)
            soup = BeautifulSoup(response.content, 'html.parser')
            logging.info(f"Opening Base URL Page number {Base_page}")

            course_links = list(dict.fromkeys([
                f"https://www.idp.com{a['href']}" 
                for a in soup.find_all("a", href=True) 
                if "/universities-and-colleges/" in a["href"]
            ]))

            # Process universities in parallel
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                # Create tasks for each course link
                future_to_url = {
                    executor.submit(process_university_parallel, (url, university_cache)): url 
                    for url in course_links
                }

                # Process completed tasks
                for future in as_completed(future_to_url):
                    url = future_to_url[future]
                    try:
                        university_data = future.result()
                        if university_data:
                            save_university_data(university_data)
                    except Exception as e:
                        logging.error(f"Error processing {url}: {e}")

            logging.info("Waiting for 15 seconds before next page")
            time.sleep(15)
            Base_page += 1
        finally:
            return_session(session)

    logging.info("Scraping complete. Check universities_data.json for results.")

if __name__ == "__main__":
    scrape_universities()
