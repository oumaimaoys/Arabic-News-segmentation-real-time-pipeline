import json
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time

class Crawler:
    def __init__(self, url, topic):
        # Set up the WebDriver (using Chrome and providing a path if necessary)
        self.driver = webdriver.Chrome()  # Specify your path
        self.url = url  # Set the target URL
        self.topic = topic
        
    def collect_data(self):
        self.driver.get(self.url)  # Navigate to the target page
        articles = []  # List to store articles
        articles_needed = 500
        
        while len(articles) < articles_needed:
            # Parse the current page source with BeautifulSoup
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            
            # Find and extract the articles
            new_articles = soup.find_all('article', class_='jeg_post')  # Adjust the selector
            
            for article in new_articles:
                title = article.find('h3').text if article.find('h3') else "No title"
                
                # Append article details to the articles list
                articles.append({
                    'title': title,
                    'topic' : self.topic
                })
                
            print(f"Found {len(articles)} articles so far...")
            
            # If 500 articles are collected, break the loop
            if len(articles) >= articles_needed:
                break
            
            # Click the "Load More" button
            try:
                load_more_button = WebDriverWait(self.driver, 60).until(
                    EC.element_to_be_clickable((By.XPATH, "//a[contains(text(), 'Load More')]"))
                )
                load_more_button.click()
                time.sleep(10)  # Allow time for the content to load
            except TimeoutException:
                print("Timed out waiting for 'Load More' button to appear.")
                break
            except NoSuchElementException:
                print("Could not find the 'Load More' button or no more articles to load.")
                break
            except Exception as e:
                print(f"An error occurred: {e}")
                break
        
        # Ensure you have exactly 500 articles
        articles = articles[:500]
        print(f"Collected {len(articles)} articles.")

        # Save articles to a JSON file
        with open('data/training.json', 'w', encoding='utf-8') as f:
            json.dump(articles, f, ensure_ascii=False, indent=4)

        self.driver.quit()  # Close the browser session

# Testing the crawler
#crawler = Crawler("https://elmassae24.ma/poli/", "politique")
#crawler.collect_data()

#crawler = Crawler("https://elmassae24.ma/diver/", "divers")
#crawler.collect_data()

crawler = Crawler("https://elmassae24.ma/%d8%a7%d9%82%d8%aa%d8%b5%d8%a7%d8%af/", "economie")
crawler.collect_data()

#crawler = Crawler("https://elmassae24.ma/%d8%b1%d9%8a%d8%a7%d8%b6%d8%a9/", "sport")
#crawler.collect_data()



