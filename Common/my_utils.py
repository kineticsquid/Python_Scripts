"""
Created on June 15, 2017

@author: kellrman

Various utility functions
"""

"""
Method to authenticate to Bluemix to make API calls. Returns a bearer token in a dictionary for use as HTTP headers. 
Requires IBM_ID and IBM_ID_PW environment variables

curl commands for testing access:
curl -X POST https://login.ng.bluemix.net/UAALoginServerWAR/oauth/token -H "Authorization:Basic Y2Y6" -d "grant_type=password&username=rboykin@us.ibm.com&password=redacted"

curl https://api.ng.bluemix.net/v2/organizations -H "Authorization:bearer <access_token from above response>"
"""

import requests

def bluemix_auth(bluemix_api_endpoint, **kwargs):

    if 'userid' in kwargs:
        ibm_id = kwargs['userid']
    else:
        ibm_id = os.getenv('IBM_ID')
        if ibm_id is None:
            raise Exception('IBM_ID environment variable not defined.')
    if 'password' in kwargs:
        ibm_id_pw = kwargs['password']
    else:
        ibm_id_pw = os.getenv('IBM_ID_PW')
        if ibm_id_pw is None:
            raise Exception('IBM_ID_PW environment variable not defined.')
    info_endpoint = '/info'
    oauth_endpoint = '/oauth/token'

    response = requests.get(bluemix_api_endpoint + info_endpoint)

    if response.status_code == 200:
        results = response.json()
        auth_endpoint = results['authorization_endpoint'] + oauth_endpoint
        http_headers = {
            'Authorization': 'Basic Y2Y6'
        }
        http_payload = {
            'grant_type': 'password',
            'username': ibm_id,
            'password': ibm_id_pw
        }
        response = requests.post(auth_endpoint, data=http_payload, headers=http_headers)

        if response.status_code == 200:
            results = response.json()
            authorization = results['token_type'] + ' ' + results['access_token']
            http_headers = {
                # 'accept': '*/*',
                # 'content-type': 'application/json;charset=utf-8',
                # 'content-type': 'application/json',
                'Authorization': authorization
            }
            return http_headers
        else:
            raise Exception('Error getting bearer token: %s %s' % (response.status_code, response.content))
    else:
        raise Exception('Error getting bearer token: %s %s' % (response.status_code, response.content))

"""
Method to handle paging in an API call to Bluemix
"""

def get_all_bluemix_results(url, http_headers):
    all_results = []
    while url is not None:
        response = requests.get(url, headers=http_headers)
        if response.status_code == 200:
            http_results = response.json()
            results = http_results.get('resources', None)
            if results is not None:
                # results key is returned if the response is a list
                all_results += http_results['resources']
                next_url = http_results['next_url']
                if next_url is not None:
                    index = url.find(next_url[0:3])
                    url = url[0:index] + next_url
                else:
                    url = None
            else:
                # if there is no results key, just a single result, so return it.
                all_results = http_results
                url = None
        else:
            raise Exception('Error getting results from %s: %s %s' %
                            (url, response.status_code, response.content))
    return all_results



"""
Method to take as input a selenium driver and authenticate to WCP using IBM id. Requires environment variables for
IBM_ID and IBM_ID_PW to be defined. Returns when the main WCP page is rendered completely. Handles both external 
IBMid and w3ids.
"""

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import os

def authenticate_to_wcp(selenium_driver):
    IBM_ID = os.getenv('IBM_ID')
    IBM_ID_PW = os.getenv('IBM_ID_PW')
    if IBM_ID is None or IBM_ID_PW is None:
        raise Exception('IBM_ID and/or IBM_ID_PW environment variables not defined.')
    selenium_driver.implicitly_wait(10)
    url = 'https://company-profiler.watson.ibm.com/'
    selenium_driver.get(url)
    assert "Watson Company Profiler" in selenium_driver.title
    selenium_driver.find_element_by_link_text('Log in').click()
    e = WebDriverWait(selenium_driver, 10).until(EC.element_to_be_clickable((By.NAME, "username")))
    # e = selenium_driver.find_element_by_name('username')
    e.clear()
    e.send_keys(IBM_ID)
    e = WebDriverWait(selenium_driver, 10).until(EC.element_to_be_clickable((By.ID, "continuebutton")))
    e.click()
    try:
        WebDriverWait(selenium_driver, 5).until(EC.element_to_be_clickable((By.NAME, "password")))
    except:
        WebDriverWait(selenium_driver, 10).until(EC.element_to_be_clickable((By.ID, "continuefedbutton")))
    page_source = selenium_driver.page_source
    # Now need to check if we sign on using a w3id or a non w3 IBMid. If the ID the user entered is displayed,
    # then this is not a w3ID. Otherwise the federated id prompt shows up. And we have one click to get to the
    # ID and PW prompt.
    if IBM_ID in page_source:
        selenium_driver.find_element_by_name('password').send_keys(IBM_ID_PW)
        selenium_driver.find_element_by_id('signinbutton').click()
    else:
        e = WebDriverWait(selenium_driver, 10).until(EC.element_to_be_clickable((By.ID, "continuefedbutton")))
        e.click()
        e = WebDriverWait(selenium_driver, 30).until(EC.element_to_be_clickable((By.ID, "desktop")))
        e.clear()
        e.send_keys(IBM_ID)
        selenium_driver.find_element_by_name('password').send_keys(IBM_ID_PW)
        selenium_driver.find_element_by_class_name('btn_signin').click()

    e = WebDriverWait(selenium_driver, 30).until(
        EC.visibility_of_element_located((By.CLASS_NAME, "Explore__container")))

"""
Method to take a soup object and recursively walk and print out the structure
"""
def walk(indent, soup_object):
    try:
        # can't reliably predict when the tag element will not have a contents and this BeautifulSoup Tag class does not
        # support a .get('key', None)
        print(indent + '$' + str(type(soup_object)) + '$  ' + str(soup_object.text))
        contents = soup_object.contents
    except (AttributeError):
        contents = None
    else:
        for o in contents:
            walk(indent + '  ', o)

"""
Method that uses concurrent threads to batch process a large number of input records in an input file
"""

from concurrent.futures import ThreadPoolExecutor, wait

def batch_and_process(input, output, operation, batch_size, max_threads, logger):

    pool = ThreadPoolExecutor(max_threads)

    total_started = 0
    futures = []
    batch = []
    for line in input:
        batch.append(line.strip())
        if len(batch) == batch_size:
            future = pool.submit(operation, batch, output)
            futures.append(future)
            total_started += len(batch)
            logger.info('Batched %s' % total_started)
            batch = []

    # catch the last bit that didn't make up a full batch
    if len(batch) > 0:
        future = pool.submit(operation, batch, output)
        futures.append(future)
        total_started += len(batch)
        logger.info('Batched %s' % total_started)

    logger.info('waiting to finish')
    wait(futures)
    logger.info('finished')

"""
Method to define and return a logger for logging
"""
import logging
import sys

def get_my_logger():
    logger = logging.getLogger('My Logger')
    logger.setLevel(logging.INFO)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(threadName)s - %(message)s', "%H:%M:%S")
    ch.setFormatter(formatter)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    return logger

"""
Returns the top level domain from a url
"""

def get_top_level_domain(url):
    top_level_domain = None
    # first get rid of http:// if it's in the URL
    double_slash = url.find('//')
    if double_slash >= 0:
        url_string = url[double_slash+2:len(url)]
    else:
        url_string = url
    # next get rid of any trailing / qualifiers, keeping only the top level domain name
    slash_index = url_string.find('/')
    if slash_index >= 0:
        url_string = url_string[0:slash_index]
    # now, let's get only the domain.com part of this string
    period_index = url_string.rfind('.')
    if period_index >= 0:
        next_period_index = url_string.rfind('.', 0, period_index)
        if next_period_index >= 0:
            top_level_domain = url_string[next_period_index+1:len(url_string)]
        else:
            top_level_domain = url_string

    return top_level_domain
