import requests, logging
from bs4 import BeautifulSoup
from pathlib import Path
from datetime import datetime, timezone, timedelta
import pandas as pd
from concurrent.futures import ThreadPoolExecutor

'''
    Ranco Xu @2024-02-16 at my Brissy home with my beagle girl Coco

    This file contains all the core functions for Seeker crawler

    Updated @2024-07-31 at RPF Albion office after I found seek /api/chalice-search/v4/search endpoint

'''


# logger set up
logger_level=logging.ERROR
logger=logging.getLogger(__name__)
logger.setLevel(logger_level)
handler=logging.StreamHandler()
formatter=logging.Formatter(fmt='[%(asctime)s %(name)s]-%(levelname)s>> %(message)s',datefmt=r'%Y-%m-%d %H:%M:%S')
handler.setFormatter(formatter)
logger.addHandler(handler)


API_URL = r'https://www.seek.com.au/api/chalice-search/v4/search'
headers = {
    'accept': '*/*',
    'accept-language': 'en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7',
    'content-type': 'application/json',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
}

classifications = {'':'', 'Accounting': '1200', 'Administration & Office Support': '6251', 
                 'Advertising, Arts & Media': '6304', 'Banking & Financial Services': '1203', 
                 'Call Centre & Customer Service': '1204', 'CEO & General Management': '7019', 
                 'Community Services & Development': '6163', 'Construction': '1206', 
                 'Consulting & Strategy': '6076', 'Design & Architecture': '6263', 
                 'Education & Training': '6123', 'Engineering': '1209', 
                 'Farming, Animals & Conservation': '6205', 'Government & Defence': '1210', 
                 'Healthcare & Medical': '1211', 'Hospitality & Tourism': '1212', 
                 'Human Resources & Recruitment': '6317', 'Information & Communication Technology': '6281', 
                 'Insurance & Superannuation': '1214', 'Legal': '1216', 'Manufacturing, Transport & Logistics': '6092', 
                 'Marketing & Communications': '6008', 'Mining, Resources & Energy': '6058', 
                 'Real Estate & Property': '1220', 'Retail & Consumer Products': '6043', 'Sales': '6362', 
                 'Science & Technology': '1223', 'Self Employment': '6261', 'Sport & Recreation': '6246', 
                 'Trades & Services': '1225'}

BASE_DIR=Path(__file__).resolve().parent


# =================== Core Functions =====================
def update_classification_list():
    res = requests.get(r'https://www.seek.com.au/',headers=headers)
    soup = BeautifulSoup(res.text,'html.parser')

    # get classification drop down list
    dropDownListNav = soup.find('nav',attrs={"role": "navigation","data-automation": "searchClassification"})
    classifications = dropDownListNav.select('li[data-automation="item-depth-0"]')
    classifications = {item.text:item.select_one('a')['data-automation'] for item in classifications}
    return classifications


# def remove_trim_inputs(input_str:str):
#     return re.sub(' +',' ',input_str.lower().strip())


# def contract_type_extractor(contract_type):
#     # this regex matches Full time or Contract/Temp or Casual/Vacation
#     if contract_type:
#         match=re.search(r'This is a (\w+[ |/?]\w+) job',contract_type,re.IGNORECASE)
#         contract_type=match.group(1)
#     return contract_type

# def get_text_or_blank(elem_found):
#     return elem_found.text.replace(chr(8211),'-') if elem_found else None


def file_name_formatter(keyword,subclassification,location,with_timestamp=False):
    # clean inputs
    keyword,subclassification,location = [*map(lambda x:x.strip().lower(),[keyword,subclassification,location])]
    # init timestamp
    timestamp=''
    # construct fname without timestamp
    if not any([keyword,subclassification,location]):
        fname='any jobs'
    else:
        fname=''.join([*map(lambda x:x+' ' if x else '',[keyword,subclassification,location])]).strip()
    # append timestamp if turned on
    if with_timestamp:
        timestamp=datetime.now().strftime(r'_%Y%m%d%H%M%S')
    return fname + timestamp + '.xlsx'


def extract_info_from_json(jobs):
    logger.debug(f"Now extracting fields from all scraped job details...")
    outputs = []
    for job in jobs:
        this_job = {}
        
        # get job id, job_title, job_link, company, company_id etc
        this_job['job_title'] = job.get('title')
        this_job['job_id'] = job.get('id')
        this_job['isPremium'] = job.get('isPremium')
        this_job['isStandOut'] = job.get('isStandOut')
        this_job['company_id'] = job.get('advertiser').get('id')
        this_job['company'] = job.get('advertiser').get('description')
        this_job['area'] = job.get('areaWhereValue')
        this_job['areaId'] = job.get('areaId')
        this_job['classification_id'] = job.get('classification').get('id')
        this_job['classification'] = job.get('classification').get('description')
        this_job['locationId'] = job.get('locationId')
        this_job['location'] = job.get('location')
        this_job['time_posted'] = datetime.fromisoformat(job.get('listingDate')).astimezone(timezone(offset=timedelta(hours=10)))
        this_job['salary'] = job.get('salary')
        this_job['contract_type'] = job.get('workType')
        this_job['teaser'] = job.get('teaser')
        # 'bulletPoints': ['Hybrid working - Brisbane Office', 'Competitive salary that reflects your skills and experience.', 'Strong focus on the use and integration of AI']
        this_job['bullet_pts'] = ' - '.join([x for x in job.get('bulletPoints')])
        # [{'id':1,'label':{'text':'On-site'}},{'id':2,'label':{'text':'Hybrid'}}]
        this_job['workArrangements'] = ' & '.join([x.get('label').get('text') for x in job.get('workArrangements').get('data')])

        outputs.append(this_job)
        logger.debug(f"Exctracted <{this_job['job_title']} - {this_job['company']}>")
    logger.debug(f"Done extracting all {len(jobs)} jobs")
    return outputs


def seek_crawler(keyword,subclass,location,BASE_URL,headers,pageNum=1):
    # construct url
    params = {
        'siteKey': 'AU-Main',
        'where': location,
        'page': pageNum,
        'seekSelectAllPages': 'true',
        'hadPremiumListings': 'true',
        'keywords': keyword,
        'classification': classifications[subclass],
        'locale': 'en-AU',
    }

    # make request
    try:
        logger.debug(f"Now retriving {keyword} jobs page {pageNum} from seek...")
        response = requests.get(BASE_URL, params=params, headers=headers)
        response.raise_for_status()
    except Exception as e:
        logger.error(f"An error occurred when getting job details with msg:\n{e}")
    json_combo = response.json()
    jobs = json_combo.get('data')
    logger.debug(f"job details retrieved with {params=}")
    
    # format scraped data
    outputs = extract_info_from_json(jobs)
    return outputs


def create_df(outputs):
    df = pd.DataFrame(outputs)
    df['url']=df['job_id'].apply(lambda x: f"https://www.seek.com.au/job/{x}")
    return df


def write_df_to_xlsx(df,fullname):
    # convert time_posted column to timezone naive format
    df['time_posted']=df['time_posted'].dt.tz_localize(None)
    with pd.ExcelWriter(fullname, engine='openpyxl') as writer:
        # Write each DataFrame to a different sheet depending on unique value in contract type
        for contract_type in df['contract_type'].unique():
            tabname=contract_type.replace('/','_')
            df_tab=df.loc[df['contract_type']==contract_type,]
            df_tab.to_excel(writer,sheet_name=tabname,index=False,header=True)


def main(BASE_URL,headers,keyword,subclassification,location,pages_to_parse,expiry,SAVE_DIR):
    
    # get filename for future IO operations
    fname=file_name_formatter(keyword,subclassification,location)
    fullname = SAVE_DIR / fname

    # init results container
    outputs=[]

    # modify max page if not enough pages to parse
    # max_page = seek_maxPage(keyword,subclassification,location,search_pattern,BASE_URL,headers)
    # if max_page < pages_to_parse:
    #     logger.info(f"User defined number of pages to parse {pages_to_parse} is more than available pages {max_page}, pages to parse modified")
    #     pages_to_parse = max_page
    
    # run main function in threads
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(seek_crawler,keyword,subclassification,location,BASE_URL,headers,i+1) for i in range(pages_to_parse)]

    for future in futures:
        output=future.result()
        # outputs.append(output)
        outputs.extend(output)
    
    # create df from outputs
    df=create_df(outputs).sort_values(by='time_posted',ascending=False).drop_duplicates(subset='job_id')

    # csv operations
    # open csv with fname if exists, if not save df as csv directly
    try:
        df_exist=pd.read_excel(fullname,header=0,sheet_name=None)
        df_old=pd.DataFrame(columns=df.columns)
        # read data from the existing xlsx as df_old
        for sheet_name, df0 in df_exist.items():
            df_old=pd.concat([df_old, df0])
        
        # convert time posted column from xlsx file to python datetime object
        df_old['time_posted'] = pd.to_datetime(df_old['time_posted']).dt.to_pydatetime()

        # convert df time_posted(datetime tz column) to tz-naive column
        df['time_posted']=df['time_posted'].dt.tz_localize(None)

        # compare job id column and remove duplicates
        df=pd.concat([df,df_old],ignore_index=True).drop_duplicates(subset=['job_id',],keep='first',ignore_index=True)

        # remove row that is older than expiry days
        expiry_flags=df['time_posted'].apply(lambda timestamp:datetime.now()-timestamp <= timedelta(days=expiry))
        df=df[expiry_flags]

        # sort by time posted but put Featured on top
        df=df.sort_values(by='time_posted',ascending=False)

        # override existing csv
        write_df_to_xlsx(df,fullname)

    except FileNotFoundError:
        write_df_to_xlsx(df,fullname)
    return f'A total of {df.shape[0]} jobs have been scraped.'



if __name__ == '__main__':
    # parameter setups
    kwargs={
        'BASE_URL':API_URL,
        'headers': headers,

        # note keyword doesn't always go along with subclassification
        'keyword':'django developer',
        'subclassification':'',
        'location':'brisbane',
        'pages_to_parse':10,
        'expiry':21,
        'SAVE_DIR':BASE_DIR,
    }

    main(**kwargs)
