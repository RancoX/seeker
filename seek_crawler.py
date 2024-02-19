import requests, re, logging
from bs4 import BeautifulSoup
from pathlib import Path
from urllib.parse import urljoin
from datetime import datetime, timedelta
from dataclasses import dataclass
import pandas as pd
from concurrent.futures import ThreadPoolExecutor

'''
    Ranco Xu @2024-02-16 at my Brissy home with my beagle girl Coco

    This file contains all the core functions for Seeker crawler

'''


# logger set up
logger_level=logging.ERROR
logger=logging.getLogger(__name__)
logger.setLevel(logger_level)
handler=logging.StreamHandler()
formatter=logging.Formatter(fmt='[%(asctime)s %(name)s]-%(levelname)s>> %(message)s',datefmt=r'%Y-%m-%d %H:%M:%S')
handler.setFormatter(formatter)
logger.addHandler(handler)


datetime_unit_mapping = {
        'm': 'minutes',
        'h': 'hours',
        'd': 'days',
        'w': 'weeks',
    }

BASE_DIR=Path(__file__).resolve().parent

# this is the object model for each job scraped
@dataclass
class Position:
    job_title:str
    job_id:str | int
    job_link:str
    company:str
    time_posted:datetime
    salary:str
    contract_type:str
    bullet_pts:list
    job_description:str
    
    def format_time_posted(self,timestamp:datetime|str):
        if isinstance(timestamp,str):
            return timestamp
        return timestamp.strftime(r'%Y-%m-%d %H:%M:%S')
    
    def __repr__(self):
        return f"Job title: {self.job_title.text}\nCompany: {self.company}\nTime posted: {self.format_time_posted(self.time_posted)}\nJob id: {self.job_id}\nSalary: {self.salary}\nContract type: {self.contract_type}\nJob link: {self.job_link}\nBullet points: {self.bullet_pts}\njob_description: {self.job_description}\n"


def remove_trim_inputs(input_str:str):
    return re.sub(' +',' ',input_str.lower().strip())

def url_constructor(keyword:str,subclassification:str,location:str,pageNum=1):
    if keyword:
        keyword='-'.join(remove_trim_inputs(keyword).split()) + '-'
    if subclassification:
        subclassification = '-in-' + '-'.join(remove_trim_inputs(subclassification).split()) 
    if location:
        location = '/in-' + '-'.join(remove_trim_inputs(location).split())
    return f"{keyword}jobs{subclassification}{location}?sortmode=ListedDate&page={pageNum}"

def get_page_n(url:str,page_num):
    return url + f'&page={page_num}'

def date_str_converter(date_elem):
    '''
        This function converts string format of date posted to a Python datetime object

        example: 1d ago -> datetime(2024,2,17,9,25,55)
    '''
    if date_elem:
        # get now time
        now_time=datetime.now()
        # get time value and unit
        match=re.search(r'(\d+)([m|h|d|w]) ago',date_elem.text)
        if match:
            time_value=int(match.group(1))
            time_unit=match.group(2)
            delta=timedelta(**{datetime_unit_mapping[time_unit]:time_value})
            timestamp=now_time-delta
            # timestamp_str=timestamp.strftime(r'%Y-%m-%d %H:%M:%S')
        return timestamp
    return 'Featured'

def contract_type_extractor(contract_type):
    # this regex matches Full time or Contract/Temp or Casual/Vacation
    if contract_type:
        match=re.search(r'This is a (\w+[ |/?]\w+) job',contract_type,re.IGNORECASE)
        contract_type=match.group(1)
    return contract_type

def get_text_or_blank(elem_found):
    return elem_found.text.replace(chr(8211),'-') if elem_found else None

def create_df(list_of_position_obj:Position):
    job_title,job_id,job_link,company,time_posted,salary,contract_type,bullet_pts,job_description = [[] for _ in range(9)]
    for pos in list_of_position_obj:
        job_title.append(pos.job_title.text)
        job_id.append(pos.job_id)
        job_link.append(pos.job_link)
        company.append(pos.company)
        time_posted.append(pos.time_posted)
        salary.append(pos.salary)
        contract_type.append(pos.contract_type)
        bullet_pts.append(' - '.join(pos.bullet_pts))
        job_description.append(pos.job_description)

    # construct dictionary
    df_dict={'job id':job_id,'job title':job_title,'company name':company,'time posted':time_posted,'salary':salary,'contract type':contract_type,
             'bullet points':bullet_pts,'job description':job_description,'job link':job_link}
    # convert to df
    df=pd.DataFrame.from_dict(df_dict)
    # convert time posted column to Timestamp object
    # df.loc[df['time posted']!='Featured','time posted']=pd.Timestamp(df.loc[df['time posted']!='Featured','time posted'])
    return df

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


def seek_crawler(keyword,subclass,location,search_pattern,BASE_URL,headers,pageNum=1):
    # construct url
    url=urljoin(BASE_URL,url_constructor(keyword,subclass,location,pageNum))

    # make request
    res=requests.get(url,headers=headers)
    res.raise_for_status()
    soup=BeautifulSoup(res.text,'html.parser')

    # start filtering
    outputs=[]
    elems=soup.select(search_pattern)
    for elem in elems:
        # for each job card
        ## get job title
        jobTitle=elem.find('a',{'data-automation':"jobTitle"})

        ## get job id and link
        job_id=int(jobTitle.get('id').rsplit('-',1)[-1])
        job_link=urljoin(BASE_URL,jobTitle.get('href'))

        ## get company name
        company=elem.find('a',{'data-automation':"jobCompany"})
        company=get_text_or_blank(company)

        ## get posted time
        time_posted=elem.find('span',{'data-automation':'jobListingDate'})
        ### convert 1m, 2h, 5d ago to python datetime object
        time_posted=date_str_converter(time_posted)

        ## get pay (if any)
        salary=elem.find('span',{'data-automation':'jobSalary'})
        salary=get_text_or_blank(salary)

        ## get contract type
        contract_type=elem.select_one('p[class="y735df0"]')
        contract_type=contract_type_extractor(get_text_or_blank(contract_type))

        ## get bullet points
        bps=elem.find('ul')
        if bps:
            bps=bps.find_all('li')
            bps=[x.text for x in bps]
        else:
            bps=[]

        ## get short description
        job_des=elem.find('span',{'data-automation':'jobShortDescription'})
        job_des=get_text_or_blank(job_des)

        job=Position(jobTitle,job_id,job_link,company,time_posted,salary,contract_type,bps,job_des)
        outputs.append(job)
        logger.info(job)
        # logger.info(f"{contract_type=}")
    logger.info(f"{'='*80}\nDone scraping {url}")
    return outputs

def write_df_to_xlsx(df,fullname):
    with pd.ExcelWriter(fullname, engine='openpyxl') as writer:
        # Write each DataFrame to a different sheet depending on unique value in contract type
        for contract_type in df['contract type'].unique():
            tabname=contract_type.replace('/','_')
            df_tab=df.loc[df['contract type']==contract_type,]
            df_tab.to_excel(writer,sheet_name=tabname,index=False,header=True)


def main(BASE_URL,headers,keyword,subclassification,location,search_pattern,pages_to_parse,expiry):
    
    # get filename for future IO operations
    fname=file_name_formatter(keyword,subclassification,location)
    fullname = BASE_DIR / fname

    # init results container
    outputs=[]
    # run main function in threads
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(seek_crawler,keyword,subclassification,location,search_pattern,BASE_URL,headers,i+1) for i in range(pages_to_parse)]

    for future in futures:
        output=future.result()
        # outputs.append(output)
        outputs.extend(output)
    
    # create df from outputs
    df=create_df(outputs)

    # csv operations
    # open csv with fname if exists, if not save df as csv directly
    try:
        df_exist=pd.read_excel(fullname,header=0,sheet_name=None)
        df_old=pd.DataFrame(columns=df.columns)
        # read data from the existing xlsx as df_old
        for sheet_name, df0 in df_exist.items():
            df_old=pd.concat([df_old, df0])
        
        # convert time posted column from xlsx file to python datetime object
        df_old.loc[df_old['time posted']!='Featured','time posted'] = pd.to_datetime(df_old.loc[df_old['time posted']!='Featured','time posted']).dt.to_pydatetime()

        # compare job id column and remove duplicates
        df=pd.concat([df,df_old],ignore_index=True).drop_duplicates(subset=['job id',],keep='first',ignore_index=True)

        # remove row that is older than expiry days
        expiry_flags=df['time posted'].apply(lambda timestamp:datetime.now()-timestamp<=timedelta(days=expiry) if timestamp!='Featured' else True)
        df=df[expiry_flags]

        # sort by time posted but put Featured on top
        df_featured=df[df['time posted']=='Featured']
        df=df.loc[df['time posted']!='Featured',].sort_values(by='time posted',ascending=False)
        df=pd.concat([df_featured, df],ignore_index=True)

        # override existing csv
        write_df_to_xlsx(df,fullname)

    except FileNotFoundError:
        write_df_to_xlsx(df,fullname)
    
    return f'A total of {df.shape[0]} jobs have been scraped.'



if __name__ == '__main__':
    # parameter setups
    kwargs={
        'BASE_URL':r'https://www.seek.com.au',
        'headers':{'user-agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
                'accept':'text/html; charset=utf-8'},
        'search_pattern':'article[data-card-type="JobCard"] > div > div > div[class="y735df0 _1akoxc50 _1akoxc56"]',

        # note keyword doesn't always go along with subclassification
        'keyword':'django developer',
        'subclassification':'',
        'location':'brisbane',
        'pages_to_parse':5,
        'expiry':21,
    }

    main(**kwargs)
