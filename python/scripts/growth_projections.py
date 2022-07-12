'''ALL IMPORTS'''
from googleads import adwords
import pandas as pd
import numpy as np
import io
import csv 
from datetime import datetime

adwords_client = adwords.AdWordsClient.LoadFromStorage()
date = datetime.now().strftime("%Y%m%d")
PAGE_SIZE = 500

'''Fetches a list of Adwords Account of all the cities'''
def accounts_list(client):
  # Initialize appropriate service.
  managed_customer_service = client.GetService(
      'ManagedCustomerService', version='v201809')

  # Construct selector to get all accounts.
  offset = 0
  selector = {
      'fields': ['CustomerId', 'Name'],
      'paging': {
          'startIndex': str(offset),
          'numberResults': str(PAGE_SIZE)
      }
  }
  more_pages = True
  accounts = {}
  child_links = {}
  parent_links = {}
  accounts_list = []
  while more_pages:
    # Get serviced account graph.
    page = managed_customer_service.get(selector)
    if 'entries' in page and page['entries']:
      # Create map from customerId to parent and child links.
      if 'links' in page:
        for link in page['links']:
          if link['managerCustomerId'] not in child_links:
            child_links[link['managerCustomerId']] = []
          child_links[link['managerCustomerId']].append(link)
          if link['clientCustomerId'] not in parent_links:
            parent_links[link['clientCustomerId']] = []
          parent_links[link['clientCustomerId']].append(link)
      # Map from customerID to account.
      for account in page['entries']:
        accounts[account['customerId']] = account
        accounts_list.append(account['customerId'])
    offset += PAGE_SIZE
    selector['paging']['startIndex'] = str(offset)
    more_pages = offset < int(page['totalNumEntries'])
  return(accounts_list)



'''Estimate Request gives a max and min value of metric. This function returns the mean value of the metric'''
def _CalculateMean(min_est, max_est):
  if min_est and max_est:
    return round((float(min_est) + float(max_est)) / 2.0,2)
  else:
    return None


'''Writes the csv with Keyword,Clicks and Impressions'''
def write_csv(list):
  with open("Keyword_Stats_" + date + '.csv', "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["Keyword", "ClicksPerDay", "ImpressionsPerDay"])
    writer.writerows(list)


'''Fetches Keywords from Adwords through AWQL and returns the list as a DF'''
def fetch_keywords(account_ids,client):
    report_downloader = client.GetReportDownloader(version='v201809')
    # Create report query.
    report_query = ('''
    SELECT Criteria
    FROM KEYWORDS_PERFORMANCE_REPORT
    WHERE KeywordMatchType = 'EXACT' AND AllConversions>5 AND Impressions>1000
    DURING LAST_MONTH''')

    # Define output as a string
    output = io.StringIO()
    # df1 = pd.DataFrame()
    # Write query result to output file
    for id in account_ids:
        if id!= 6162648732:
            report_downloader.DownloadReportWithAwql(
                report_query, 
                'CSV',
                output,
                client_customer_id=id, # denotes which adw account to pull from
                skip_report_header=True, 
                skip_column_header=False,
                skip_report_summary=True, 
                include_zero_impressions=False)
            output.seek(0)
            df = pd.read_csv(output)  
    
    return(df)


def main(client):
  # Initialize appropriate service.
  traffic_estimator_service = client.GetService(
      'TrafficEstimatorService', version='v201809')

  account_ids = accounts_list(client)

  keywords = fetch_keywords(account_ids,client)
  keywords_matchType = []

  for keyword in list(pd.unique(keywords['Keyword'])):
    a = {'text': keyword,'matchType':'EXACT'}
    keywords_matchType.append(a)

  keyword_estimate_requests = []
  
  for keyword in keywords_matchType:
    keyword_estimate_requests.append({
        'keyword': {
            'xsi_type': 'Keyword',
            'matchType': keyword['matchType'],
            'text': keyword['text']
        }
    })
  # Create ad group estimate requests.
  adgroup_estimate_requests = [{
      'keywordEstimateRequests': keyword_estimate_requests,
      'maxCpc': {
          'xsi_type': 'Money',
          'microAmount': '1000000'
      }
  }]
  # Create campaign estimate requests.
  campaign_estimate_requests = [{
      # 'campaignId' : 1411261158,
      'adGroupEstimateRequests': adgroup_estimate_requests
  }]
  # Create the selector.
  selector = {
      'campaignEstimateRequests': campaign_estimate_requests,
  }

  # Get traffic estimates.
  estimates = traffic_estimator_service.get(selector)
  campaign_estimate = estimates['campaignEstimates'][0]


  if 'adGroupEstimates' in campaign_estimate:
    ad_group_estimate = campaign_estimate['adGroupEstimates'][0]
    if 'keywordEstimates' in ad_group_estimate:
      keyword_estimates = ad_group_estimate['keywordEstimates']

      keyword_estimates_and_requests = zip(keyword_estimates,
                                           keyword_estimate_requests)

      final_data = []
      for keyword_tuple in keyword_estimates_and_requests:
        if keyword_tuple[1].get('isNegative', False):
          continue
        keyword = keyword_tuple[1]['keyword']
        #keyword['text'] gives keyword name
        keyword_estimate = keyword_tuple[0]
        mean_clicks_per_day = _CalculateMean(keyword_estimate['min']['clicksPerDay'],keyword_estimate['max']['clicksPerDay'])
        mean_impressions_per_day = _CalculateMean(keyword_estimate['min']['impressionsPerDay'],keyword_estimate['max']['impressionsPerDay'])
        keyword_data = [keyword['text'],mean_clicks_per_day,mean_impressions_per_day]
        final_data.append(keyword_data)
      write_csv(final_data)



if __name__ == '__main__':
  # Initialize client object.
  main(adwords_client)