''' ALL IMPORTS '''
import os
import requests
import json
import pandas as pd
from datetime import datetime, timedelta
import threading
from google.cloud import bigquery

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "vfw-creds.json"
#creating bq client
client = bigquery.Client()

# Fetching data from HTTP Server based on Report Type, Region and Dates
def api_response(region, report, date_start, date_end):
    """
    Args:
        region: SG (Singapore) or UK (United Kingdom)
        report: Report type i.e. Sales or Purchase
        date_start: Start date for data collection
        date_end: End date for data collection
    """
    url = "http://13.250.72.114:90/Vinum_API_" + region + "/api/" + report + "/Get" + region + report + "Order?DateFrom=" + date_start + "&DateTo=" + date_end
    response = requests.get(url)
    data = json.loads(response.text)
    df = pd.DataFrame(data)

    return df

# Latest data date from BQ table
def max_data_date(region, report_type):
    """
    Args:
        report_type: Either 'Sales' or 'Purchase'
    """
    if 'Sales' in report_type:
        dml_statement = "SELECT MAX(posting_date) AS max_date FROM `vfw-bigquery.vfw_sandbox.sales_invoices` WHERE country = '" + region + "'"
    elif 'Purchase' in report_type:
        dml_statement = "SELECT MAX(po_date) AS max_date FROM `vfw-bigquery.vfw_sandbox.purchase_orders` WHERE country = '" + region + "'"

    query_job = client.query(dml_statement)  # API request
    data = query_job.result().to_dataframe() # Waits for the job to finish and convert it to DF

    return data["max_date"].iloc[0] # Read the first row of the 'max_date' column in the DF

#Function to add Date and Country to the DFs
def add_date_country(region, df):
    """
    Args:
        region: SG (Singapore) or UK (United Kingdom)
        df: DataFrame in consideration
    """
    df['country'] = region
    if 'PO_Date' in list(df.columns):
        df['PO_Date'] = pd.to_datetime(df['PO_Date'])
    elif 'Posting_Date' in list(df.columns):
        df['Posting_Date'] = pd.to_datetime(df['Posting_Date'])
        

    return df

#Function to delete data from BQ table
def del_bq_table_data(region, max_date, report_type):
    """
    Args:
        max_date: End date for data deletion
        region: SG (Singapore) or UK (United Kingdom)
        table: The table where records need to be deleted
    """
    if 'Sales' in report_type:
        dml_statement = "DELETE FROM `vfw-bigquery.vfw_sandbox.sales_invoices` WHERE country = '" + region + "' AND posting_date >= '" + max_date + "'"
    elif 'Purchase' in report_type:
        dml_statement = "DELETE FROM `vfw-bigquery.vfw_sandbox.purchase_orders` WHERE country = '" + region + "' AND po_date >= '" + max_date + "'"
    
    query_job = client.query(dml_statement)  # API request
    query_job.result()  # Waits for statement to finish

#Function to write data to BQ tables
def write_to_bq(df, table):
    """
    Args:
        list_df: List of DFs to be ingested
        table: Name of the table in the database
    """
    df = df.fillna(0)
    try:
        df['po_date'] = df['po_date'].astype(str)
    except:
        df['posting_date'] = df['posting_date'].astype(str)
    N = 6000
    list_df = [df[i:i+N] for i in range(0,len(df),N)]
    for data in list_df:
        table_id = 'vfw-bigquery.vfw_sandbox.' + table
        result = data.to_dict(orient="records")
        errors = client.insert_rows_json(table_id,result)  # Make an API request.
        if errors == []:
            print("New rows have been added")
        else:
            print("Encountered errors while inserting rows: {}".format(errors))

# Latest Data date for Sales and Purchase Reports
# sg_purchase_max_date = max_data_date('SG', 'Purchase').strftime('%Y-%m-%d')
sg_purchase_max_date = (datetime.today() - timedelta(180)).strftime('%Y-%m-%d')
sg_sales_max_date = max_data_date('SG', 'Sales').strftime('%Y-%m-%d')
# uk_purchase_max_date = max_data_date('UK', 'Purchase').strftime('%Y-%m-%d')
uk_purchase_max_date = (datetime.today() - timedelta(180)).strftime('%Y-%m-%d')
uk_sales_max_date = max_data_date('UK', 'Sales').strftime('%Y-%m-%d')
curr_date = datetime.today().strftime('%Y-%m-%d')

# API Responses as DFs
sg_po = api_response('SG', 'Purchase', sg_purchase_max_date, curr_date)
sg_si = api_response('SG', 'Sales', sg_sales_max_date, curr_date)
uk_po = api_response('UK', 'Purchase', uk_purchase_max_date, curr_date)
uk_si = api_response('UK', 'Sales', uk_sales_max_date, curr_date)

# Add 'country' column and correct date's datatype
sg_po = add_date_country('SG', sg_po)
sg_si = add_date_country('SG', sg_si)
uk_po = add_date_country('UK', uk_po)
uk_si = add_date_country('UK', uk_si)

# Deleting data from BQ table
del_bq_table_data('SG', sg_purchase_max_date, 'Purchase')
del_bq_table_data('SG', sg_sales_max_date, 'Sales')
del_bq_table_data('UK', uk_purchase_max_date, 'Purchase')
del_bq_table_data('UK', uk_sales_max_date, 'Sales')

# Rearrange/Rename columns in the DF
si_columns = ['type', 'document_no', 'posting_date', 'slp_name', 'status', 'code', 'customer_name', 'warehouse_code',
            'item_no', 'item_description', 'currency', 'currency_rate', 'quantity', 'local_unit_price', 'local_net_sales',
            'local_item_cost', 'local_total_cost', 'country']

po_columns = ['po_date', 'po_number', 'po_line_number', 'apr_number', 'credit_term', 'payment_status', 'vendor_code',
            'vendor_name', 'item_no', 'item_description', 'uom', 'qty', 'balance_qty', 'currency', 'rate', 'foreign_unit_price',
            'foreign_total_amount', 'local_unit_price', 'local_total_amount', 'tax_rate', 'po_warehouse', 'purchaser', 'remarks',
            'reservation_type', 'so_details1', 'so_details2', 'so_details3', 'batch_details', 'country']

# Rearranging SG_PO columns to match final table for ingestion
sg_po = sg_po.drop(['CreateDate', 'UpdateDate'], axis = 1)
sg_po.columns = sg_po.columns.str.lower()
apr_col = sg_po.pop("apr_number")
sg_po.insert(3, apr_col.name, apr_col)
sg_po.insert(loc = 25, column = 'so_details2', value = '')
sg_po.insert(loc = 26, column = 'so_details3', value = '')

sg_si = sg_si.drop(['Base_Document_Reference', 'Linenum', 'Discount_Percent_for_Document', 'Price_after_Discount',
                'Discount_Percent_per_Row', 'Tax_Code', 'Rate_Percent', 'Ref_No'], axis = 1)

uk_po.columns = uk_po.columns.str.lower()
apr_col = uk_po.pop("apr_number")
uk_po.insert(3, apr_col.name, apr_col)

uk_si = uk_si.drop(['Base_Document_Reference', 'Linenum', 'Discount_Percent_for_Document', 'Price_after_Discount',
                'Discount_Percent_per_Row', 'Tax_Code', 'Rate_Percent', 'Ref_No'], axis = 1)

sg_po.columns = po_columns
sg_si.columns = si_columns
uk_po.columns = po_columns
uk_si.columns = si_columns

po_data_list = [sg_po,uk_po]
si_data_list = [sg_si,uk_si]

po_data = pd.concat(po_data_list)
po_data.dropna(subset = ['po_date'], inplace = True)

si_data = pd.concat(si_data_list)
si_data.dropna(subset = ['posting_date'], inplace=True)

# creating threads
t1 = threading.Thread(target=write_to_bq, args=(po_data,'purchase_orders'))
t2 = threading.Thread(target=write_to_bq, args=(si_data,'sales_invoices'))

# starting threads
t1.start()
t2.start()
# wait until all threads finish
t1.join()
t2.join()
