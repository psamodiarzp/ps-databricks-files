# Databricks notebook source
import pandas as pd
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
import smtplib
import os
from datetime import datetime, timedelta, date
import time
import sys

# COMMAND ----------

tracker_details_db = sqlContext.sql("""
WITH payments as(
  SELECT
created_date as payment_date,
 CONCAT(
    YEAR(created_date),
    '-',
    LPAD(MONTH(created_date), 2, '0')
  ) AS payment_month_year,
order_id,
merchant_id,
   ROUND(CAST(COALESCE(SUM(( base_amount  *1.0/100) ), 0)AS DECIMAL(10, 2)), 2)  as amount
   FROM
  realtime_hudi_api.payments 
  where created_date >= '2023-04-01'
  and payments.method = 'cod'
  GROUP BY 1,2,3,4
), fulfillment as (

SELECT
created_date,
 lower(status) AS shipping_status,
  get_json_object(shipping_provider, '$.awb_number') AS awb_number,
  get_json_object(shipping_provider, '$.rto_charges') AS rto_charges,
  get_json_object(shipping_provider, '$.shipping_status') AS status,
  get_json_object(shipping_provider, '$.shipping_charges') AS shipping_charges,
  get_json_object(source, '$.origin')  as shipping_provider_name,
merchant_order_id,
order_id
FROM
realtime_prod_shipping_service.fulfillment_orders 

), model_details as(
SELECT 
order_id,
risk_tier,
cod_eligible,
cod_intelligence_enabled,
manual_control_cod_order
--rto_charges
from aggregate_pa.magic_rto_reimbursement_fact
where isemailwhitelisted=0
and isphonewhitelisted=0

), merchants AS (
    select id, name from realtime_hudi_api.merchants
)
select * from payments
left join fulfillment on payments.order_id = fulfillment.order_id
INNER JOIN batch_sheets.magic_checkout_rto_insurance as mid_insurance on payments.merchant_id = mid_insurance.mid
LEFT JOIN model_details on payments.order_id = model_details.order_id 
LEFT JOIN merchants on payments.merchant_id = merchants.id
where   payments.payment_date >= rto_insurance_start_date
  and fulfillment.created_date >= rto_insurance_start_date
--limit 1000
""")
tracker_details_df = tracker_details_db.toPandas()
tracker_details_df.head()

# COMMAND ----------

if tracker_details_df.empty:
  print("The DataFrame is empty. Exiting the code.")
  sys.exit()
else:
    print("The DataFrame is not empty. Proceeding with the code.")

# COMMAND ----------

# Get the unique column names
unique_columns = ~tracker_details_df.columns.duplicated()

# Select only the unique columns
tracker_details_df = tracker_details_df.loc[:, unique_columns]

#Column Type modifications
tracker_details_df.loc[tracker_details_df['rto_charges'] =='0', 'rto_charges'] = 0.0
tracker_details_df['rto_charges'] = tracker_details_df['rto_charges'].astype('float')
tracker_details_df = tracker_details_df[(tracker_details_df['merchant_order_id']).str.lower()!='order pending']
tracker_details_df.head()

# COMMAND ----------

tracker_details_df[(tracker_details_df['merchant_id']=='INmSeldc63QJ9j')  & (tracker_details_df['payment_month_year']=='2024-03')
                   # & (tracker_details_df['shipping_status']=='rto')
                   ]

# COMMAND ----------

tracker_details_df[(tracker_details_df['merchant_id']=='INmSeldc63QJ9j')
                   ]

# COMMAND ----------

tracker_details_df['calculated_reimbursement'] = tracker_details_df.apply(
    lambda row: row['max_reimbursement'] if row['reimbursement_bucket'] == 'flat'
    else row['max_reimbursement'] if (row['rto_charges']) < float(row['max_reimbursement']) and (row['rto_charges']) == 0
    else row['rto_charges'] if row['rto_charges'] < float(row['max_reimbursement'])
    else row['max_reimbursement'],
    axis=1
)

tracker_details_df['calculated_reimbursement'] = tracker_details_df.apply(
    lambda row: 0 if ((row['manual_control_cod_order'] == 'true' and row['risk_tier'] != 'low') or (row['cod_eligible'] != 'true' and  row['cod_intelligence_enabled'] == 'true'))
    else row['calculated_reimbursement'],
    axis=1
)

tracker_details_df.head()


# COMMAND ----------

valid_delivery_status = ['delivered','rto','cancelled','lost','partially_delivered','returned']

tracker_summary_new_df = tracker_details_df.groupby(['payment_month_year', 'merchant_id']).agg(
    total_order_count=('order_id', 'count'),
    order_count_w_delivery=('shipping_status', lambda x: x[x.isin(valid_delivery_status)].count()),
    order_count_w_rto=('shipping_status', lambda x: x[x.isin(['rto'])].count()),
    email=('email', 'max'),
    email_cc=('email_cc', 'max'),
    name=('name', 'max'),
).reset_index()
tracker_summary_new_df = tracker_summary_new_df.fillna('')
tracker_summary_new_df.head()



# COMMAND ----------

tracker_summary_past_df = pd.read_csv('/dbfs/FileStore/magic_rto_reimbursement_tracker_summary.csv')
#tracker_summary_past_df = pd.DataFrame(columns = ['payment_month_year','merchant_id','rto_invoiced','reminder_sent','rto_amount','status'])
#tracker_summary_past_df.rename(columns={'status': 'invoice_status'}, inplace=True)
tracker_summary_past_df = tracker_summary_past_df.fillna('')
tracker_summary_past_df[['payment_month_year','merchant_id','rto_invoiced','reminder_sent','invoice_status','status']]

# COMMAND ----------

tracker_summary_past_df[tracker_summary_past_df['payment_month_year']=='2024-10']

# COMMAND ----------

tracker_summary_df =  pd.merge(tracker_summary_new_df, tracker_summary_past_df[['payment_month_year','merchant_id','rto_invoiced','reminder_sent','rto_amount','invoice_status','status']], on = ['payment_month_year','merchant_id'], how='left')
tracker_summary_df = tracker_summary_df.fillna('')
tracker_summary_df[tracker_summary_df['merchant_id'] == 'INmSeldc63QJ9j']

# COMMAND ----------

tracker_summary_df[tracker_summary_df['payment_month_year']=='2023-08']

# COMMAND ----------

"""
Merchants sent to before
HakfWT9UoxpRxO - sent the email for July
GX2Z0I7L07K19g - sent the email from April till July
FGmEEx9rbF5e8F - sent the email for July
JkuJOHbtFLHR1p - sent the email from April till July
"""
mid_list = ['HakfWT9UoxpRxO','FGmEEx9rbF5e8F','GX2Z0I7L07K19g',
            'GX2Z0I7L07K19g','GX2Z0I7L07K19g','GX2Z0I7L07K19g',
            'JkuJOHbtFLHR1p','JkuJOHbtFLHR1p','JkuJOHbtFLHR1p','JkuJOHbtFLHR1p',]
month_list = ['2023-07','2023-07','2023-04',
              '2023-05','2023-06','2023-07',
              '2023-04','2023-05','2023-06','2023-07',]

for mid, month in zip(mid_list, month_list):
    tracker_summary_df.loc[(tracker_summary_df['merchant_id'] == mid) & (tracker_summary_df['payment_month_year'] == month),'invoice_status'] = 'Success'

# COMMAND ----------

#Blacklisting certain merchants from the report
# blacklisted_mx = ['IH7E2OJQGEKKTN','FR2kZCaWIQTnDn','J5i4wFF86Uo1up',]
# Removing Gunia from the blacklisted list
blacklisted_mx = ['IH7E2OJQGEKKTN','J5i4wFF86Uo1up',]
tracker_summary_df = tracker_summary_df[~tracker_summary_df['merchant_id'].isin(blacklisted_mx) ]
tracker_summary_df[tracker_summary_df['payment_month_year']=='2023-08']

# COMMAND ----------

tracker_summary_df[tracker_summary_df['merchant_id']=='JkuJOHbtFLHR1p']

# COMMAND ----------

# Get the current date
current_date = datetime.now()

# Calculate the target date
if current_date.day < 14:
    # Subtract 2 months from the current date
    year = current_date.year
    month = current_date.month - 2
    if month <= 0:
        month += 12
        year -= 1
else:
    # Subtract 1 month from the current date
    year = current_date.year
    month = current_date.month - 1
    if month <= 0:
        month += 12
        year -= 1

# Format the result date as 'YYYY-MM'
end_date = f'{year:04d}-{month:02d}'

print(end_date)

# COMMAND ----------

def calculate_rto_amount(month_year, mid):
    return tracker_details_df[
        (tracker_details_df['payment_month_year'] == month_year) &
         (tracker_details_df['merchant_id'] == mid) &
         (tracker_details_df['shipping_status']=='rto')
                              ]['calculated_reimbursement'].astype('float').sum()

# COMMAND ----------

def generate_order_level_report(month_year, mid):
    order_level_report = tracker_details_df[
        (tracker_details_df['payment_month_year'] == month_year) &
         (tracker_details_df['merchant_id'] == mid) & 
         (tracker_details_df['shipping_status']=='rto') &
         (tracker_details_df['calculated_reimbursement'] !=0)
                              ][['merchant_id','order_id','merchant_order_id','shipping_provider_name','payment_date','calculated_reimbursement','payment_month_year']].reset_index(drop=True)
    return order_level_report


# COMMAND ----------

def generate_prending_status_report(month_year, mid):
    return tracker_details_df[
        (tracker_details_df['payment_month_year'] == month_year) &
         (tracker_details_df['merchant_id'] == mid) & 
         (~tracker_details_df['shipping_status'].isin(valid_delivery_status))
                              ][['merchant_order_id','status','shipping_charges','awb_number','shipping_provider_name',]].reset_index(drop=True)


# COMMAND ----------

def generate_incorrect_status_report(month_year, mid):
    return tracker_details_df[
        (tracker_details_df['payment_month_year'] == month_year) &
         (tracker_details_df['merchant_id'] == mid)
                              ][['merchant_order_id','status','shipping_charges','awb_number','shipping_provider_name',]].reset_index(drop=True)

# COMMAND ----------

def convert_to_month_year_text(date_str):
    date_obj = datetime.strptime(date_str, '%Y-%m')
    month_name = date_obj.strftime('%B')  # %B gives the full month name
    year = date_obj.year
    result = f"{month_name} {year}"
    return result
convert_to_month_year_text('2023-07')

# COMMAND ----------



# COMMAND ----------


user = 'utkarsh.sarkar@razorpay.com'
app_password = 'ehvkxwxlhyfoucco' # Guide for app passwords: https://support.google.com/accounts/answer/185833?hl=en
host = 'smtp.gmail.com'
port = 465
server = smtplib.SMTP_SSL(host, port) 
server.login(user, app_password)


# COMMAND ----------



def send_emails(month_year, mid, mailer,subject_line,total_rto_amount, email_id, cc_id, mx_name):
    e_id = ['santosh.taduru@razorpay.com',
            'pallavi.samodia@razorpay.com'
            ]
    to = email_id
    cc = cc_id
    if cc != '':
        tocc = ['magic-rto@razorpay.com', cc,to]
    else:
        tocc = ['magic-rto@razorpay.com',to]
  
    a = '{0}-{1}.csv'.format(month_year, mid)
    mailer.to_csv(a, index=False)
    subject = subject_line
    attachment = a
    html_t = """
<html>
<head>
    <title>Reimbursement Process and Invoice Details</title>
</head>
<body>
    <p>Hi,</p>
    
    <p>Hope you are well.</p>
    
    <p>We are in the middle of processing the reimbursement for RTO protection for the month of {1}. As mentioned earlier you are required to raise an invoice with the reimbursement amount + GST (if applicable). You'll have to email us the invoices to gateway-invoices@razorpay.com , shashirekha.s@razorpay.com , nimisha.agarwal@razorpay.com with a subject line <b>"Magic checkout RTO protection - {0} | {1}"</b> and we will remit the amount within 30 days as per the standard norm. When sending the invoice, please attach the file with RTO details attached to this email as well for our reference.</p>
    
    <p>Please have a look at the file for more details. You are eligible for a reimbursement amount of INR {2}.</p>
    
    <p>Let me know if you have any questions.</p>
    
    <p>Request to provide the below details in the invoice:</p>
    
    <table border="1" cellpadding="5" cellspacing="0">
        <thead>
            <tr>
                <th>Sr. No</th>
                <th>Invoice Checklist</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td>1</td>
                <td>The invoice should be mentioned as "Tax Invoice / GST Invoice"</td>
            </tr>
            <tr>
                <td>2</td>
                <td>Supplier / Service Provider Name & Address</td>
            </tr>
            <tr>
                <td>3</td>
                <td>Invoice number and date</td>
            </tr>
            <tr>
                <td>4</td>
                <td>Supplier / Service Provider GST No</td>
            </tr>
            <tr>
                <td>5</td>
                <td>Customer name (Razorpay Name)</td>
            </tr>
            <tr>
                <td>6</td>
                <td>Customer GSTIN (Razorpay GSTIN)</td>
            </tr>
            <tr>
                <td>8</td>
                <td>Shipping Address (Razorpay Address)</td>
            </tr>
            <tr>
                <td>9</td>
                <td>HSN code</td>
            </tr>
            <tr>
                <td>10</td>
                <td>Description of services</td>
            </tr>
            <tr>
                <td>11</td>
                <td>Total value of supply of services</td>
            </tr>
            <tr>
                <td>12</td>
                <td>Taxable value of supply of services</td>
            </tr>
            <tr>
                <td>13</td>
                <td>Rate of tax (Central tax, State tax, Integrated tax, union territory tax or cess)</td>
            </tr>
            <tr>
                <td>14</td>
                <td>Amount of tax charged in respect of taxable or services (Central tax, State tax, Integrated tax, union territory tax or cess)</td>
            </tr>
            <tr>
                <td>15</td>
                <td>Place of supply along with the name of State, in case of a supply in the course of inter-State trade or commerce</td>
            </tr>
            <tr>
                <td>16</td>
                <td>Whether GST is payable on a reverse charge basis (Yes/No)</td>
            </tr>
            <tr>
                <td>17</td>
                <td>Supplier signature (Manually or Digitally) & Seal</td>
            </tr>

        </tbody>
    </table>

     <p><b>How Does RTO Reimbursement Work</b></p>

    <p>RTO Reimbursement gives sellers the freedom to ship COD orders without worrying about RTOs.</p>

    <p>If you're a seller who has opted for COD Intelligence:

    <li>Magic Checkout automatically blocks risky RTO users from placing COD orders.</li>

    <li>If you still get RTO orders from the COD orders that get placed, Magic Checkout will reimburse the reverse shipping cost at the rate agreed with you during onboarding.</li></p>

    <p>If you are a seller who has opted for Manual review of COD orders:

    <li>Magic Checkout will provide order-level risk to you via tags on your Shopify dashboard and COD orders tab in Magic Checkout on Razorpay dashboard for COD orders.</li>

    <li>The recommendation is to go ahead and ship ‘Low’ risk COD orders and take necessary action on High and Medium risk orders (Cancel order or contact the customer).</li>

    <li>If you ship low-risk COD orders and they still get RTO’ed, Magic Checkout will reimburse the reverse shipping cost at the rate agreed with you during onboarding.</li></p>

    <p><strong>Note:</strong> You should have enabled COD Intelligence or Manual review of COD orders from RTO Settings in Magic Checkout to receive reimbursement on RTO orders. In case neither of these are enabled, you will not be eligible for reimbursement.</p>
</body>
</html>
    """.format(mx_name, convert_to_month_year_text(month_year),total_rto_amount)

    message = MIMEMultipart()
    # add From 
    message['From'] = Header(user)
    # add To
    message['To'] = Header(to)
    message['cc'] = Header(cc)
    message['reply-to'] = "magic-rto@razorpay.com"
    # add Subject
    message['Subject'] = Header(subject)
    # add content text
    message.attach(MIMEText(html_t, 'html', 'utf-8'))
    # add attachment
    att_name = os.path.basename(attachment)
    att1 = MIMEText(open(attachment, 'rb').read(), 'base64', 'utf-8')
    att1['Content-Type'] = 'application/octet-stream'
    att1['Content-Disposition'] = 'attachment; filename=' + att_name
    message.attach(att1)
   
    ### Send email ###
    server.sendmail(user, tocc, message.as_string()) 
    print('Sent email successfully for: {0}' ,subject_line)

                

    
    


# COMMAND ----------



def send_incorrect_status_emails(month_year, mid, mailer,subject_line,total_rto_amount, email_id, cc_id,):
    e_id = ['angad.s@razorpay.com',
            'santosh.taduru@razorpay.com',
            'pallavi.samodia@razorpay.com'
            ]
    
    to = email_id
    
    cc = cc_id
    if cc != '':
        tocc = ['magic-rto@razorpay.com', cc,to]
    else:
        tocc = ['magic-rto@razorpay.com',to]
    a = '{0}-{1}.csv'.format(month_year, mid)
    mailer.to_csv(a, index=False)
    subject = subject_line
    attachment = a
    html_t = """
    <html>
    <body>
    <p>Hi,</p>
    
    <p>Hope you are well.</p>
    
    <p>We are in the middle of processing the reimbursement for RTO protection for the month of {1}</p>
    
    <p>We’ve found that your RTO reimbursement amount is Rs. 0 since there are no eligible RTOs in the data that we have received for your Magic Checkout orders for the month of {1}. We only see delivered orders in your data.</p>
    <p>If you believe that data is incorrect, you are required to manually upload the order delivery status for the RTO orders onto the Razorpay dashboard. You can undertake the following steps to upload your data: 
</p>
    
    <ol>
        <li>Login to your Razorpay account.</li>
        <li>Visit: <a href="https://dashboard.razorpay.com/app/magic/delivery-status">https://dashboard.razorpay.com/app/magic/delivery-status</a></li>
        <li>Select Delivery Statuses from the left, Click on the +Upload Delivery Statuses on the top right</li>
        <li><i>Attached is the file which you can use to update the missing details.</i></li>
    </ol>

    <ul>
        <li>Please make sure that you correctly mention your Shopify/WooCommerce order ids as seen in your orders dashboard on Shopify/WooCommerce. Examples are covered in the sample file.</li>
        <li>Also, please ensure that you use the standard shipping statuses (RTO, Delivered, Canceled, Returned, Lost).</li>
    </ul>
    
    <p>Post this, we will reach out with the reimbursement amount and you can raise the invoice to us.</p>
    
    <p>Please revert if you have any questions/trouble in uploading the data.</p>
    
    <hr> 
     <h1>How Does RTO Reimbursement Work</h1>

    <p>RTO Reimbursement gives sellers the freedom to ship COD orders without worrying about RTOs.</p>

    <p>If you're a seller who has opted for COD Intelligence:

    <li>Magic Checkout automatically blocks risky RTO users from placing COD orders.</li>

    <li>If you still get RTO orders from the COD orders that get placed, Magic Checkout will reimburse the reverse shipping cost at the rate agreed with you during onboarding.</li></p>

    <p>If you are a seller who has opted for Manual review of COD orders:

    <li>Magic Checkout will provide order-level risk to you via tags on your Shopify dashboard and COD orders tab in Magic Checkout on Razorpay dashboard for COD orders.</li>

    <li>The recommendation is to go ahead and ship ‘Low’ risk COD orders and take necessary action on High and Medium risk orders (Cancel order or contact the customer).</li>

    <li>If you ship low-risk COD orders and they still get RTO’ed, Magic Checkout will reimburse the reverse shipping cost at the rate agreed with you during onboarding.</li></p>

    <p><strong>Note:</strong> You should have enabled COD Intelligence or Manual review of COD orders from RTO Settings in Magic Checkout to receive reimbursement on RTO orders. In case neither of these are enabled, you will not be eligible for reimbursement.</p>

</body>

</html>
    """.format(mid, convert_to_month_year_text(month_year),total_rto_amount)

    message = MIMEMultipart()
    # add From 
    message['From'] = Header(user)
    # add To
    message['To'] = Header(to)
    message['cc'] = Header(cc)
    message['reply-to'] = "magic-rto@razorpay.com"
    # add Subject
    message['Subject'] = Header(subject)
    # add content text
    message.attach(MIMEText(html_t, 'html', 'utf-8'))
    # add attachment
    att_name = os.path.basename(attachment)
    att1 = MIMEText(open(attachment, 'rb').read(), 'base64', 'utf-8')
    att1['Content-Type'] = 'application/octet-stream'
    att1['Content-Disposition'] = 'attachment; filename=' + att_name
    message.attach(att1)
   
    ### Send email ###
    server.sendmail(user, tocc, message.as_string()) 
    print('Sent email successfully for: {0}' ,subject_line)

 
  
                

    
    


# COMMAND ----------



def send_pending_emails(month_year, mid, mailer,subject_line,total_rto_amount, email_id, cc_id,):
    e_id = ['angad.s@razorpay.com',
            'santosh.taduru@razorpay.com',
            'pallavi.samodia@razorpay.com'
            ]
 
    to = email_id

    cc = cc_id
    if cc != '':
        tocc = ['magic-rto@razorpay.com', cc,to]
    else:
        tocc = ['magic-rto@razorpay.com',to]
    a = '{0}-{1}.csv'.format(month_year, mid)
    mailer.to_csv(a, index=False)
    subject = subject_line
    attachment = a
    html_t = """
    <html>
    <body>
    <p>Hi,</p>
    
    <p>Hope you are well.</p>
    
    <p>We are in the middle of processing the reimbursement for RTO protection for the month of {1}</p>
    
    <p>We’ve found that we don’t have the delivery status for some of your Magic Checkout orders for the month of {1}. To process the reimbursement, you are required to manually upload the order delivery status for each of the attached orders onto the Razorpay dashboard. You can undertake the following steps to upload your data:</p>
    
    <ol>
        <li>Login to your Razorpay account.</li>
        <li>Visit: <a href="https://dashboard.razorpay.com/app/magic/delivery-status">https://dashboard.razorpay.com/app/magic/delivery-status</a></li>
        <li>Select Delivery Statuses from the left, Click on the +Upload Delivery Statuses on the top right</li>
        <li><i>Attached is the file which you can use to update the missing details.</i></li>
    </ol>

    <ul>
        <li>Please make sure that you correctly mention your Shopify/WooCommerce order ids as seen in your orders dashboard on Shopify/WooCommerce. Examples are covered in the sample file.</li>
        <li>Also, please ensure that you use the standard shipping statuses (RTO, Delivered, Canceled, Returned, Lost).</li>
    </ul>
    
    <p>Post this, we will reach out with the reimbursement amount and you can raise the invoice to us.</p>
    
    <p>Please revert if you have any questions/trouble in uploading the data.</p>
    <hr> 
     <h1>How Does RTO Reimbursement Work</h1>

    <p>RTO Reimbursement gives sellers the freedom to ship COD orders without worrying about RTOs.</p>

    <p>If you're a seller who has opted for COD Intelligence:

    <li>Magic Checkout automatically blocks risky RTO users from placing COD orders.</li>

    <li>If you still get RTO orders from the COD orders that get placed, Magic Checkout will reimburse the reverse shipping cost at the rate agreed with you during onboarding.</li></p>

    <p>If you are a seller who has opted for Manual review of COD orders:

    <li>Magic Checkout will provide order-level risk to you via tags on your Shopify dashboard and COD orders tab in Magic Checkout on Razorpay dashboard for COD orders.</li>

    <li>The recommendation is to go ahead and ship ‘Low’ risk COD orders and take necessary action on High and Medium risk orders (Cancel order or contact the customer).</li>

    <li>If you ship low-risk COD orders and they still get RTO’ed, Magic Checkout will reimburse the reverse shipping cost at the rate agreed with you during onboarding.</li></p>

    <p><strong>Note:</strong> You should have enabled COD Intelligence or Manual review of COD orders from RTO Settings in Magic Checkout to receive reimbursement on RTO orders. In case neither of these are enabled, you will not be eligible for reimbursement.</p>
</body>
</html>
    """.format(mid, convert_to_month_year_text(month_year),total_rto_amount)

    message = MIMEMultipart()
    # add From 
    message['From'] = Header(user)
    # add To
    message['To'] = Header(to)
    message['cc'] = Header(cc)
    message['reply-to'] = "magic-rto@razorpay.com"
    # add Subject
    message['Subject'] = Header(subject)
    # add content text
    message.attach(MIMEText(html_t, 'html', 'utf-8'))
    # add attachment
    att_name = os.path.basename(attachment)
    att1 = MIMEText(open(attachment, 'rb').read(), 'base64', 'utf-8')
    att1['Content-Type'] = 'application/octet-stream'
    att1['Content-Disposition'] = 'attachment; filename=' + att_name
    message.attach(att1)
   
    ### Send email ###
    server.sendmail(user, tocc, message.as_string()) 
    print('Sent email successfully for: {0}' ,subject_line)

 
  
                

    
    


# COMMAND ----------

#Additional columns for tracking
tracker_summary_df['order_percentage'] = round(tracker_summary_df['order_count_w_delivery'] * 1.00 / tracker_summary_df['total_order_count'],4)

# COMMAND ----------

tracker_summary_df.dtypes

# COMMAND ----------

tracker_summary_df[(tracker_summary_df['order_count_w_delivery'] == tracker_summary_df['order_count_w_rto']) & (tracker_summary_df['order_count_w_delivery'] != 0) ]

# COMMAND ----------

tracker_summary_df[(tracker_summary_df['merchant_id'] =='EJu3xUe1qpgeB8') & (tracker_summary_df['payment_month_year'] <= (end_date)) & (tracker_summary_df['invoice_status']=='')]


# COMMAND ----------

tracker_summary_df

# COMMAND ----------

tracker_summary_df[tracker_summary_df['name']=='OLYMPIA INDUSTRIES LIMITED']

# COMMAND ----------

tracker_summary_df[tracker_summary_df['payment_month_year']=='2024-10']

# COMMAND ----------


today = datetime.now().strftime('%Y-%m-%d')

#tracker_summary_df['status']=''
for index, row in tracker_summary_df.iterrows():
    tracker_summary_df.at[index,'rto_amount'] = (calculate_rto_amount(row['payment_month_year'], row['merchant_id']))
    #if row['payment_month_year'] == '2023-08':
    if row['payment_month_year'] <= end_date and row['invoice_status'] != 'Success' and row['invoice_status'] == '':
        #Case when we have enough delivery data + RTO amount is not missing
        if (row['order_percentage'] >= 0.80 and row['rto_amount'] != 0.0 and row['rto_amount'] !='' and row['invoice_status'] != 'Success'):
            mailer = generate_order_level_report(row['payment_month_year'], row['merchant_id'])
            subject = '[Razorpay] RTO Report for: {0} | Period: {1}'.format(row['name'],convert_to_month_year_text(row['payment_month_year']) )
            send_emails(row['payment_month_year'], row['merchant_id'], mailer,subject, tracker_summary_df.loc[index,'rto_amount'],row['email'], row['email_cc'], row['name'] )
            tracker_summary_df.to_csv('/dbfs/FileStore/magic_rto_reimbursement_tracker_summary.csv')
            tracker_summary_df.at[index,'rto_invoiced'] = today
            tracker_summary_df.at[index,'invoice_status'] = 'Success'
            tracker_summary_df.at[index,'status'] = 'Successful'
            
           
            print(subject)

        #Case when enough delivery data present  but RTO amount is 0 indicating we did not receive actual delivery status for the mx
        elif row['order_percentage'] >= 0.80 and row['reminder_sent'] != today and current_date.day in (14, 28):
        #elif row['order_percentage'] >= 0.80 and row['reminder_sent'] != today:
            mailer = generate_incorrect_status_report(row['payment_month_year'], row['merchant_id'])
            subject = '[Razorpay] Delivery Status for: {0} | Period: {1}'.format(row['name'],convert_to_month_year_text(row['payment_month_year']))
            print(subject)
            
            send_incorrect_status_emails(row['payment_month_year'], row['merchant_id'], mailer,subject, tracker_summary_df.loc[index,'rto_amount'],row['email'], row['email_cc'], )
            tracker_summary_df.to_csv('/dbfs/FileStore/magic_rto_reimbursement_tracker_summary.csv')
            tracker_summary_df.at[index,'reminder_sent'] = today
            tracker_summary_df.at[index,'status'] = 'Incorrect status'
            
            



        #Case when enough delivery data is not present    
        elif row['order_percentage'] < 0.80 and row['reminder_sent'] != today and current_date.day in (14, 28):
        #elif row['order_percentage'] < 0.80 and row['reminder_sent'] != today:
        
            mailer = generate_prending_status_report(row['payment_month_year'], row['merchant_id'])
            subject = '[Razorpay] Pending Delivery Status for: {0} | Period: {1}'.format(row['name'],convert_to_month_year_text(row['payment_month_year']))
            print(subject)
            
            send_pending_emails(row['payment_month_year'], row['merchant_id'], mailer,subject, tracker_summary_df.loc[index,'rto_amount'],row['email'], row['email_cc'], )
            tracker_summary_df.to_csv('/dbfs/FileStore/magic_rto_reimbursement_tracker_summary.csv')
            tracker_summary_df.at[index,'reminder_sent'] = today
            tracker_summary_df.at[index,'status'] = 'Pending status'
            
            
    else:
        None

send_emails(datetime.now().strftime('%Y-%m'), 'All Merchants', tracker_summary_df,'RTO Reimbursement Tracker Summary', 0,'magic-rto@razorpay.com','ismail.alam@razorpay.com','Razorpay')

#also sending a copy to finance team + CSM so they have the updated data

send_emails(datetime.now().strftime('%Y-%m'), 'All Merchants', tracker_summary_df,'RTO Reimbursement Tracker Summary', 0,'shashirekha.s@razorpay.com','chetna.handa@razorpay.com','Razorpay')





# COMMAND ----------



# COMMAND ----------

tracker_summary_df[tracker_summary_df['merchant_id']=='Hs5Tc4CWinf3um']

# COMMAND ----------

tracker_summary_df.info()


# COMMAND ----------

tracker_summary_df.to_csv('/dbfs/FileStore/magic_rto_reimbursement_tracker_summary.csv')

# COMMAND ----------


server.quit()

# COMMAND ----------

# https://razorpay-dev.cloud.databricks.com/files/magic_rto_reimbursement_tracker_summary.csv
