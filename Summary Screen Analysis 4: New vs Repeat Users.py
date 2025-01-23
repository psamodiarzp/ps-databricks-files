# Databricks notebook source
import matplotlib.pyplot as plt
import pandas as pd

# COMMAND ----------

device_base_db = sqlContext.sql("""
select get_json_object(context, '$["device.id"]'), min(producer_created_date) as min_date
from aggregate_pa.cx_1cc_events_dump_v1
  group by 1
""")

device_base_df = device_base_db.toPandas()
device_base_df.head()

# COMMAND ----------

device_base_df['min_date'] = device_base_df['min_date'].astype('str')

# COMMAND ----------

a = list(device_base_df[device_base_df['min_date'] == '2023-08-01']['device_id'])
b = list(summary_part2_df[(summary_part2_df['producer_created_date']=='2023-08-01') 
                          & (summary_part2_df['new_or_repeating_user']=='new')]['device_id'].unique())
print(len(a))
print(len(b))


# COMMAND ----------

device_base_df.to_csv('/dbfs/FileStore/device_base_df.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/device_base_df.csv"

# COMMAND ----------

#Repeating Users
summary_part2_db = sqlContext.sql("""
with base as(
select get_json_object(context, '$["device.id"]') AS device_id, min(producer_created_date) as min_date
from aggregate_pa.cx_1cc_events_dump_v1
  group by 1
  
)
select 
distinct
get_json_object(a.context, '$["device.id"]') AS device_id,
producer_created_date,
case when producer_created_date = min_date then 'new'
when producer_created_date > min_date then 'repeating'
 else null end as new_or_repeating_user,
 min_date,
CAST(
          get_json_object(properties, '$.data.meta.initial_loggedIn') AS boolean
        )
     AS initial_loggedin,
--DATEDIFF(producer_created_date, min_date) AS date_difference
checkout_id,
event_name,
get_json_object(properties,'$.options.amount') as order_amount,
merchant_id,
browser_name,
get_json_object(context,'$.user_agent_parsed.os.family') as os
from aggregate_pa.cx_1cc_events_dump_v1 a 
left join base on get_json_object(a.context, '$["device.id"]') = base.device_id
where producer_created_date >= date('2023-08-01') 
and event_name in ('render:1cc_summary_screen_loaded_completed')
""")

summary_part2_df = summary_part2_db.toPandas()
summary_part2_df.head()

# COMMAND ----------

summary_part2_df.to_csv('/dbfs/FileStore/summary_part2_df.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/summary_part2_df.csv"

# COMMAND ----------

summary_part2_df = pd.read_csv('/dbfs/FileStore/summary_part2_df.csv')

# COMMAND ----------

summary_part2_df.shape

# COMMAND ----------

summary_part2_df['producer_created_date'] = summary_part2_df['producer_created_date'].astype('str')
summary_part2_df = summary_part2_df[~summary_part2_df['order_amount'].isna()]
summary_part2_df['order_amount'] = summary_part2_df['order_amount'].astype('int')

# COMMAND ----------

new_total = summary_part2_df.groupby(by=['new_or_repeating_user','initial_loggedin']).agg({'device_id':'nunique','checkout_id':'nunique'}).reset_index()
new_total['device_percentage'] = new_total['device_id'] / new_total['device_id'].sum()
new_total

# COMMAND ----------

aug_1 = summary_part2_df[summary_part2_df['producer_created_date']=='2023-08-01'].groupby(by=['new_or_repeating_user',]).agg({'checkout_id':'nunique','device_id':'nunique'}).reset_index()
aug_1
#ug_1[aug_1['new_or_repeating_user']=='repeating'].sort_values(by='checkout_id', ascending=False)

# COMMAND ----------

summary_part2_df['aov_bucket'] = pd.cut(summary_part2_df['order_amount']/100, bins=[0,500,1000,2000,5000,10000, 20000, 50000])
summary_part2_df['aov_bucket'] = summary_part2_df['aov_bucket'].astype('str')
summary_part2_df

# COMMAND ----------

total_cr = summary_part2_df.groupby(by=['new_or_repeating_user','initial_loggedin']).agg({'device_id':'nunqiue'}).reset_index()
total_cr['device_id'] = total_cr['device_id']*1.00 / total_cr['device_id'].sum()
total_cr

# COMMAND ----------

summary_cr_db = sqlContext.sql("""
select checkout_id,  (case
          when magic_checkout_fact.summary_screen_continue_cta_clicked = 1 then 'Summary Screen CTA clicked'
          when magic_checkout_fact.summary_screen_loaded = 0 then 'Summary Screen did not load'
        --  when magic_checkout_fact.summary_screen_loaded = 1 and magic_checkout_fact.summary_screen_continue_cta_clicked = 1 then 'Exited Summary Screen successfully'
          when magic_checkout_fact.summary_screen_continue_cta_clicked = 0
          and (magic_checkout_fact.contact_email_entered=1 or magic_checkout_fact.contact_number_entered=1
    or magic_checkout_fact.clicked_change_contact=1
    or (magic_checkout_fact.contact_fill_began=1 and magic_checkout_fact.prefill_contact_number=0)
    or (magic_checkout_fact.email_fill_began=1 and magic_checkout_fact.prefill_email=0)) = True and (magic_checkout_fact.have_coupon_clicked=1 or magic_checkout_fact.coupon_screen_loaded=1) = False then 'Interacted w contact but not coupons'
          when magic_checkout_fact.summary_screen_continue_cta_clicked = 0
          and (magic_checkout_fact.contact_email_entered=1 or magic_checkout_fact.contact_number_entered=1
    or magic_checkout_fact.clicked_change_contact=1
    or (magic_checkout_fact.contact_fill_began=1 and magic_checkout_fact.prefill_contact_number=0)
    or (magic_checkout_fact.email_fill_began=1 and magic_checkout_fact.prefill_email=0)) = False and (magic_checkout_fact.have_coupon_clicked=1 or magic_checkout_fact.coupon_screen_loaded=1) = True then 'Interacted w coupons but not contact'
          when magic_checkout_fact.summary_screen_continue_cta_clicked = 0
          and (magic_checkout_fact.contact_email_entered=1 or magic_checkout_fact.contact_number_entered=1
    or magic_checkout_fact.clicked_change_contact=1
    or (magic_checkout_fact.contact_fill_began=1 and magic_checkout_fact.prefill_contact_number=0)
    or (magic_checkout_fact.email_fill_began=1 and magic_checkout_fact.prefill_email=0)) = True and (magic_checkout_fact.have_coupon_clicked=1 or magic_checkout_fact.coupon_screen_loaded=1) = True then 'Interacted w both coupons and contact'
          when magic_checkout_fact.summary_screen_continue_cta_clicked = 0
          and (magic_checkout_fact.contact_email_entered=1 or magic_checkout_fact.contact_number_entered=1
    or magic_checkout_fact.clicked_change_contact=1
    or (magic_checkout_fact.contact_fill_began=1 and magic_checkout_fact.prefill_contact_number=0)
    or (magic_checkout_fact.email_fill_began=1 and magic_checkout_fact.prefill_email=0)) = False and (magic_checkout_fact.have_coupon_clicked=1 or magic_checkout_fact.coupon_screen_loaded=1) = False then 'Bounced w/o any interaction'
          when magic_checkout_fact.summary_screen_continue_cta_clicked = 0
          and magic_checkout_fact.edit_address_clicked = 1 then 'Exited to Edit Address'
          else 'Others'
          end
) AS summary_screen_dropoffs
from aggregate_pa.magic_checkout_fact
where producer_created_date >= date('2023-08-01')
""")
summary_cr_df = summary_cr_db.toPandas()
summary_cr_df.head()

# COMMAND ----------

summary_cr_df.to_csv('/dbfs/FileStore/summary_cr_df.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/summary_cr_df.csv"

# COMMAND ----------

summary_cr_df = pd.read_csv('/dbfs/FileStore/summary_cr_df.csv')
summary_cr_df

# COMMAND ----------

percentiles = [5, 10, 25, 50, 75, 90, 95]
summary_part2_df['order_amount'].quantile([p / 100 for p in percentiles])
#summary_part2_df['order_amount'].describe()

# COMMAND ----------

summary_temp = summary_part2_df.groupby(by=['device_id', 'producer_created_date', 'new_or_repeating_user',
       'min_date',  'checkout_id',
       'merchant_id', 'browser_name', 'os']).agg({'initial_loggedin':'max','order_amount':'max'})


# COMMAND ----------

summary_temp = summary_temp.reset_index()
summary_temp.head(10)

# COMMAND ----------

summary_final = summary_temp.merge(summary_cr_df, on='checkout_id', how='left')
summary_final = summary_final[summary_final['device_id']!='']
summary_final

# COMMAND ----------



# COMMAND ----------

summary_final['aov_bucket'] = pd.cut(summary_final['order_amount']/100, bins=[0,500,1000,2000,5000,10000, 20000, 50000])
summary_final['aov_bucket'] = summary_final['aov_bucket'].astype('str')
summary_final

# COMMAND ----------

total_users = summary_final.groupby(by=['new_or_repeating_user','initial_loggedin']).agg({'device_id':'nunique','checkout_id':'nunique'}).reset_index()
total_users['col_percent'] = total_users['device_id'] / total_users['device_id'].sum()
total_users['checkout_percent'] = total_users['checkout_id'] / total_users['checkout_id'].sum()
total_users

# COMMAND ----------

summary_reasons_temp = summary_final.groupby(by=['new_or_repeating_user','initial_loggedin','summary_screen_dropoffs']).agg({'checkout_id':'nunique'}).reset_index()
summary_reasons = summary_reasons_temp.pivot(index=['new_or_repeating_user','initial_loggedin'], columns=['summary_screen_dropoffs'], values='checkout_id').reset_index()
summary_reasons.head(10)

# COMMAND ----------

summary_reasons['total'] = summary_reasons.iloc[:,-5:].sum(axis=1)
summary_reasons['converted'] = summary_reasons['Summary Screen CTA clicked'] * 1.00 / summary_reasons['total']

summary_reasons['post_load_bounce'] = summary_reasons['Bounced w/o any interaction']  * 1.00 / summary_reasons['total']
summary_reasons['post_engagement_dropoff'] = 1 - (summary_reasons['converted']  + summary_reasons['post_load_bounce'])
summary_reasons.head(10)

# COMMAND ----------

#Summary CR x AOV only for Android [for normalization purposes]
summary_reasons_aov_temp = summary_final[summary_final['os']=='Android'].groupby(by=['new_or_repeating_user','initial_loggedin','aov_bucket','summary_screen_dropoffs']).agg({'checkout_id':'nunique'}).reset_index()
summary_reasons_aov = summary_reasons_aov_temp.pivot(index=['new_or_repeating_user','initial_loggedin','aov_bucket'], columns=['summary_screen_dropoffs'], values='checkout_id').reset_index()
summary_reasons_aov['total'] = summary_reasons_aov.iloc[:,-5:].sum(axis=1)
summary_reasons_aov['converted'] = summary_reasons_aov['Summary Screen CTA clicked'] * 1.00 / summary_reasons_aov['total']
summary_reasons_aov.head()

# COMMAND ----------

summary_reasons_aov_final = summary_reasons_aov[summary_reasons_aov['aov_bucket'].isin(['(0, 500]','(500, 1000]','(1000, 2000]','(2000, 5000]'])].pivot(index=['new_or_repeating_user','initial_loggedin'], columns=['aov_bucket'], values=['total','converted']).reset_index()
summary_reasons_aov_final.sort_values(by=['new_or_repeating_user','initial_loggedin',], ascending=False)

# COMMAND ----------

total_cr = summary_final.groupby(by=['new_or_repeating_user','initial_loggedin']).agg({'open':'sum','summary_screen_continue_cta_clicked':'sum'}).reset_index()
total_cr['summary_cr'] = total_cr['summary_screen_continue_cta_clicked']*1.00 / total_cr['open']
total_cr

# COMMAND ----------

summary_final

# COMMAND ----------

browser_os_temp = summary_final[(summary_final['os']=='Android') & (summary_final['browser_name'].isin(['Facebook','Instagram','Chrome Mobile']))].groupby(by=['new_or_repeating_user','initial_loggedin','browser_name','merchant_id']).agg({'checkout_id':'count',}).reset_index()
browser_os_temp = browser_os_temp.fillna(0)
#browser_os_temp['summary_cr'] = browser_os_temp['summary_screen_continue_cta_clicked']*1.00 / browser_os_temp['open']
browser_os = browser_os_temp.pivot(index=['merchant_id',], columns=['browser_name','new_or_repeating_user','initial_loggedin'], values=['checkout_id']).reset_index()
browser_os = browser_os.fillna(0)
#browser_os.columns = browser_os.columns.map('_'.join)
#browser_os.sort_values(by='open_repeating', ascending=False)
browser_os

# COMMAND ----------

browser_os.to_csv('/dbfs/FileStore/browser_os.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/browser_os.csv"

# COMMAND ----------

browser_os_temp = summary_final.groupby(by=['new_or_repeating_user','browser_name','os']).agg({'open':'sum','summary_screen_continue_cta_clicked':'sum'}).reset_index()
browser_os_temp = browser_os_temp.fillna(0)
browser_os_temp['summary_cr'] = browser_os_temp['summary_screen_continue_cta_clicked']*1.00 / browser_os_temp['open']
browser_os = browser_os_temp.pivot(index=['browser_name','os'], columns=['new_or_repeating_user'], values=['open',
       'summary_screen_continue_cta_clicked', 'summary_cr']).reset_index()
browser_os = browser_os.fillna(0)
browser_os.columns = browser_os.columns.map('_'.join)
browser_os.sort_values(by='open_repeating', ascending=False)

# COMMAND ----------

aov_temp = summary_final.groupby(by=['new_or_repeating_user','aov_bucket']).agg({'open':'sum','summary_screen_continue_cta_clicked':'sum'}).reset_index()
aov_temp = aov_temp.fillna(0)
aov_temp['summary_cr'] = aov_temp['summary_screen_continue_cta_clicked']*1.00 / aov_temp['open']
aov_final = aov_temp.pivot(index=['aov_bucket'], columns=['new_or_repeating_user'], values=['open',
       'summary_screen_continue_cta_clicked', 'summary_cr']).reset_index()
aov_final = aov_final.fillna(0)
aov_final.columns = aov_final.columns.map('_'.join)
aov_final['repeat_percentage'] = aov_final['open_repeating'] /(aov_final['open_new'] + aov_final['open_repeating'])
aov_final.sort_values(by='open_repeating', ascending=False)

# COMMAND ----------

aov_final[['aov_bucket_','repeat_percentage']]

# COMMAND ----------

summary_final_category = summary_final.merge(mx_final, left_on='merchant_id',right_on='id', how='inner')
summary_final_category

# COMMAND ----------

summary_final_category[summary_final_category['category'] == '5499']

# COMMAND ----------

aov_temp = summary_final_category.groupby(by=['new_or_repeating_user','aov_bucket','mx_category']).agg({'checkout_id':'count','merchant_id':'nunique'}).reset_index()
aov_temp = aov_temp.fillna(0)
#aov_temp['summary_cr'] = aov_temp['summary_screen_continue_cta_clicked']*1.00 / aov_temp['open']
aov_final = aov_temp.pivot(index=['aov_bucket','mx_category'], columns=['new_or_repeating_user'], values=[
       'checkout_id',]).reset_index()
aov_final = aov_final.fillna(0)
aov_final.columns = aov_final.columns.map('_'.join)
aov_final['repeat_percentage'] = aov_final['checkout_id_repeating'] /(aov_final['checkout_id_new'] + aov_final['checkout_id_repeating'])
aov_final.sort_values(by='checkout_id_repeating', ascending=False)

# COMMAND ----------

aov_final.to_csv('/dbfs/FileStore/aov_final.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/aov_final.csv"

# COMMAND ----------

mid_temp = summary_final.groupby(by=['new_or_repeating_user','merchant_id']).agg({'open':'sum','summary_screen_continue_cta_clicked':'sum'}).reset_index()
mid_temp = mid_temp.fillna(0)
mid_temp['summary_cr'] = mid_temp['summary_screen_continue_cta_clicked']*1.00 / mid_temp['open']
mid_final = mid_temp.pivot(index=['merchant_id'], columns=['new_or_repeating_user'], values=['open',
       'summary_screen_continue_cta_clicked', 'summary_cr']).reset_index()
mid_final = mid_final.fillna(0)
mid_final.columns = mid_final.columns.map('_'.join)
mid_final['total_open'] =mid_final['open_new'] + mid_final['open_repeating']
mid_final['repeat_rate'] = mid_final['open_repeating'] / mid_final['total_open']
mid_final.sort_values(by='open_repeating', ascending=False)

# COMMAND ----------

mid_final.to_csv('/dbfs/FileStore/mid_final.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/mid_final.csv"

# COMMAND ----------



# COMMAND ----------

mx_db = sqlContext.sql(

"""
select category, id, website
from realtime_hudi_api.merchants
"""
)
mx_df = mx_db.toPandas()
mx_df.head()

# COMMAND ----------

mx_df

# COMMAND ----------

mcc_list = mcc_list.groupby(by='category').max().reset_index()
mcc_list

# COMMAND ----------

mx_final

# COMMAND ----------

mcc_list['category'] = mcc_list['category'].astype('str')
mx_final = mx_df.merge(mcc_list, on='category', how='inner')
mx_final = mx_final.groupby(by='id').agg('max')
mx_final

# COMMAND ----------

mid_level_2 = mid_final.merge(mx_df, left_on='merchant_id_', right_on='id', how='left')
mid_level_2['repeat_percentage'] = mid_level_2['open_repeating'] /(mid_level_2['open_repeating']+mid_level_2['open_new'])
mid_level_3 = mid_level_2.merge(mcc_list, on='category',how='left')
mid_level_3

# COMMAND ----------

aov_temp = summary_final.groupby(by=['new_or_repeating_user','aov_bucket']).agg({'open':'sum','summary_screen_continue_cta_clicked':'sum'}).reset_index()
aov_temp = aov_temp.fillna(0)
aov_temp['summary_cr'] = aov_temp['summary_screen_continue_cta_clicked']*1.00 / aov_temp['open']
aov_final = aov_temp.pivot(index=['aov_bucket'], columns=['new_or_repeating_user'], values=['open',
       'summary_screen_continue_cta_clicked', 'summary_cr']).reset_index()
aov_final = aov_final.fillna(0)
aov_final.columns = aov_final.columns.map('_'.join)
aov_final.sort_values(by='open_repeating', ascending=False)


# COMMAND ----------

mid_level_3

# COMMAND ----------

mid_level_3[mid_level_3['mx_category']=='Fashion and Lifestyle']

# COMMAND ----------

mid_level_3.to_csv('/dbfs/FileStore/mid_level_3.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/mid_level_3.csv"

# COMMAND ----------

mid_level_3.groupby(by='mx_category')['repeat_percentage'].describe()

# COMMAND ----------

mcc_list

# COMMAND ----------

mcc_list['category'] = mcc_list['category'].astype('str')
mcc_list

# COMMAND ----------

#mcc_list
data = {
    'mx_category': [
'Mutual Fund',
'Lending',
'Cryptocurrency',
'Insurance',
'NBFC',
'Cooperatives',
'Pension Fund',
'Forex',
'Securities',
'Commodities',
'Accounting and Taxes',
'Financial and Investment Advisors/Financial Advisor',
'Crowdfunding Platform',
'Stock Brokerage and Trading',
'Betting',
'Get Rich Schemes',
'MoneySend Funding',
'WIRE TRANSFER MONEY ORDER',
'Tax Preparation Service',
'Tax Payments',
'Digital Goods-Multi-Category',
'Financial Institutions-Automated Cash Disbursements',
'College',
'Schools',
'University',
'Professional Courses',
'Distance Learning',
'Pre-School/Day Care',
'Coaching Institute',
'E-Learning',
'SCHOOLS, TRADE AND VOCATIONAL',
'ATHLETIC FEES',
'Dance Halls, Studios and Schools',
'Schools, Correspondence',
'Pharmacy',
'Clinic',
'Hospital',
'Lab',
'Dietician/Diet Services',
'Gym and Fitness',
'Health and Lifestyle Coaching',
'Health Products',
'Marketplace/Aggregator',
'Osteopaths',
'Dental/Laboratory/Medical/Ophthalmic Hospital Equipment and Supplies',
'DRUGS, DRUG PROPRIETARIES',
'Podiatrists and Chiropodists',
'Dentists, Orthodontists',
'Hardware Stores',
'Optometrists, Ophthalmologists',
'Orthopedic Goods-Artificial Limb Stores',
'Health Practitioners, Medical Services-not elsewhere classified',
'Testing Laboratories-Non medical',
'Doctors-not elsewhere classified',
'Electricity',
'Gas',
'Telecom Service Provider',
'Water',
'Cable operator',
'Broadband',
'DTH',
'Internet service provider',
'Bill Payment and Recharge Aggregators',
'Central Department',
'State Department',
'Intra-Government Purchases -Government Only',
'Postal Services -Government Only',
'Freight Consolidation/Management',
'Courier Shipping',
'Public/Contract Warehousing',
'Distribution Management',
'End-to-end logistics',
'Courier Services-Air &amp, Ground, Freight Forwarders',
'Aviation',
'Lodging and Accommodation',
'OTA',
'Tours and Travel Agency',
'Tourist Attractions and Exhibits',
'Aquariums, Dolphinariums, and Seaquariums',
'Timeshares',
'Cab/auto hailing',
'Bus ticketing',
'Train and metro ticketing',
'National Car Rental',
'CRUISE LINES',
'Automobile Parking Lots and Garages',
'BRIDGE &amp, ROAD FEES, TOLLS',
'Railroads and Freight',
'Truck and Utility Trailer Rentals',
'Transportation‚ÄîSuburban and Local Commuter Passenger, including Ferries',
'Horizontal Commerce/Marketplace',
'Agricultural products',
'Books and Publications',
'Electronics and Furniture',
'Coupons and deals',
'Product Rental',
'Fashion and Lifestyle',
'Flowers and Gifts',
'Grocery',
'Baby Care and Toys',
'Office Supplies',
'Wholesale/Bulk trade',
'Religious products',
'Pet Care and Supplies',
'Sports goods',
'Arts, crafts and collectibles',
'Sexual Wellness Products',
'Dropshipping',
'Crypto Machinery',
'Tobacco',
'Weapons and Ammunitions',
'Stamps & Coins Stores',
'MOTOR VEHICLE SUPPLIES',
'Office, Photographic, Photocopy, and Microfilm Equipment',
'Lawn and Garden Supply Stores',
'Household Appliance Stores',
'NONDURABLE GOODS',
'Electrical Parts and Equipment',
'Wig and Toupee Shops',
'Card, Gift, Novelty, and Souvenir Shops',
'Duty Free Stores',
'Office and Commercial Furniture',
'Piece Goods, Notions, and Other Dry Goods',
'Book Stores',
'Camera and Photographic Supply Stores',
'Freezer, Locker Meat Provisioners',
'Leather Goods and Luggage Stores',
'Snowmobile Dealers',
'Men‚Äôs and Boy‚Äôs Clothing and Furnishings Store',
'Varnishes, Paints, Supplies',
'Automotive Parts, Accessories Stores',
'Precious Stones and Metals,Watches and Jewelry',
'Auto Store, Home Supply Stores',
'Tent and Awning Shops',
'Petroleum and Petroleum Products',
'Department Stores',
'Shoe Stores',
'Automotive Tire Stores',
'Sports Apparel, Riding Apparel Stores',
'Chemicals and Allied Products',
'FIREPLACE FIREPLACE SCREENS AND ACCESSORIES STORES',
'COMMERCIAL EQUIPMENTS',
'Family Clothing Stores',
'Fabric, Needlework, Piece Goods, and Sewing Stores',
'Camper, Recreational and utility trailer dealers',
'Record Shops',
'Home Supply Warehouse',
'Clock, Jewelry, Watch, and Silverware Store',
'Artist Supply Stores, Craft Shops',
'Pawn Shops',
'Office, School Supply, and Stationery Stores',
'Opticians, Optical Goods, and Eyeglasses',
'Clock, Jewelry, and Watch Repair Shops',
'Commercial Footwear',
'Antique Reproduction Stores',
'Plumbing and Heating Equipment',
'Variety Stores',
'Package Stores, Beer, Wine, Liquor',
'Boat Dealers',
'Cosmetic Stores',
'Miscellaneous House Furnishing Specialty Shops',
'Telecommunication Equipment Including Telephone Sales',
'Womens Ready to Wear Stores',
'Florists',
'Commercial Art, Graphics, Photography',
'Building Materials, Lumber Stores',
'Candy, Nut, Confectionery Stores',
'Glass, Paint, Wallpaper Stores',
'Video Amusement Game Supplies',
'Drapery, Upholstery, and Window Coverings Stores',
'Men‚Äôs, Women‚Äôs, and Children‚Äôs Uniforms and Commercial Clothing',
'Automotive Paint Shops',
'Durable Goods not elsewhere classified',
'Furriers and Fur Shops',
'Industrial Supplies',
'Motorcycle Shops and Dealers',
'Children‚Äôs and Infants‚Äô Wear Stores',
'Computer Software Stores',
'Women Accessory and Specialty Stores',
'Books, Periodicals and Newspapers',
'Floor Covering Stores',
'Crystal and Glassware Stores',
'Hardware Equipement and Supplies',
'Discount stores',
'Computers, Computer Peripheral Equipment, Software',
'Automobile and Truck Dealers-Used Only-Sales',
'Miscellaneous Automotive, Aircraft, and Farm Equipment Dealers-not elsewhere classified',
'Antique Shops-Sales, Repairs, and Restoration Services',
'Bicycle Shops-Sales and Service',
'Hearing Aids-Sales, Service, Supply Stores',
'Music Stores-Musical Instruments, Pianos, Sheet Music',
'Construction Materials-not elsewhere classified',
'Accessory and Apparel Stores-Miscellaneous',
'SECOND HAND STORES-USED MERCHANDISE STORES',
'Fuel Dealers-Coal, Fuel Oil, Liquefied Petroleum, Wood',
'Furniture and Home Furnishing store',
'Online Food Ordering',
'Restaurants',
'Food Courts/Corporate Cafetaria',
'Catering Services',
'Alcoholic Beverages',
'Restaurant search and reservations',
'Dairy Products Stores',
'Bakeries',
'SaaS (Software as a service)',
'Platform as a service',
'Infrastructure as a service',
'Consulting and Outsourcing',
'Web designing, development and hosting',
'Technical Support',
'Data processing',
'Game developer and publisher',
'E-sports',
'Online Casino',
'Fantasy Sports',
'Game distributor/Marketplace',
'Video on demand',
'Music streaming services',
'Multiplexes',
'Content and Publishing',
'Events and movie ticketing',
'News',
'Video Game Arcades/Establishments',
'Motion Pictures and Video Tape Production and Distribution',
'Bowling Alleys',
'Billiard and Pool Establishments',
'Amusement Parks, Carnivals, Circuses, Fortune Tellers',
'Theatrical Producers-except Motion Pictures, Ticket Agencies',
'Repair and cleaning services',
'Interior Designing and Architect',
'Movers and Packers',
'Legal Services',
'Event planning services',
'Service Centre',
'Consulting Services',
'Ad and marketing agencies',
'Services Classifieds',
'Multi-level Marketing',
'GENERAL CONTRACTORS',
'Horticultural and Landscaping Services',
'CAR WASHES',
'A MOTOR HOME AND RECREATIONAL',
'Stenographic and Secretarial Support Services',
'Chiropractors',
'Automotive Service Shops',
'Hat Cleaning Shops, Shoe Repair Shops, Shoe Shine Parlors',
'Telecom Servc including but not ltd to prepaid phone serv',
'FINES',
'Agencies ‚Äì Security Services',
'Typesetting, Plate Making, Related Services',
'Appliance Repair Shops, Electrical and Small',
'Photo Developing, Photofinishing Laboratories',
'Dry Cleaners',
'Electronic Repair Shops',
'Specialty Cleaning, Polishing, and Sanitation Preparations',
'Nursing and Personal Care Facilities',
'Direct Marketing Other Direct Marketers not elsewhere classified',
'Veterinary Services',
'AFFILIATED AUTO RENTAL',
'COURT COST INCLUDING ALIMONY AND CHILD SUPPORT',
'AIRPORT FLYING FIELDS',
'Tire Retreading and Repair Shops',
'Cable and other Pay Television Services',
'Recreational and Sporting Camps',
'AGRICLUTRAL COPERATIVES',
'Carpentry Contractors',
'Wrecking and Salvage Yards',
'Towing Services',
'Barber and Beauty Shops',
'Video Tape Rental Stores',
'Golf Courses, Public',
'Miscellaneous Repair Shops and Related Services',
'MOTOR HOME DEALERS',
'Debt, Marriage, Personal Counseling Service',
'Air Conditioning and Refrigeration Repair Shops',
'Alterations, Mending, Seamstresses, Tailors',
'Massage Parlors',
'Government Licensed Horse/Dog Racing',
'Consumer Credit Reporting Agencies',
'Air Conditioning, Heating, and Plumbing Contractors',
'ELECTRICAL CONTRACTORS',
'Carpet and Upholstery Cleaning',
'Roofing and Siding, Sheet Metal Work Contractors',
'Computer Network/Information Services',
'Cleaning, Garment, and Laundry Services',
'Trailer Parks and Campgrounds',
'Insulation, Masonry, Plastering, Stonework, and Tile Setting Contractors',
'Exterminating and Disinfecting Services',
'Ambulance Services',
'Funeral Services and Crematories',
'Metal Service Centers and Offices',
'Quick Copy, Reproduction, and Blueprinting Services',
'Fuel Dispenser, Automated',
'Government Owned Lottery,Government Owned Lottery',
'WELDING REPAIR',
'Mobile Home Dealers',
'CONCRETE WORK CONTRACTORS',
'Boat Leases and Boat Rentals',
'Buying/Shopping Clubs, Services',
'DOOR-TO-DOOR SALES',
'Direct Marketing-Travel-Related Arrangement Services',
'Betting -including Lottery Tickets, Chips at Gaming Casinos, Off-Track Betting and Wagers at Race Tracks',
'Bands, Orchestras, and Miscellaneous Entertainers-not elsewhere classified',
'Furniture-Reupholstery and Repair, Refinishing',
'Direct Marketing-Continuity/Subscription Merchants',
'Typewriter Stores-Sales, Service, and Rentals',
'Direct Marketing-Insurance Services',
'Business Services-not elsewhere classified',
'Direct Marketing-Inbound Telemarketing Merchants',
'Recreation Services-not elsewhere classified',
'Swimming Pools-Sales and Supplies',
'Direct Marketing-Outbound Telemarketing Merchants',
'Public Warehousing-Farm Products, Refrigerated Goods,',
'Clothing Rental-Costumes, Uniforms, and Formal Wear',
'Contractors, Special Trade-not elsewhere classified',
'Transportation Services-not elsewhere classified',
'Electric Razor Stores-Sales and Service',
'Service Stations with or without Ancillary Services',
'Photographic studios',
'Professional services',
'Developer',
'Facility Management Company',
'RWA',
'Co-working spaces',
'Real estate classifieds',
'Home or office rentals',
'Charity',
'Educational',
'Religious',
'Personal',
'Dating and Matrimony platforms',
'Social Network',
'Messaging and Communication',
'Professional Network',
'Local/Neighbourhood network',
'Automobile Associations',
'Political Organizations',
'Clubs-Country Clubs, Membership-Athletic, Recreation, Sports, Private Golf Courses',
'Associations and membership',

    ]



















































































































































































































































































































































































































































































































































































































































































,
    'category': [6211,
6012,
6051,
6300,
6012,
6012,
6012,
6010,
6211,
6211,
8931,
8931,
6050,
6211,
7801,
7361,
6538,
4829,
7276,
9311,
5818,
6011,
8220,
8211,
8220,
8299,
8299,
8351,
8299,
8299,
8249,
7941,
7911,
8241,
5912,
8062,
8062,
8071,
7298,
7298,
7298,
5499,
5399,
8031,
5047,
5122,
8049,
8021,
5251,
8042,
5976,
8099,
8734,
8011,
4900,
4900,
4814,
4900,
4899,
4899,
4899,
4816,
4814,
9399,
9399,
9405,
9402,
4214,
4215,
4225,
4214,
4214,
4215,
4511,
7011,
4722,
4722,
7991,
7998,
7012,
4121,
4131,
4112,
7512,
4411,
7523,
4784,
4011,
7513,
4111,
5399,
5193,
5942,
5732,
7311,
7394,
5691,
5193,
5411,
5945,
5111,
5300,
5973,
5995,
5941,
5971,
5999,
5399,
5999,
5993,
5999,
5972,
5013,
5044,
5261,
5722,
5199,
5065,
5698,
5947,
5309,
5021,
5131,
5942,
5946,
5422,
5948,
5598,
5611,
5198,
5533,
5094,
5531,
5998,
5172,
5311,
5661,
5532,
5655,
5169,
5718,
5046,
5651,
5949,
5561,
5735,
5200,
5944,
5970,
5933,
5943,
8043,
7631,
5139,
5937,
5074,
5331,
5921,
5551,
5977,
5719,
4812,
5621,
5992,
7333,
5211,
5441,
5231,
7993,
5714,
5137,
7535,
5099,
5681,
5085,
5571,
5641,
5734,
5631,
5192,
5713,
5950,
5072,
5310,
5045,
5521,
5599,
5932,
5940,
5975,
5733,
5039,
5699,
5931,
5983,
5712,
5811,
5812,
5814,
5811,
5813,
7299,
5451,
5462,
5817,
5817,
5817,
7392,
7372,
7379,
7372,
5816,
5816,
7801,
5816,
5816,
5815,
5815,
7832,
2741,
7832,
5994,
7994,
7829,
7933,
7932,
7996,
7922,
7531,
8911,
4214,
8111,
8999,
5511,
7392,
7311,
7311,
5964,
1520,
780,
7542,
7519,
7339,
8041,
7538,
7251,
4813,
9222,
7393,
2791,
7629,
7395,
7216,
7622,
2842,
8050,
5969,
742,
3351,
9211,
4582,
7534,
4899,
7032,
763,
1750,
5935,
7549,
7230,
7841,
7992,
7699,
5592,
7277,
7623,
5697,
7297,
7802,
7321,
1711,
1731,
7217,
1761,
4816,
7210,
7033,
1740,
7342,
4119,
7261,
5051,
7338,
5542,
7800,
7692,
5271,
1771,
4457,
7278,
5963,
5962,
7995,
7929,
7641,
5968,
5978,
5960,
7399,
5967,
7999,
5996,
5966,
4225,
7296,
1799,
4789,
5997,
5541,
7221,
8999,
6513,
7349,
7349,
6513,
6513,
6513,
8398,
8398,
8661,
8398,
7273,
8641,
4821,
8699,
8699,
8675,
8651,
7997,
8699,],
    
}

mcc_list = pd.DataFrame(data)

# COMMAND ----------

mcc_list

# COMMAND ----------

device_base_df[device_base_df['device_id']=='1.0009eaca2db2f16036a236fbc7bdbb0fb417fb5f']

# COMMAND ----------

summary_part2_df.to_csv('/dbfs/FileStore/device2_df.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/device2_df.csv"

# COMMAND ----------

summary_part2_df.to_csv('/dbfs/FileStore/device_df.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/device_df.csv"

# COMMAND ----------

total_users = summary_part2_df.groupby(by=['new_or_repeating_user']).agg({'device_id':'nunique'}).reset_index()
total_users['col_percent'] = total_users['device_id'] / total_users['device_id'].sum()
total_users

# COMMAND ----------

summary_part2_df['device_id'].nunique()

# COMMAND ----------



# COMMAND ----------

date_level_temp = summary_part2_df.groupby(by=['producer_created_date','new_or_repeating_user']).agg({'device_id':'nunique','checkout_id':'nunique'}).reset_index()
date_level  = date_level_temp.pivot(index=['producer_created_date'], columns=['new_or_repeating_user'], values=['device_id','checkout_id',]).reset_index()
date_level = date_level.fillna(0)
date_level.columns = date_level.columns.map('_'.join)
date_level['total_device'] = date_level['device_id_new'] + date_level['device_id_repeating']
date_level['device_repeat_rate'] = date_level['device_id_repeating'] / date_level['total_device'] 
date_level['total_checkout_id'] = date_level['checkout_id_new'] + date_level['checkout_id_repeating']
date_level['checkout_repeat_rate'] = date_level['checkout_id_repeating'] / date_level['total_checkout_id'] 
date_level

# COMMAND ----------

mid_level_temp = summary_part2_df.groupby(by=['merchant_id','new_or_repeating_user']).agg({'device_id':'nunique','checkout_id':'nunique','producer_created_date':'nunique',}).reset_index()
mid_level = mid_level_temp.pivot(index=['merchant_id'], columns=['new_or_repeating_user'], values=['device_id','checkout_id','producer_created_date']).reset_index()
mid_level = mid_level.fillna(0)
mid_level.columns = mid_level.columns.map('_'.join)
mid_level['total_device'] = mid_level['device_id_new'] + mid_level['device_id_repeating']
mid_level['device_repeat_rate'] = mid_level['device_id_repeating'] / mid_level['total_device'] 
mid_level['total_checkout_id'] = mid_level['checkout_id_new'] + mid_level['checkout_id_repeating']
mid_level['checkout_repeat_rate'] = mid_level['checkout_id_repeating'] / mid_level['total_checkout_id'] 
mid_level.sort_values(by='total_device', ascending=False)

# COMMAND ----------

mid_level.sort_values(by='total_device', ascending=False)

# COMMAND ----------

plt.hist(mid_level['device_repeat_rate'], bins=25, color='blue', edgecolor='black')

# Add labels and a title
plt.xlabel('Value')
plt.ylabel('Frequency')
plt.title('Histogram Example')

# Show the histogram
plt.show()

# COMMAND ----------

device_to_checkout_temp = summary_part2_df.groupby(by=['device_id','producer_created_date']).agg({'checkout_id':'nunique'}).reset_index()
device_to_checkout = device_to_checkout_temp.groupby(by='checkout_id').agg({'device_id':'nunique'}).reset_index()
device_to_checkout['col_percentage'] = device_to_checkout['device_id']*1.00 / device_to_checkout['device_id'].sum()
device_to_checkout

# COMMAND ----------

device_exclusion_list = device_to_checkout_temp[device_to_checkout_temp['checkout_id'] > 20]['device_id']
device_exclusion_list

# COMMAND ----------

device_to_checkout.to_csv('/dbfs/FileStore/device_to_checkout.csv', index=False)
"https://razorpay-dev.cloud.databricks.com/files/device_to_checkout.csv"

# COMMAND ----------



pd.pivot_table(summary_part2_df, values='producer_created_date', index='device_id', columns='new_or_repeating_user', aggfunc='pd.Series.nunique')

# COMMAND ----------

percentiles = [5, 10, 25, 50, 75, 90, 95]
mid_level['device_repeat_rate'].quantile([p / 100 for p in percentiles])


# COMMAND ----------

#note: for excluded list check the break between ios + android 

# COMMAND ----------

summary_final.columns

# COMMAND ----------


