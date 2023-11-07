from flask import Flask, request, jsonify
import requests
import json
import os
from datetime import timedelta
import time
import datetime
from collections import Counter
from collections import defaultdict

def load_data_from_file(filename):
    """Load JSON data from a file."""
    with open(filename, "r") as file:
        return json.load(file)

def extract_data_from_labels(data, labels, expected_unit):
    """Extract data from specified labels and return if matches the expected unit."""
    extracted_data = []
    for label in labels:
        label_data = data["facts"]["us-gaap"].get(label, {}).get("units", {})
        if expected_unit in label_data:
            extracted_data.extend(label_data[expected_unit])
    return extracted_data

def extract_and_sum_data_from_labels(data, labels, expected_unit):
    extracted_data = []
    years_found = set()
    for label in labels:
        # If we encounter a list, we sum the data from each label in the list
        if isinstance(label, list):
            summed_data = []
            for sub_label in label:
                sub_data = data["facts"]["us-gaap"].get(sub_label, {}).get("units", {})
                if expected_unit in sub_data:
                    if summed_data:
                        summed_data = [x + y for x, y in zip(summed_data, sub_data[expected_unit])]
                    else:
                        summed_data = sub_data[expected_unit]
            extracted_data.extend(summed_data)
        else:
            label_data = data["facts"]["us-gaap"].get(label, {}).get("units", {})
            datas = []
            if expected_unit in label_data:
                # for record in label_data[expected_unit]:
                #     year = record["end"][:4]
                #     print(year)
                #     if year not in years_found:
                #         datas.append(record)
                #         years_found.add(year)
                # print(datas)
                # extracted_data.extend(datas)
                extracted_data.extend(label_data[expected_unit])
    return extracted_data


def deduplicate_data(data):
    """Deduplicate data based on start and end dates and fiscal period."""
    unique_data = {}
    for record in data:
        if is_valid_period(record):
            if "start" not in record:
                date_obj = datetime.datetime.strptime(record["end"], "%Y-%m-%d")

                # Subtract one year
                new_date_obj = date_obj - timedelta(days=365)

                # Convert the new datetime object back to string format
                new_date_string = new_date_obj.strftime("%Y-%m-%d")
                record["start"] = new_date_string
            key = (record["start"], record["end"], record["fp"])
            if key not in unique_data:
                unique_data[key] = record
    return sorted(list(unique_data.values()), key=lambda x: x["start"])

def is_valid_period(record):
    if "start" not in record:
        return record["fp"] == "FY"
    """Check if record has a valid period for annual or quarterly data."""
    start_date = datetime.datetime.strptime(record["start"], "%Y-%m-%d")
    end_date = datetime.datetime.strptime(record["end"], "%Y-%m-%d")
    
    duration = (end_date - start_date).days + 1  # Including the start day
    
    if record["fp"] == "FY" and duration > 330:  # Note: This does not account for leap years
        return True
    if record["fp"] in ["Q1", "Q2", "Q3", "Q4"] and (duration >= 80 and duration <= 100):  # Assuming each quarter has 91 days
        return True
    return False

def get_fiscal_year_from_dates(start_date, end_date):
    # Count the days in each year
    days_counter = Counter()

    # Consider each date in the range
    current_date = start_date
    while current_date <= end_date:
        days_counter[current_date.year] += 1
        current_date += timedelta(days=1)

    # Find the year with the maximum number of days
    fiscal_year, _ = days_counter.most_common(1)[0]
    return fiscal_year

def get_quarter_from_dates(start_date, end_date):
    # Define the quarters
    quarters = {
        'Q1': ((1, 1), (3, 31)),
        'Q2': ((4, 1), (6, 30)),
        'Q3': ((7, 1), (9, 30)),
        'Q4': ((10, 1), (12, 31))
    }

    # Count the days in each quarter
    days_counter = Counter()

    # Consider each date in the range
    current_date = start_date
    while current_date <= end_date:
        # Find the quarter for the current date
        for quarter, ((start_month, start_day), (end_month, end_day)) in quarters.items():
            if current_date.month >= start_month and current_date.month <= end_month:
                if (current_date.month == start_month and current_date.day >= start_day) or \
                   (current_date.month == end_month and current_date.day <= end_day) or \
                   (current_date.month > start_month and current_date.month < end_month):
                    days_counter[quarter] += 1
        current_date += timedelta(days=1)

    # Find the quarter with the maximum number of days
    quarter, _ = days_counter.most_common(1)[0]
    return quarter

def estimate_missing_quarterly_value(data):
    """Estimate missing quarterly value for years with one missing quarter."""
    yearly_data = {}
    # Store the date ranges for each quarter
    date_ranges = {}

    # Populate the dictionary with existing data and collect date ranges
    for record in data:
        start_date = datetime.datetime.strptime(record['start'], "%Y-%m-%d")
        end_date = datetime.datetime.strptime(record['end'], "%Y-%m-%d")
        year = get_fiscal_year_from_dates(start_date, end_date)
        quarter = get_quarter_from_dates(start_date, end_date) if record['fp'] != 'FY' else 'FY'

        if year not in yearly_data:
            yearly_data[year] = {}
            date_ranges[year] = []

        yearly_data[year][quarter] = record['val']
        date_ranges[year].append((quarter, start_date, end_date))

    # Process each year to estimate missing quarterly value
    for year, quarters in yearly_data.items():
        if 'FY' in quarters and len(quarters) == 4:  # Full year and 3 quarters are present
            known_quarters_sum = sum(val for q, val in quarters.items() if q != 'FY')
            all_quarters = {'Q1', 'Q2', 'Q3', 'Q4'}
            missing_quarters = all_quarters - set(quarters.keys())
            if len(missing_quarters) == 1:
                missing_quarter = missing_quarters.pop()
                # Calculate the missing quarter's range
                missing_start_date, missing_end_date = get_missing_date_range(year, date_ranges[year], missing_quarter)
                missing_value = quarters['FY'] - known_quarters_sum
                data.append({
                    'start': missing_start_date.strftime("%Y-%m-%d"),
                    'end': missing_end_date.strftime("%Y-%m-%d"),
                    'val': missing_value,
                    'fp': missing_quarter
                })

    return data

def get_missing_date_range(year, quarter_ranges, missing_quarter):
    # Sort the ranges by start date
    quarter_ranges.sort(key=lambda x: x[1])
    
    # Assuming quarters are in chronological order: Q1, Q2, Q3, Q4
    # Find the gap in the existing ranges
    prev_end_date = None
    for qr in quarter_ranges:
        q, start_date, end_date = qr
        if prev_end_date and start_date - prev_end_date > timedelta(days=1):
            # Found a gap, this must be the missing quarter range
            return prev_end_date + timedelta(days=1), start_date - timedelta(days=1)
        prev_end_date = end_date
    
    # If the missing quarter is the last one, determine the end date based on the fiscal year end
    fy_end_date = max(end_date for _, _, end_date in quarter_ranges if _ == 'FY')
    return prev_end_date + timedelta(days=1), fy_end_date

def get_next_day(date_str):
    """ Helper function to get the next day given a date string """
    date_format = "%Y-%m-%d"
    date_obj = datetime.datetime.strptime(date_str, date_format)
    next_day = date_obj + datetime.timedelta(days=1)
    return next_day.strftime(date_format)

def get_quarter_dates(year, quarter):
    """ Helper function to get the start and end dates of a quarter """
    q_start_month = (int(quarter[-1]) - 1) * 3 + 1
    start_date = datetime.date(year, q_start_month, 1)
    end_month = start_date.month + 2
    end_year = start_date.year
    if end_month > 12:
        end_month -= 12
        end_year += 1
    end_date = datetime.date(end_year, end_month,
                             datetime.date(end_year, end_month + 1, 1).day - 1)
    return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")

def append_missing_quarters(json_data):
    # Group records by fiscal year
    fiscal_years = defaultdict(lambda: {'quarters': [], 'FY': None})

    # Process each revenue record
    for record in json_data:
        fy = record['fy']
        fp = record['fp']
        if fp == 'FY':
            fiscal_years[fy]['FY'] = record
        else:
            fiscal_years[fy]['quarters'].append(record)

    # Append missing quarters to the original data
    # appended_data = json_data['Revenues'].copy()  # Create a copy to modify
    for fy, data in fiscal_years.items():
        if data['FY']:  # There is an annual record
            total_annual = data['FY']['val']
            quarters = sorted(data['quarters'], key=lambda x: x['start'])
            total_quarters = sum(q['val'] for q in quarters)

            #print(quarters)
            if total_annual != total_quarters:
                missing_value = total_annual - total_quarters

                # Find the missing quarter(s) by looking for gaps in the sequence
                for i, quarter in enumerate(quarters):
                    # If the next quarter is not consecutive, we found a gap
                    if i + 1 < len(quarters) and quarter['end'] != get_next_day(quarters[i + 1]['start']):
                        missing_start_date = get_next_day(quarter['end'])
                        missing_end_date = quarters[i + 1]['start']
                        break
                else:  # No gaps found, the missing quarter is at the end
                    last_quarter_end = quarters[-1]['end'] if quarters else data['FY']['start']
                    missing_start_date = get_next_day(last_quarter_end)
                    missing_end_date = data['FY']['end']

                #print(missing_start_date, missing_end_date)

                # Create the missing quarter record
                missing_record = {
                    'start': missing_start_date,
                    'end': missing_end_date,
                    'val': missing_value,
                    'accn': data['FY']['accn'],
                    'fy': fy,
                    'fp': 'QX',  # Placeholder for the missing quarter designation
                    'form': '10-Q',
                    'filed': data['FY']['filed'],
                    'frame': f'CY{fy}QX'  # Placeholder
                }
                json_data.append(missing_record)

    return json_data


# Function to ensure that a date is a datetime object for processing
def parse_date(date_str):
    return datetime.datetime.strptime(date_str, '%Y-%m-%d')

# Function to add missing quarterly data to the list of records
def add_missing_quarter_data(revenues):
    # Separate full year and quarter data
    full_years = [r for r in revenues if r['fp'] == 'FY']
    quarters = [r for r in revenues if 'Q' in r['fp']]

    # Process each full year to find and add missing quarters
    for fy in full_years:
        fy_start = parse_date(fy['start'])
        fy_end = parse_date(fy['end'])

        # Sort quarters within the fiscal year by their start date
        fy_quarters = [q for q in quarters if parse_date(q['start']) >= fy_start and parse_date(q['end']) <= fy_end]
        fy_quarters.sort(key=lambda q: q['start'])

        if len(fy_quarters) != 3:
            continue
        # Calculate the covered range by quarters
        covered_range = []
        for q in fy_quarters:
            q_start = parse_date(q['start'])
            q_end = parse_date(q['end'])
            covered_range.append((q_start, q_end))

        # Find the gap in the covered range for the missing quarter
        prev_end = fy_start
        for start, end in covered_range:
            if start != prev_end:
                # This gap is where the missing quarter lies
                missing_start = prev_end
                missing_end = start - timedelta(days=1)
                break
            prev_end = end + timedelta(days=1)
        else:
            # If no gap found, the missing quarter is after the last known quarter
            missing_start = prev_end
            missing_end = fy_end

        # Calculate the missing quarter's revenue
        known_revenue = sum(q['val'] for q in fy_quarters)
        missing_revenue = fy['val'] - known_revenue

        # Add the missing quarter record
        revenues.append({
            'start': missing_start.strftime('%Y-%m-%d'),
            'end': missing_end.strftime('%Y-%m-%d'),
            'val': missing_revenue,
            'accn': fy['accn'],
            'fy': fy['fy'],
            'fp': 'QX',  # Placeholder for 'unknown' quarter
            'form': fy['form'],
            'filed': fy['filed']
        })

    # Return the updated list, including the newly added missing quarters
    return revenues


# def estimate_missing_quarterly_value(data):
#     """Estimate missing quarterly value for years with one missing quarter."""
#     yearly_data = {}
#     # Populate the dictionary with existing data
#     for record in data:
#         start_date = datetime.strptime(record['start'], "%Y-%m-%d")
#         end_date = datetime.strptime(record['end'], "%Y-%m-%d")
#         #year = end_date.year
#         # Use the get_fiscal_year_from_dates function to determine the fiscal year
#         year = get_fiscal_year_from_dates(start_date, end_date)
#         quarter = get_quarter_from_dates(start_date, end_date)
#         if record['fp'] == 'FY':
#             quarter = 'FY'
#         print(start_date, end_date, quarter)
        
#         if year not in yearly_data:
#             yearly_data[year] = {}
#         yearly_data[year][quarter] = record['val']
#     print(yearly_data)
#     # Estimate the missing quarterly value
#     for year, values in yearly_data.items():
#         if 'FY' in values and len(values) == 4:  # Full year and 3 quarters are present
#             known_quarters_sum = sum(val for quarter, val in values.items() if quarter != 'FY')
#             all_quarters = {'Q1', 'Q2', 'Q3', 'Q4'}
#             missing_quarters = all_quarters - set(values.keys())
#             print(known_quarters_sum, missing_quarters)
#             if len(missing_quarters) == 1:
#                 missing_quarter = missing_quarters.pop()
#                 missing_value = values['FY'] - known_quarters_sum
#                 print(missing_quarter, missing_value)
#                 data.append({
#                     'start': f"{year}-01-01" if missing_quarter == 'Q1' else f"{year}-04-01" if missing_quarter == 'Q2' else f"{year}-07-01" if missing_quarter == 'Q3' else f"{year}-10-01",
#                     'end': f"{year}-03-31" if missing_quarter == 'Q1' else f"{year}-06-30" if missing_quarter == 'Q2' else f"{year}-09-30" if missing_quarter == 'Q3' else f"{year}-12-31",
#                     'val': missing_value,
#                     'fp': missing_quarter
#                 })
#     return data

# def estimate_missing_quarterly_value(data):
#     """Estimate missing quarterly value for years with one missing quarter."""
#     yearly_data = {}
#     for record in data:
#         year = record['start'].split('-')[0]
#         if year not in yearly_data:
#             yearly_data[year] = {}
#         yearly_data[year][record['fp']] = record['val']

#     for year, values in yearly_data.items():
#         if 'FY' in values and len(values) == 4:  # We have full year and 3 quarters
#             known_quarters_sum = sum(val for quarter, val in values.items() if quarter != 'FY')
#             missing_quarter = set(['Q1', 'Q2', 'Q3', 'Q4']) - set(values.keys())
#             missing_quarter = list(missing_quarter)[0]
#             missing_value = values['FY'] - known_quarters_sum
#             data.append({
#                 'start': f"{year}-01-01" if missing_quarter == 'Q1' else f"{year}-04-01" if missing_quarter == 'Q2' else f"{year}-07-01" if missing_quarter == 'Q3' else f"{year}-10-01",
#                 'end': f"{year}-03-31" if missing_quarter == 'Q1' else f"{year}-06-30" if missing_quarter == 'Q2' else f"{year}-09-30" if missing_quarter == 'Q3' else f"{year}-12-31",
#                 'val': missing_value,
#                 'fp': missing_quarter
#             })
#     return data

def get_quarter(start_date, end_date):
    """Get the quarter based on the majority of days between start and end dates."""
    quarters = {
        'Q1': ((1, 1), (3, 31)),
        'Q2': ((4, 1), (6, 30)),
        'Q3': ((7, 1), (9, 30)),
        'Q4': ((10, 1), (12, 31))
    }
    
    days_in_quarter = {q: 0 for q in quarters}

    start_year = start_date.year
    end_year = end_date.year

    for year in range(start_year, end_year + 1):
        for q, ((start_month, start_day), (end_month, end_day)) in quarters.items():
            quarter_start = datetime.datetime(year, start_month, start_day)
            quarter_end = datetime.datetime(year, end_month, end_day)
            # Adjust the quarter start and end to the start and end dates if they fall within the quarter
            quarter_start = max(quarter_start, start_date)
            quarter_end = min(quarter_end, end_date)
            # Calculate the number of days in the quarter
            if quarter_start <= quarter_end:
                days_in_quarter[q] += (quarter_end - quarter_start).days + 1  # +1 to include the start date

    # Find the quarter with the maximum number of days
    majority_quarter = max(days_in_quarter, key=days_in_quarter.get)
    return majority_quarter

def format_data(data):
    """Format the annual data as 'YYYY' and quarterly data based on start and end dates."""
    formatted_data = {}
    for record in data:
        start_date = datetime.datetime.strptime(record['start'], "%Y-%m-%d")
        end_date = datetime.datetime.strptime(record['end'], "%Y-%m-%d")
        
        if record["fp"] == "FY":
            key = str(get_fiscal_year_from_dates(start_date, end_date))
        else:
            # quarter = get_quarter(start_date, end_date)
            # key = f"{end_date.year}-{quarter}"
            key = record['end']

        formatted_data[key] = record["val"]
    return formatted_data

# def format_data(data):
#     """Format the annual data as 'YYYY' and quarterly data as 'YYYY-QQ'."""
#     formatted_data = {}
#     for record in data:
#         year = record["end"].split("-")[0]
#         if record["fp"] == "FY":
#             key = year
#         else:
#             key = f"{year}-{record['fp']}"
#         formatted_data[key] = record["val"]
#     return formatted_data

app = Flask(__name__)

CACHE_DIR = "cache"

def cache_data(local_path, data):
    directory = os.path.dirname(local_path)
    if not os.path.exists(directory):
        os.makedirs(directory)
    
    with open(local_path, "w") as file:
        json.dump({
            'timestamp': time.time(),
            'data': data
        }, file)

CACHE_DURATION = 3600  # e.g., cache is valid for 1 hour

def load_cached_data(local_path):
    try:
        with open(local_path, "r") as file:
            cached_content = json.load(file)
            cached_time = cached_content['timestamp']
            if (time.time() - cached_time) <= CACHE_DURATION:
                return cached_content['data']
    except FileNotFoundError:
        pass
    return None  # Return None if the cache is invalid or the file does not exist



def ticker_to_cik(ticker):
    """Convert ticker symbol to CIK using local mapping."""
    with open("ticker_cik_mapping.json", "r") as file:
        mapping = json.load(file)
    
    for key, value in mapping.items():
        if value['ticker'] == ticker.upper():
            return str(value['cik_str']).zfill(10)  # Convert the CIK to a string, since in your mapping it's an integer

    return None  # Return None if the ticker is not found

def get_data_from_cik(cik):
    """Retrieve data for a given CIK either from the cache or from the remote server."""
    local_path = os.path.join(CACHE_DIR, f"{cik}.json")
    
    # Check cache first
    cached_data = load_cached_data(local_path)
    if cached_data:
        return cached_data
    
    # If not in cache, fetch from remote and store in cache
    data_url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    response = requests.get(data_url, headers=headers)

    # Check if the response status code is not 200 OK
    if response.status_code != 200:
        print(f"Error: Received status code {response.status_code}")
        print(response.text)  # Log the raw response to see what's returned
        return None

    try:
        data = response.json()
    except json.JSONDecodeError:
        print("Error decoding JSON from response.")
        return None


    # Cache the data
    cache_data(local_path, data)
    
    return data

@app.route('/process_annual', methods=['GET'])
def process_annual():
    results = process_data(request.args.get('ticker', default='', type=str))
    return jsonify(filter_annual_quarterly(results, "annual"))

@app.route('/process_quarterly', methods=['GET'])
def process_quarterly():
    results = process_data(request.args.get('ticker', default='', type=str))
    return jsonify(filter_annual_quarterly(results, "quarterly"))

def filter_annual_quarterly(data, mode):
    filtered_data = {}
    for label, periods in data.items():
        if not isinstance(periods, dict):  # to handle potential non-dict entries
            continue
        if mode == "annual":
            filtered_data[label] = {key: value for key, value in periods.items() if '-' not in key}
        elif mode == "quarterly":
            filtered_data[label] = {key: value for key, value in periods.items() if '-' in key}
    return filtered_data


def process_data(ticker):
    if not ticker:
        return {'error': 'No ticker symbol provided'}

    cik = ticker_to_cik(ticker)
    if not cik:
        return {'error': 'Unable to find CIK for the provided ticker'}

    data = get_data_from_cik(cik)

    labels_sets = {
        'Revenues': (["Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax", "SalesRevenueNet", 
                    "OtherSalesRevenueNet", "RevenueFromContractWithCustomerIncludingAssessedTax", "SalesRevenueGoodsNet"], "USD"),
        'Cost': (["CostOfGoodsAndServicesSold", "CostOfGoodsSold", "CostOfRevenue", 
                 "CostOfGoodsAndServiceExcludingDepreciationDepletionAndAmortization", "CommunicationsAndInformationTechnology"], "USD"),
        'OpEx': (["OperatingExpenses", "CostsAndExpenses"],"USD"),
        'SGA': (["SellingGeneralAndAdministrativeExpense","GeneralAndAdministrativeExpense"],"USD"),
        'Marketing': (["MarketingExpense","SellingAndMarketingExpense"],"USD"),
        'Research': (["ResearchAndDevelopmentExpense"],"USD"),
        'OpIncome': (["OperatingIncomeLoss","IncomeLossFromContinuingOperations"],"USD"),
        'NonOpExpense': (["NonoperatingIncomeExpense","OtherNonoperatingIncomeExpense","InterestIncomeExpenseNonoperatingNet"],"USD"),
        'IncomeBeforeTax': (["IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest","IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethodInvestments"],"USD"),
        'IncomeTaxExpenseBenefit': (["IncomeTaxExpenseBenefit"],"USD"),
        'NetIncome': (["NetIncomeLoss", "ProfitLoss"],"USD"),
        'BasicSharesOut': (["WeightedAverageNumberOfSharesOutstandingBasic","CommonStockSharesOutstanding"],"shares"),
        'DilutedSharesOut': (["WeightedAverageNumberOfDilutedSharesOutstanding","CommonStockSharesOutstanding"],"shares"),
        'DividendPerShare': (["CommonStockDividendsPerShareDeclared","CommonStockDividendsPerShareCashPaid"],"USD/shares"),
        'SplitCoef': (["StockholdersEquityNoteStockSplitConversionRatio1"],"pure"),
        'EPS': (["EarningsPerShareBasic","IncomeLossFromContinuingOperationsPerBasicShare","IncomeLossFromContinuingOperationsPerBasicAndDilutedShare"],"USD/shares")
    }

    all_extracted_data = {}
    for label_name, (labels, expected_unit) in labels_sets.items():
        extracted_data = extract_and_sum_data_from_labels(data, labels, expected_unit)
        all_extracted_data[label_name] = extracted_data

    print(all_extracted_data)
    deduplicated_data = {label_name: deduplicate_data(values) for label_name, values in all_extracted_data.items()}
    #print(deduplicated_data)
    print({label_name: format_data(values) for label_name, values in deduplicated_data.items()})
    #completed_data = {label_name: estimate_missing_quarterly_value(values) for label_name, values in deduplicated_data.items()}
    completed_data = {label_name: add_missing_quarter_data(values) for label_name, values in deduplicated_data.items()}
    #print(completed_data)
    formatted_data = {label_name: format_data(values) for label_name, values in completed_data.items()}
    #print(formatted_data)
    #formatted_data = {label_name: format_data(values) for label_name, values in all_extracted_data.items()}

    return formatted_data


if __name__ == "__main__":
    # Check if the cache directory exists, if not, create it
    if not os.path.exists(CACHE_DIR):
        os.makedirs(CACHE_DIR)
    app.run(debug=True)
