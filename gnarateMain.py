#!/bin/python3.10

import config
from config import *

# Database connection
def read_data_from_db():
    connection = psycopg2.connect(
        host= HOST_NAME,
        database=DB_NAME,
        user=USER_NAME,
        password=PWD
    )

    cursor = connection.cursor(cursor_factory=RealDictCursor)

    cursor.execute("SELECT * FROM samp.test.users")
    users_data = cursor.fetchall()

    cursor.execute("SELECT * FROM samp.test.ips")
    ips_data = cursor.fetchall()

    cursor.execute("SELECT * FROM samp.public.status_codes")
    status_codes_data = cursor.fetchall()

    cursor.execute("SELECT * FROM samp.test.getway")
    getways_data = cursor.fetchall()

    cursor.execute("SELECT * FROM test.endpoint")
    endpoints_data = cursor.fetchall()

    cursor.execute("SELECT * FROM samp.test.packages")
    packages_data = cursor.fetchall()

    cursor.execute("SELECT * FROM samp.test.monz_user_packages")
    monz_user_packages_data = cursor.fetchall()

    cursor.close()
    connection.close()
#select all data
    username_to_userId = {user['username']: user['userid'] for user in users_data}
    username_to_src = {user['username']: user['src'] for user in users_data if user['src']}
    username_to_ip = {ip['username']: ip['ip'] for ip in ips_data}
    status_codes = [status['code'] for status in status_codes_data]
    getway_to_endpoints = {}
    endid_to_method = {endpoint['endid']: endpoint['method'] for endpoint in endpoints_data}
    endid_to_name = {endpoint['endid']: endpoint['endname'] for endpoint in endpoints_data}
    apiId_to_provider = {
        getway['apiid']: {
            'providerId': getway['providerid'],
            'providerName': getway['providername'],
            'apiName': getway['apiname']
        } for getway in getways_data
    }

    for endpoint in endpoints_data:
        apiId = endpoint['apiid']
        if apiId not in getway_to_endpoints:
            getway_to_endpoints[apiId] = []
        getway_to_endpoints[apiId].append(endpoint['endid'])
    
    package_to_details = {}
    for package in packages_data:
        packagId = package['packagid']
        if packagId not in package_to_details:
            package_to_details[packagId] = []
        package_to_details[packagId].append({
            'apiId': package['apiid'],
            'endid': package['endid'],
            'planid': package['planid'],
            'price': package['price'],
            'paymenttype': package['paymenttype']
        })

    username_to_packages = {}
    for user_package in monz_user_packages_data:
        username = user_package['username']
        if username not in username_to_packages:
            username_to_packages[username] = []
        username_to_packages[username].append({
            'packagid': user_package['packagid'],
            'planid': user_package['planid'],
            'pid': user_package['pid']
        })

    return username_to_ip, username_to_userId, username_to_src, status_codes, getway_to_endpoints, endid_to_method, endid_to_name, apiId_to_provider, package_to_details, username_to_packages
# Gregorian to Jalali
def gregorian_to_jalali(g_date):
    j_date = jdatetime.date.fromgregorian(date=g_date)
    return f"{j_date.year}", f"{j_date.month:02d}", f"{j_date.day:02d}"


def Random_date (start_date, end_date):
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    delta = end_dt - start_dt
    int_delta = delta.days
    random_day = random.randint(0, int_delta)
    random_date = start_dt + timedelta(days=random_day)
    return random_date
    # int_delta = int(delta.total_seconds())
    # random_second = random.randrange(int_delta)
    # return start_dt + timedelta(seconds=random_second)

# def random_time(start_time, end_time):
#     start_t = datetime.strptime(start_time, "%H:%M:%S").time()
#     end_t = datetime.strptime(end_time, "%H:%M:%S").time()
#     start_seconds = start_t.hour * 3600 + start_t.minute * 60 + start_t.second
#     end_seconds = end_t.hour * 3600 + end_t.minute * 60 + end_t.second
#     random_seconds = random.randint(start_seconds, end_seconds)
#     random_time_obj = (datetime.min + timedelta(seconds=random_seconds)).time()
#     return random_time_obj

def generate_logs(count_lines, start_date, end_date, db_data):
    (username_to_ip, username_to_userId, username_to_src, status_codes,
     getway_to_endpoints, endid_to_method, endid_to_name, apiId_to_provider,
     package_to_details, username_to_packages) = db_data

    logs = []
    # log_date = Random_date(start_date,end_date)

    for _ in range(count_lines):
        username = random.choice(list(username_to_userId.keys()))

        # Assign unique IP to any username
        if username not in username_to_ip:
            raise ValueError(f"Username: {username} does not have an associated IP in the database.")


        proxyIp = random.choice(config.proxyIp)
        className = random.choice(config.className)
        threadName = random.choice(config.threadName)
        action = random.choice(config.actions)
        host = random.choice(config.host)
        appName = random.choices(["getway", "Samp.V3.Getway"],
                                  weights=[90, 10])[0]
        
        # logLevel = random.choices(["INFO", "ERROR"],
        #                            weights=[99, 1])[0] if appName == "getway" else "ERROR"
        logLevel = "INFO"
        
        logMarker = random.choices(["API", "CALL", "API_TEST"],
                                    weights=[85, 13, 2])[0]
        
        ip = username_to_ip[username]   
        userId = username_to_userId[username]        

        logTimestamp = Random_date(start_date, end_date) + timedelta(hours=random.randint(0, 24),minutes=random.randint(0, 59),seconds=random.randint(0, 59))
        jy, jm, jd = gregorian_to_jalali(logTimestamp)
        logTimestamp = logTimestamp.strftime("%Y-%m-%d %H:%M:%S")
        # if logLevel == "ERROR":
        #     jy, jm, jd = '', '', ''
        # else:
        #     jy, jm, jd = gregorian_to_jalali(logTimestamp)

        requestId = uuid.uuid4().hex

        apiType = random.choices(["GATEWAY", "PROXY"], weights=[70, 30])[0]

        callType = "EXTERNAL"
        gatewayType = "REST"
        client = actionType = logType = target = apiName = productid = paymentType= \
        flowId= flowEvent= url= userAgent= actionDetails= billingStatus= inputId =\
        outputId = inputs = outputs = msg = apiId = httpMethod = price = providerId =\
        providerName = planid =paymenttype = packageid = pid = endpointName = endpointId = ''
        
        src = ''
        if username in username_to_src:
            src = random.choice(username_to_src[username])
            


# check any username in monz_user_packages
        if username in username_to_packages:
            user_packages = username_to_packages[username]
            selected_package = random.choice(user_packages)
            packageid = selected_package.get('packagid', '')
            planid = selected_package.get('planid', '')
            pid = selected_package.get('pid', '')
    
            package_details = package_to_details.get(packageid, [])
            if package_details:
                package_detail = package_details[0]
                apiId = package_detail.get('apiId', '')
                endpointId = package_detail.get('endid', '')
                
                price = package_detail.get('price', '')
                paymenttype = package_detail.get('paymenttype', '')
                httpMethod = endid_to_method.get(endpointId, '')
                endpointName = endid_to_name.get(endpointId, '')

                provider_data = apiId_to_provider.get(apiId, {})
                providerId = provider_data.get('providerId', '')
                providerName = provider_data.get('providerName', '')
                apiName = provider_data.get('apiName', '')

        code = random.choice(status_codes)
        # Check ApiManagement status code
        if 200 <= code < 300:
            gwCode = random.choice([200, 201, 202, 400, 401, 404, 500, 502, 503, 504, 505, 506, 507, 508, 509, 300, 301, 302])
            status = responseStatus = 1 if gwCode in range(500,600) else 0 
            gwStatus = 1 if 200 <= gwCode < 300 else 0
            providerResponseTime = random.randint(1, 100)
        else:
            status = responseStatus = 0
            gwCode = gwStatus = providerResponseTime = ''

        responseTime = random.randint(50, 200)
        if providerResponseTime != '':
            responseTime = max(responseTime, providerResponseTime + random.randint(1, 50))
        
        billingStatus = "0" if paymenttype == 'Repayment' else "1"

        log_entry = f"{logTimestamp}|{jy}|{jm}|{jd}|{logLevel}|{appName}|{logMarker}|{host}|{ip}|"\
        f"{proxyIp}|{className}|{threadName}|{requestId}|{logType}|{apiType}|{callType}|{client}|"\
        f"{username}|{userId}|{action}|{actionType}|{src}|{target}|{responseStatus}|{responseTime}|"\
        f"{status}|{code}|{providerResponseTime}|{gwStatus}|{gwCode}|{gatewayType}|{apiId}|{endpointId}|"\
        f"{apiName}|{endpointName}|{productid}|{pid}|{packageid}|{planid}|{providerId}|{providerName}|"\
        f"{price}|{paymentType}|{flowId}|{flowEvent}|{httpMethod}|{url}|{userAgent}|{actionDetails}|{billingStatus}|" \
        f"{inputId}|{outputId}|{inputs}|{outputs}|{msg}"
        logs.append(log_entry)


    output_file = "fake_logs.txt"
    with open(output_file, "w") as f:
        f.write("\n".join(logs))

    print(f"Data saved to {output_file}")
    return logs

# count_lines_to_generate = int(input("ENTER NUMBER OF LOG: "))
# logs = generate_logs(count_lines_to_generate)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-count_lines", type=int, required=True, help="Number of logs to generate")
    parser.add_argument("-start_date", type=str, required=True, help="Start date in YYYY-MM-DD format")
    parser.add_argument("-end_date", type=str, required=True, help="End date in YYYY-MM-DD format")
    args = parser.parse_args()
    db_data = read_data_from_db()
    logs = generate_logs(args.count_lines, args.start_date, args.end_date, db_data)
