__author__ = 'stephenlechner'

# This file contains all the functions for the tsr_calculator's backend 
# processing. It's still very much a work in progress.
# TODO: 
# 1. create a front-end to work with these back-end processing functions. 
#    a. add charting functionality
#    b. add data download (in csv) functionality)
# 2. add a data upload functionality so that the front-end can receive the
#    results and users can download it in csv.
# 3. host all this stuff on Heroku
# 4. finalize paths

import csv
import urllib2
import requests
import psycopg2
import datetime
import json


def get_yahoo_stock_data(ticker, s_date, e_date):
    """This function downloads yahoo's adjusted stock price history for a
    company with a given ticker between a given time period. It downloads
    the data in csv and returns it as a list of touples to be added to
    the database.
    """
    data_list = []
    startmonth = str(int(s_date[:s_date.find("/")]) - 1)

    url = ("http://real-chart.finance.yahoo.com/table.csv?s=" + ticker +
           "&amp;a=" + startmonth + "&amp;b=" +
           s_date[s_date.find("/")+1:s_date.find("/", 3)] + "&amp;c=" +
           s_date[-4:] + "&amp;d=" + e_date[:e_date.find("/")] + "&amp;e=" +
           e_date[e_date.find("/")+1:e_date.find("/", 3)] + "&amp;f=" +
           e_date[-4:] + "&amp;g=d&amp;ignore=.csv")

    data_file = urllib2.urlopen(url)
  
    data_reader = csv.reader(data_file)
    for dat in data_reader:
        data_list.append((dat[0],dat[6],))
    data_list = data_list[1:]
    
    return data_list


def db_connect():
    """Connects to the database
    """
    return psycopg2.connect("dbname=ytsr")


def add_co_data(name, data_list):
    """This function takes the data for a new company and adds it to the
    database. Then it adds a line to the view (top_view) of what company 
    data has been entered and for which time periods.
    name is the ticker, data_list is the adjusted stock price history from 
    yahoo.
    NB: data_list must be a list/tuple of 2-value-tuples
    """
    db = db_connect()

    c = db.cursor()

    name_lc = name.lower()

    draft_name = "table_%s" % (name_lc,)
    
    if table_exists(draft_name) is False:
        c.execute(
            "CREATE TABLE table_%s(id serial UNIQUE NOT NULL, date_val date UNIQUE, %s float);" 
            % (name_lc, name_lc,)
        )
        db.commit()

    que = "INSERT INTO %s(date_val, %s) " % (draft_name, name_lc,)
    que = que + "VALUES(%s, %s);"
    c.executemany(que, data_list)
    db.commit()

    c.execute("SELECT * FROM top_view WHERE ticker = %s;", (name_lc,))
    if c.fetchall() == []:
        c.execute(
            "INSERT INTO top_view(ticker, s_date, e_date) VALUES('%s', (SELECT MIN(date_val) FROM %s), (SELECT MAX(date_val) FROM %s));" %
            (name_lc,"table_" + name_lc, "table_" + name_lc,)
        )
        db.commit()

    db.close()


def drop_table(table_name):
    """drops a specified table
    """
    db = db_connect()
    c = db.cursor()
    c.execute("DROP TABLE %s;" % table_name)
    db.commit()


def table_exists(name):
    """checks to see if a table exists in the database or not. 
    """
    db = db_connect()

    c = db.cursor()

    c.execute("SELECT * FROM table_exists(%s)", (name,))
    val = c.fetchall()[0][0]
    db.close()
    if val is True:
        return True
    elif val is False:
        return False
    # function should never get to this, but in case it does, 
    # we should know. 
    else:
        print "Error: did not return true or false."
        print val


def get_max_date(table_name):
    """grabs the largest value in the date_val column from a given
    table.
    """
    db = db_connect()
    c = db.cursor()
    c.execute("SELECT MAX( date_val) FROM %s;" % table_name)
    max = c.fetchall()[0][0]
    db.close()
    return max


def default(obj):
    """Default JSON serializer."""
    """Grabbed this from Jay Taylor at
    http://stackoverflow.com/questions/11875770/how-to-overcome-datetime-datetime-not-json-serializable-in-python
    in order to make the json-ing of date objects work.
    """
    import calendar, datetime

    if isinstance(obj, datetime.datetime):
        if obj.utcoffset() is not None:
            obj = obj - obj.utcoffset()
    millis = int(
        calendar.timegm(obj.timetuple()) * 1000
    )
    return millis


def update_database(tic_list, e_date):
    """This function makes sure the data in the database is up to
    date (up to the given end date) for a given list of tickers.
    For each ticker in the list, if there exists a coresponding 
    data table whose latest date is beyond the given end date, 
    the function will skip it. Otherwise it will drop any existing
    table and create a new one with the most up-to-date historical
    stock data available. 
    NB: The reason it's been written to update by dropping tables 
    altogether (as opposed to just adding new data for dates that 
    don't yet exist in the table) is that yahoo's adjusted historical 
    stock prices change when certain events occur, such as stock splits 
    and dividend-grants.
    NB: s_date is the earliest date that the app will provide historical 
    stock data for. 
    """
    s_date = "1/1/1980"
    for tic in tic_list:
        tab_name = "table_" + tic.lower()
        if table_exists(tab_name) is False:
            # get data
            dat = get_yahoo_stock_data(tic, s_date, e_date)
            add_co_data(tic, dat)
        else:
            tic_last_date = get_max_date(tab_name)
            end_date = datetime.datetime.strptime(e_date, '%m/%d/%Y').date()
            if end_date > tic_last_date:
                # drop table
                drop_table("table_" + tic)
                # get data
                dat = get_yahoo_stock_data(tic, s_date, e_date)
                add_co_data(tic, dat)


def csvate_results(tics, s_date, e_date):
    """This function takes a list of tickers, a start-date and an
    end-date and it creates csv and json files with the total 
    shareholder return data for those companies within the given 
    time period. It does this by joining all the tsr calculations
    and fetching them from the database.
    NB: the function also grabs the highest and lowest tsr values,
    as well as the number of datapoints each company has in the
    time period and writes them into the json. This is to lighten
    the load of the front-end when it comes to graphing the data.
    """
    tic_list = []
    for tic in tics:
        tic_list.append(tic.lower())
    project_path = '***ADD PROJECT PATH***'
    with open(project_path + "/results.csv", "wb") as write_doc:
        doc_writer = csv.writer(write_doc)
        # convert date types
        start_date = datetime.datetime.strptime(s_date, '%m/%d/%Y').date()
        end_date = datetime.datetime.strptime(e_date, '%m/%d/%Y').date()

        # add tsr data to each ticker's data table
        update_tsr(tics, start_date, end_date)

        db = db_connect()
        c = db.cursor('my_cursor')
        # specify cursor so as to iterate the fetching, just in case the
        # size of the fetch_all is ever too much. 
        c.itersize = 1000
        select_part = ("SELECT table_%s.date_val" % (tic_list[0],))
        
        # Build psql query
        for tic in tic_list:
            select_part += ", table_%s.tsr" % (tic,)

        from_part = (" FROM table_%s" % (tic_list[0],))
        join_part = ""
        max_query = str("SELECT GREATEST((SELECT MAX(tsr) FROM table_%s WHERE date_val >= '%s' AND date_val <= '%s')" %
                        (tic_list[0], start_date, end_date,))
        min_query = str("SELECT LEAST((SELECT MIN(tsr) FROM table_%s WHERE date_val >= '%s' AND date_val <= '%s')" %
                        (tic_list[0], start_date, end_date,))
        for x in range(1,len(tic_list)):
            join_part += str(" FULL OUTER JOIN table_%s on table_%s.date_val = table_%s.date_val" %
                             (tic_list[x], tic_list[0], tic_list[x],))
            max_query += str(", (SELECT MAX(tsr) FROM table_%s WHERE date_val >= '%s' AND date_val <= '%s')" %
                             (tic_list[x], start_date, end_date,))
            min_query += str(", (SELECT MIN(tsr) FROM table_%s WHERE date_val >= '%s' AND date_val <= '%s')" %
                             (tic_list[x], start_date, end_date,))

        diff = end_date - start_date
        num_total = diff.days
        print num_total
        where_part = (") AS bulk WHERE (bulk.date_val >= '%s' AND bulk.date_val <= '%s');" %
             (start_date, end_date,))

        que = ("SELECT * FROM (" + select_part + from_part + join_part + where_part)
        c.execute(que)
        d = db.cursor()
        d.itersize = 1000

        header_row = ["Date"]
        for each in tic_list:
            header_row.append(each)
        doc_writer.writerow(header_row)

        all_results = []
        while True:
            d.execute("FETCH 1000 FROM my_cursor;")
            results = d.fetchall()
            all_results += results
            if not results:
                break

            for each in results:
                doc_writer.writerow(each)
        
        # get max tsr value
        d.execute(max_query + ");")
        max_val = d.fetchall()[0][0]
        # get min txr value
        d.execute(min_query + ");")
        min_val = d.fetchall()[0][0]

        count_query = str(
            "SELECT COUNT(*) FROM(" + select_part + from_part + join_part +
            where_part
        )
        d.execute(count_query)
        count_vals = d.fetchall()[0][0]

        js_data = {
            'max': max_val,
            'min': min_val,
            'count': count_vals,
            'data': all_results
        }
        with open(project_path + "/upload/results.json","w") as js_doc:
            js_doc.write("tsr_json = '[%s]';" % (json.dumps(js_data, default=default)),)

        db.close()
        # remove tsr data for each ticker's data table
        clear_tsr(tics)


def update_tsr(tics, s_date, e_date):
    """this function adds a column in each ticker's stock data table
    for its tsr data within a given time period.
    """
    db = db_connect()
    c = db.cursor()
    for tic_draft in tics:
        tic = tic_draft.lower()

        c.execute("ALTER TABLE table_%s ADD COLUMN tsr float;" % (tic,))
        db.commit()

        c.execute("SELECT MIN(date_val) FROM table_%s;" % (tic,))

        start_date = c.fetchall()[0][0]
        # Use whichever date is more recent: the requested date or the first
        # date where trade data is available (catches cases where the stock
        # started trading after the date requested)
        if start_date < s_date:
            start_date = s_date

        c.execute("SELECT %s FROM table_%s WHERE date_val = '%s';" %
                 (tic, tic, start_date,))
        
        # sometimes the requested start date will not be a day that has
        # stock data affiliated with it, probably becuase the markets
        # were not open that day. So we want to step back day by day 
        # until we reach a day that has stock data available. 
        start_val_test = c.fetchall()
        while start_val_test == []:
            start_date = start_date + datetime.timedelta(days=1)
            c.execute("SELECT %s FROM table_%s WHERE date_val = '%s';" %
                     (tic, tic, start_date,))
            start_val_test = c.fetchall()

        start_val = start_val_test[0][0]
        # add column with tsr data.
        # tsr is calculated as each day's value's change (as a %) from the
        # original date's value. Only works for adjusted stock values;
        # else it's not tsr but stock price appreciation.
        c.execute("UPDATE table_%s SET tsr = (%s / %s) - 1 WHERE (date_val >= '%s' AND date_val <= '%s');" %
                  (tic, tic, start_val, start_date, e_date,))

        db.commit()
    db.close()


def clear_tsr(tics):
    """this function clears the tsr data for a list of tickers. This
    allows for future tsr calculations.
    """
    db = db_connect()
    c = db.cursor()
    for tic_draft in tics:
        tic = tic_draft.lower()
        c.execute("ALTER TABLE table_%s DROP COLUMN tsr;" % (tic,))
        db.commit()
    db.close()


def string_passes(the_string):
    """this function makes sure the inputted ticker lists are 
    acceptable strings.
    """
    test_string = the_string.strip()
    test_string = test_string.replace(", ", ",")
    for c in test_string:
        if c.isalpha() == False and c != ",":
            return False
    return True
