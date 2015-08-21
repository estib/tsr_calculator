__author__ = 'stephenlechner'

# This file contains all the functions for the tsr_calculator's backend 
# processing. It's very much a work in progress.

import csv
import urllib2
import requests
import psycopg2
import datetime
import json

def default(obj):
    """Default JSON serializer."""
    """Grabbed this from Jay Taylor at
    http://stackoverflow.com/questions/11875770/how-to-overcome-datetime-datetime-not-json-serializable-in-python
    """
    import calendar, datetime

    if isinstance(obj, datetime.datetime):
        if obj.utcoffset() is not None:
            obj = obj - obj.utcoffset()
    millis = int(
        calendar.timegm(obj.timetuple()) * 1000
    )
    return millis


def get_yahoo_stock_data(ticker, s_date, e_date):

    data_list = []
    startmonth = str(int(s_date[:s_date.find("/")]) - 1)
    #startday = str(s_date[s_date.find("/")+1:s_date.find("/", 3)])
    #startyear = str(s_date[-4:])
    #endmonth = str(e_date[:e_date.find("/")])
    #endday = str(e_date[e_date.find("/")+1:e_date.find("/", 3)])
    #endyear = str(e_date[-4:])
    print ticker
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
    #print data_list[0]
    return data_list


def db_connect():
    return psycopg2.connect("dbname=ytsr")


def add_co_data(name, data_list):
    # This function takes the data for a new company and adds it to the
    # database.
    # data_list must be a list/tuple of 2-value-tuples
    db = db_connect()

    c = db.cursor()

    name_lc = name.lower()

    draft_name = "table_%s" % (name_lc,)

    if table_exists(draft_name) == False:
        c.execute(
            "CREATE TABLE table_%s(id serial UNIQUE NOT NULL, date_val date UNIQUE, %s float);" % (name_lc, name_lc,)
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
    db = db_connect()
    c = db.cursor()
    c.execute("DROP TABLE %s;" % table_name)
    db.commit()


def table_exists(name):
    db = db_connect()

    c = db.cursor()

    c.execute("SELECT * FROM table_exists(%s)", (name,))
    val = c.fetchall()[0][0]
    db.close()
    if val == True:
        return True
    elif val == False:
        return False
    else:
        print "Error: did not return true or false."
        print val


def get_max_date(table_name):
    db = db_connect()
    c = db.cursor()
    c.execute("SELECT MAX( date_val) FROM %s;" % table_name)
    max = c.fetchall()[0][0]
    db.close()
    return max


def update_database(tic_list, e_date):
    for tic in tic_list:
        tab_name = "table_" + tic.lower()
        if table_exists(tab_name) == False:
            # get data
            dat = get_yahoo_stock_data(tic, "1/1/1980", e_date)
            add_co_data(tic, dat)
        else:
            tic_last_date = get_max_date(tab_name)
            end_date = datetime.datetime.strptime(e_date, '%m/%d/%Y').date()
            if end_date > tic_last_date:
                # drop table
                drop_table("table_" + tic)
                # get data
                dat = get_yahoo_stock_data(tic, "1/1/1980", e_date)
                add_co_data(tic, dat)


def csvate_results_2(tics, s_date, e_date):
    tic_list = []
    for tic in tics:
        tic_list.append(tic.lower())
    with open("/Users/stephenlechner/Google Drive/Steve's Python Projects/yahoo_tsr/results_B.csv", "wb") as write_doc:
        doc_writer = csv.writer(write_doc)
        start_date = datetime.datetime.strptime(s_date, '%m/%d/%Y').date()
        end_date = datetime.datetime.strptime(e_date, '%m/%d/%Y').date()

        update_tsr(tics, start_date, end_date)

        db = db_connect()
        c = db.cursor('my_cursor')
        c.itersize = 100
        select_part = ("SELECT table_%s.date_val" % (tic_list[0],))
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
        d.itersize = 100

        header_row = ["Date"]
        for each in tic_list:
            header_row.append(each)
        doc_writer.writerow(header_row)

        all_results = []
        while True:
            d.execute("FETCH 100 FROM my_cursor;")
            results = d.fetchall()
            #results = c.fetchmany(100)
            all_results += results
            if not results:
                break

            for each in results:
                doc_writer.writerow(each)

        d.execute(max_query + ");")
        max_val = d.fetchall()[0][0]
        print max_val

        d.execute(min_query + ");")
        min_val = d.fetchall()[0][0]
        print min_val

        count_query = str(
            "SELECT COUNT(*) FROM(" + select_part + from_part + join_part +
            where_part
        )
        d.execute(count_query)
        count_vals = d.fetchall()[0][0]
        print count_vals

        js_data = {
            'max': max_val,
            'min': min_val,
            'count': count_vals,
            'data': all_results
        }
        with open("/Users/stephenlechner/Google Drive/Steve's Python Projects/yahoo_tsr/upload/results_B.json","w") as js_doc:
            js_doc.write("tsr_json = '[%s]';" % (json.dumps(js_data, default=default)),)

        db.close()
        clear_tsr(tics)


def update_tsr(tics, s_date, e_date):

    db = db_connect()
    c = db.cursor()
    for tic_draft in tics:
        tic = tic_draft.lower()

        c.execute("ALTER TABLE table_%s ADD COLUMN tsr float;" % (tic,))
        db.commit()

        c.execute("SELECT MIN(date_val) FROM table_%s;" % (tic,))

        start_date = c.fetchall()[0][0]

        if start_date < s_date:
            start_date = s_date

        c.execute("SELECT %s FROM table_%s WHERE date_val = '%s';" %
                 (tic, tic, start_date,))

        start_val_test = c.fetchall()
        while start_val_test == []:
            start_date = start_date + datetime.timedelta(days=1)
            c.execute("SELECT %s FROM table_%s WHERE date_val = '%s';" %
                     (tic, tic, start_date,))
            start_val_test = c.fetchall()

        start_val = start_val_test[0][0]

        c.execute("UPDATE table_%s SET tsr = (%s / %s) - 1 WHERE (date_val >= '%s' AND date_val <= '%s');" %
                  (tic, tic, start_val, start_date, e_date,))

        db.commit()
    db.close()


def clear_tsr(tics):
    db = db_connect()
    c = db.cursor()
    for tic_draft in tics:
        tic = tic_draft.lower()

        c.execute("ALTER TABLE table_%s DROP COLUMN tsr;" % (tic,))
        db.commit()
    db.close()


def string_passes(the_string):
    test_string = the_string.strip()
    test_string = test_string.replace(", ", ",")
    for c in test_string:
        if c.isalpha() == False and c != ",":
            return False
    return True
