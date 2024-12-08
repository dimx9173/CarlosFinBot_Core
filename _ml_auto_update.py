from finlabML.crawler import table_date_range, update_table, to_pickle, out, time
from finlabML.crawler import (
    crawl_price,
    crawl_bargin,
    crawl_pe,
    crawl_monthly_report,
    crawl_finance_statement_by_date,
    crawl_benchmark,
    crawl_twse_divide_ratio,
    crawl_otc_divide_ratio,
    crawl_twse_cap_reduction,
    crawl_otc_cap_reduction,


    date_range,
    month_range,
    season_range,

    widget, out,
    commit,
)


import datetime

from inspect import signature

def auto_update(table_name, crawl_function, time_range=None):
    sig = signature(crawl_function)
    if len(sig.parameters) != 0:
        first_date, last_date = table_date_range(table_name)
        dates = time_range(last_date, datetime.datetime.now())
        #dates = time_range(last_date, datetime.date(2019,7,1))
        #dates = time_range(datetime.date(2019,6,21), datetime.date(2019,7,1))
        #dates = time_range(last_date, last_date + datetime.timedelta(days=30))
        if dates:
            update_table(table_name, crawl_function, dates)
            print("***** update compileted")
    else:
        df = crawl_function()
        to_pickle(df, table_name)
        print("***** update compileted seconde type")

def auto_update_with_commit(table_name, crawl_function, time_range=None):
    print(">>>>>Start of update:" + table_name)
    auto_update(table_name, crawl_function, time_range)
    commit(table_name)
    print("<<<<<End of update")

print("===============[", time.ctime(),"]_ml_auto_update start=============")
auto_update_with_commit('price', crawl_price, date_range)
auto_update_with_commit('bargin_report', crawl_bargin, date_range)
auto_update_with_commit('pe', crawl_pe, date_range)
auto_update_with_commit('benchmark', crawl_benchmark, date_range)
auto_update_with_commit('monthly_report', crawl_monthly_report, month_range)
auto_update_with_commit('financial_statement', crawl_finance_statement_by_date, season_range)
auto_update_with_commit('twse_divide_ratio', crawl_twse_divide_ratio)
auto_update_with_commit('otc_divide_ratio', crawl_otc_divide_ratio)
auto_update_with_commit('twse_cap_reduction', crawl_twse_cap_reduction)
auto_update_with_commit('otc_cap_reduction', crawl_otc_cap_reduction)
#time.sleep(5)
print("===============[", time.ctime(),"]_ml_auto_update end=============")
#commit()
