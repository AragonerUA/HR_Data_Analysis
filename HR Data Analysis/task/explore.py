from collections import Counter
import pandas as pd
import numpy as np
import requests
import os


def count_bigger_5(df):
    return sum(df>5)


# scroll down to the bottom to implement your solution

if __name__ == '__main__':

    """
    if not os.path.exists('../Data'):
        os.mkdir('../Data')

    # Download data if it is unavailable.
    if ('A_office_data.xml' not in os.listdir('../Data') and
        'B_office_data.xml' not in os.listdir('../Data') and
        'hr_data.xml' not in os.listdir('../Data')):
        print('A_office_data loading.')
        url = "https://www.dropbox.com/s/jpeknyzx57c4jb2/A_office_data.xml?dl=1"
        r = requests.get(url, allow_redirects=True)
        open('../Data/A_office_data.xml', 'wb').write(r.content)
        print('Loaded.')

        print('B_office_data loading.')
        url = "https://www.dropbox.com/s/hea0tbhir64u9t5/B_office_data.xml?dl=1"
        r = requests.get(url, allow_redirects=True)
        open('../Data/B_office_data.xml', 'wb').write(r.content)
        print('Loaded.')

        print('hr_data loading.')
        url = "https://www.dropbox.com/s/u6jzqqg1byajy0s/hr_data.xml?dl=1"
        r = requests.get(url, allow_redirects=True)
        open('../Data/hr_data.xml', 'wb').write(r.content)
        print('Loaded.')

        # All data in now loaded to the Data folder. """

    # write your code here
    A_of_dt = pd.read_xml("/Users/aragonerua/PycharmProjects/HR Data Analysis/HR Data Analysis/Data/A_office_data.xml")
    A_new_indexes = list()
    for i in range(len(A_of_dt["employee_office_id"])):
        A_new_indexes.append("A" + str(A_of_dt["employee_office_id"][i]))
    A_of_dt.index = A_new_indexes
    # print(A_of_dt)

    B_of_dt = pd.read_xml("/Users/aragonerua/PycharmProjects/HR Data Analysis/HR Data Analysis/Data/B_office_data.xml")
    B_new_indexes = list()
    for i in range(len(B_of_dt["employee_office_id"])):
        B_new_indexes.append("B" + str(B_of_dt["employee_office_id"][i]))
    B_of_dt.index = B_new_indexes
    # print(B_of_dt)

    united_a_and_b = pd.concat([A_of_dt, B_of_dt])
    # print(united_a_and_b)

    hr_data = pd.read_xml("/Users/aragonerua/PycharmProjects/HR Data Analysis/HR Data Analysis/Data/hr_data.xml")
    hr_data_new_indexes = list()
    for i in range(len(hr_data["employee_id"])):
        hr_data_new_indexes.append(hr_data["employee_id"][i])
    hr_data.index = hr_data_new_indexes
    # print(hr_data)
    # print(hr_data.set_index(hr_data["employee_id"]))

    merged_all = pd.merge(united_a_and_b, hr_data, how='left', left_index=True, right_index=True,
                          sort=True, indicator=True)
    merged_all = merged_all.dropna().drop(columns=["employee_id", "employee_office_id", "_merge"])
    # print(merged_all)

    # print(list(merged_all.index), list(merged_all.columns), sep="\n", end="") # The second stage answer

    '''
    print(merged_all)
    print(merged_all.columns)
    '''

    sorted_with_average_monthly_hours = merged_all.sort_values("average_monthly_hours", ascending=False)
    top_hours_departments = list()
    for i in range(10):
        top_hours_departments.append(sorted_with_average_monthly_hours.Department[i])
    # print(top_hours_departments)

    # print(sorted_with_average_monthly_hours.query("(Department == 'IT' & salary == 'low')")['number_project'].sum())
    full_list = list()
    full_list.append(list(merged_all.loc["A4", "last_evaluation":"satisfaction_level":-1]))
    full_list.append(list(merged_all.loc["B7064", "last_evaluation":"satisfaction_level":-1]))
    full_list.append(list(merged_all.loc["A3033", "last_evaluation":"satisfaction_level":-1]))
    # print(full_list)  # stage 3/5 answer

    #/--------------------------------------------------------------------------/#

    '''
    print(merged_all.groupby('left').agg({
        'number_project':['median', count_bigger_5],
        'time_spend_company':['mean', 'median'],
        'Work_accident':'mean',
        'last_evaluation':['mean', 'std']}).round(2).to_dict())
    '''

    # print(merged_all)
    first_table = merged_all.pivot_table(index="Department", columns=["left", "salary"], values="average_monthly_hours", aggfunc=np.median)
    result_on_first_table = first_table.loc[(first_table[(0, "high")] < first_table[(0, "medium")]) | (first_table[(1, "low")] < first_table[(1, "high")])]
    print(result_on_first_table.to_dict())

    '''
    all_departments = list(set(merged_all.Department))
    first_criteria = merged_all.query("left == 1.0").pivot_table(index="Department", columns="salary", values="average_monthly_hours", aggfunc=np.median)
    # print(first_criteria)
    departments_first_criteria = list((first_criteria.loc[first_criteria.high < first_criteria.medium]).index)
    # departments_first_criteria_df = first_criteria.loc[first_criteria.high < first_criteria.medium]

    second_criteria = merged_all.query("left == 0.0").pivot_table(index="Department", columns="salary", values="average_monthly_hours", aggfunc=np.median)
    # print(second_criteria)
    departments_second_criteria = list((second_criteria.loc[second_criteria.low < second_criteria.high]).index)
    # departments_second_criteria_df = second_criteria.loc[second_criteria.low < second_criteria.high]

    all_merged_departments = list(set(departments_second_criteria + departments_first_criteria))
    # print(all_merged_departments)
    non_criteria_department = list((Counter(all_departments) - Counter(all_merged_departments)).elements())
    # need = pd.concat([departments_second_criteria_df, departments_second_criteria_df])
    # print(need)
    print(merged_all.pivot_table(index="Department", columns=["left", "salary"], values="average_monthly_hours").drop(labels=non_criteria_department))
    '''

    # print(A_new_indexes, B_new_indexes, hr_data_new_indexes, sep="\n", end="")

    second_pivot_with_time_spent = pd.pivot_table(merged_all, index=['time_spend_company'], columns=['promotion_last_5years'],
                            values=['satisfaction_level', 'last_evaluation'], aggfunc=['min', 'max', 'mean'])
    second_pivot_with_time_spent = second_pivot_with_time_spent.round(decimals=2)
    result_on_second_table = second_pivot_with_time_spent.loc[(second_pivot_with_time_spent[("mean", "last_evaluation", 1)] < second_pivot_with_time_spent[("mean", "last_evaluation", 0)])]
    # print(table2)

    # print((merged_all.pivot_table(index="Department", columns=["left", "salary"], values="average_monthly_hours").drop(labels=non_criteria_department)).round(decimals=2).to_dict())
    print(result_on_second_table.to_dict())
    # print(merged_all.pivot_table(index="time_spend_company", columns=["promotion_last_5years", "satisfaction_level", "last_evaluation"], aggfunc=["max", "mean", "min"]))
