import pandas as pd
import numpy as np

# class Lib:
#     def fun1(self,data):
#         return pd.DataFrame(data, columns = ["StudentID", "Department", "Date", "Status"])
    
#     def fun2(self,df):
#         df['month'] = df['date'].str[:7]
#         borrowed = df[df['status'] == 'Borrowed']
#         count = borrowed.groupby(["studentId", "Month"]).size().reset_index(name = "Borrowed count")
        
#     def add_late(self,df):
#         df["isLate"] = df[df["status"] == 'Late'].astype(int)
#         return df
    
#     def High_default(self, df, n):
#         late_df = df[df["status"] == 'late']
#         count = late_df.groupby("student_id").size().reset_index(name = "newcol")
#         return count[count['late count'] > 4]
    
#     def department_borrowing_summary(self, df: pd.DataFrame) -> pd.DataFrame:
#         summary = df.groupby(["Department", "Status"]).size().unstack(fill_value=0).reset_index()
#         return summary
    
#     def cn1(self, df):
#         filed = df[df['status'] == 'failed']
#         count = filed.groupby('studentId').size().reset_index(name ="newcol")
#         return count[count['newcol'] > max_allowed]
#     def missed(self, df, limit):
#         filter = df[df['sttus'] == 'missed']
#         count = filter.groupby().size().reset_index(name = "missedCnt")
#         return count[count['missedCnt'] > limit]
    
    #df is your dataframe with a datetime column
    #df is your dataframe
    
# import pandas as pd

# class StudentPerformanceDashBoard:

#     def create_performance_df(self, data):
#         columns = ['StudentID', 'Class', 'Subject', 'Date', 'Score']
#         df = pd.DataFrame(data, columns=columns)
#         df['Date'] = pd.to_datetime(df['Date'])  # Ensure Date is datetime
#         return df

#     def compute_student_avg_score(self, df):
#         return df.groupby('StudentID')['Score'].mean().reset_index(name='AverageScore')

#     def add_pass_fail_flag(self, df, pass_mark):
#         df = df.copy()
#         df['Pass'] = df['Score'] >= pass_mark
#         return df

#     def top_scorers_by_subject(self, df, top_n):
#         return df.sort_values(['Subject', 'Score'], ascending=[True, False]).groupby('Subject').head(top_n)

#     def class_subject_summary(self, df):
#         return df.groupby(['Class', 'Subject'])['Score'].agg(['mean', 'min', 'max']).reset_index()
# data = [
#     [1, "10A", "Math", "2024-01-10", 85],
#     [2, "10A", "Math", "2024-01-10", 78],
#     [1, "10A", "Science", "2024-01-15", 92],
#     [3, "10B", "Math", "2024-01-10", 88],
#     [2, "10A", "Science", "2024-01-15", 75],
#     [3, "10B", "Science", "2024-01-15", 80],
# ]

# dash = StudentPerformanceDashBoard()
# df = dash.create_performance_df(data)

# print("🎓 Performance DataFrame:")
# print(df)

# print("\n📊 Student Average Scores:")
# print(dash.compute_student_avg_score(df))

# print("\n✅ Pass/Fail Flag (pass mark = 80):")
# print(dash.add_pass_fail_flag(df, pass_mark=80))

# print("\n🏅 Top Scorers by Subject (top 1):")
# print(dash.top_scorers_by_subject(df, top_n=1))

# print("\n📚 Class-Subject Summary:")
# print(dash.class_subject_summary(df))
      


class LibraryBook:
    def add_book(self, lib, book_title, copies):
        lib[book_title] = lib.get(book_title, 0) + copies
    
    def update_copies(self, lib, book_title, new_cop):
        if book_title not in lib:
            return "Error"
        lib[book_title] = new_cop
        return lib

    def get_copies(self,lib, book_title):
        return lib[book_title]

    def get_avail(self, lib):
        newL = list()
        for x,y in lib.items():
            if y > 0:
                newL.append(x)


class Product_sales(self,sales):
    def create_sales(self, sales_data):
        return pd.DataFrame(sales_data, columns=["transion",'union'])
    
    def merge_product(self, sales, product):
        return pd.merge(sales, product, how = 'left', on = 'product_id')
    
    def total_unit_sold(self, merge_df):
        newDf = df.groupby("productName")["unitsold"].sum().reset_index()
    
    def filter_high(self, merged_df, threshold):
        return merged_df[merged_df['unit sold'] > threshold]
    
    def most_popular(self, maerged):
        region_grp = merged.groupby(['region','productName'])['unitsSold'].sum().reset_index()
        idx = region_grp.groupby("region")["unitsSold"].idxmax()




class HealthTracker:
    def createSteps(self, self_list):
        return np.array(self_list)
    
    def validate(self, step_array):
        if step_array.size == 0:
            return False
    
    def add_new_day(self, sarray, ndarray, new_steps):
        return np.append(sarray, new_steps)
    
    def category(self, step_array, data):
        cat

        np.array([f"{step} steps" for step in arry])



class FlightDelayedAnayalize:
    def create_flighLog(self, fight_sim):
        return pd.DataFrame(fight_sim, columns=['flightID','Airline','Route','Delay'])
    
    def create_2ndflight(self, nd_flight):
        return pd.DataFrame(nd_flight, columns= ['Airline','Name'])
    
    def airline_name(self, flight, airlinndf):
        return pd.merge(flight, airlinndf, how = 'left', on = 'Airline')
    
    def avgDetail(self, merged_df):
        newDf = df.groupby("airlineName")["ColumnTwo"].mean().reset_index(name = "no new")
        return newDf
    
    def filterd(self, d1, treshhold):
        return df[df['delay'] > treshhold]
    

    def most_deloayed(self, fightlog):
        avgdetails = fightlog.groupby("ColumnOne")["ColumnTwo"].mean().reset_index(name = "new time")
        return avgdetails.sort_values(by = "new time", ascending = False).reset_index(drop = True).head(1)
    
